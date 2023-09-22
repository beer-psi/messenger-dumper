import asyncio
import datetime
import getpass
import hashlib
import hmac
import mimetypes
import itertools
import json
import os
import random
import re
import time
import uuid
from typing import Any, Optional

import aiohttp
import aiosqlite
from aiohttp.client_exceptions import ContentTypeError, ClientPayloadError, ClientOSError
from maufbapi import AndroidAPI, AndroidState
from maufbapi.http.errors import RateLimitExceeded, ResponseTypeError
from maufbapi.types.graphql import Message, MinimalSticker, Attachment, AttachmentType
from mautrix.util import utf16_surrogate
from mautrix.util.proxy import ProxyHandler
from tqdm import tqdm


def add_command(subparsers):
    dump_parser = subparsers.add_parser(
        "dump",
        help="dump chat logs to database"
    )
    # dump_parser.set_defaults(func=handle_dump_command)
    dump_parser.add_argument(
        "-i",
        "--id",
        type=int,
        nargs="+",
        required=True,
        help="Thread IDs (the long number in the chat URL) to dump messages from",
    )
    dump_parser.add_argument(
        "-l",
        "--latest",
        action="store_true",
        help="Start from latest message instead of from earliest timestamp",
    )
    dump_parser.add_argument(
        "-c",
        "--credentials",
        type=str,
        nargs="?",
        default=os.path.join(
            os.path.dirname(
                os.path.dirname(
                    __file__
                )
            ),
            ".credentials"
        ),
        help="File to save/read credentials",
    )
    dump_parser.add_argument(
        "-w",
        "--webhook",
        type=str,
        nargs="*",
        help="Discord webhook URL (for preserving attachments)",
    )
    dump_parser.add_argument(
        "-m",
        "--messages-per-fetch",
        type=int,
        default=95,
        required=False,
        help="Number of messages to fetch each time",
    )

    return dump_parser


_MARKDOWN_ESCAPE_SUBREGEX = '|'.join(r'\{0}(?=([\s\S]*((?<!\{0})\{0})))'.format(c) for c in ('*', '`', '_', '~', '|'))

_MARKDOWN_ESCAPE_COMMON = r'^>(?:>>)?\s|\[.+\]\(.+\)|^#{1,3}|^\s*-'

_MARKDOWN_ESCAPE_REGEX = re.compile(fr'(?P<markdown>{_MARKDOWN_ESCAPE_SUBREGEX}|{_MARKDOWN_ESCAPE_COMMON})', re.MULTILINE)

_URL_REGEX = r'(?P<url><[^: >]+:\/[^ >]+>|(?:https?|steam):\/\/[^\s<]+[^<.,:;\"\'\]\s])'

_MARKDOWN_STOCK_REGEX = fr'(?P<markdown>[_\\~|\*`]|{_MARKDOWN_ESCAPE_COMMON})'


def escape_markdown(text: str, *, as_needed: bool = False, ignore_links: bool = True) -> str:
    r"""A helper function that escapes Discord's markdown.

    Parameters
    -----------
    text: :class:`str`
        The text to escape markdown from.
    as_needed: :class:`bool`
        Whether to escape the markdown characters as needed. This
        means that it does not escape extraneous characters if it's
        not necessary, e.g. ``**hello**`` is escaped into ``\*\*hello**``
        instead of ``\*\*hello\*\*``. Note however that this can open
        you up to some clever syntax abuse. Defaults to ``False``.
    ignore_links: :class:`bool`
        Whether to leave links alone when escaping markdown. For example,
        if a URL in the text contains characters such as ``_`` then it will
        be left alone. This option is not supported with ``as_needed``.
        Defaults to ``True``.

    Returns
    --------
    :class:`str`
        The text with the markdown special characters escaped with a slash.
    """

    if not as_needed:

        def replacement(match):
            groupdict = match.groupdict()
            is_url = groupdict.get('url')
            if is_url:
                return is_url
            return '\\' + groupdict['markdown']

        regex = _MARKDOWN_STOCK_REGEX
        if ignore_links:
            regex = f'(?:{_URL_REGEX}|{regex})'
        return re.sub(regex, replacement, text, 0, re.MULTILINE)
    else:
        text = re.sub(r'\\', r'\\\\', text)
        return _MARKDOWN_ESCAPE_REGEX.sub(r'\\\1', text)


async def get_credentials(credentials_filename) -> tuple[AndroidState, AndroidAPI]:
    def generate_state() -> AndroidState:
        state = AndroidState()
        state.session.region_hint = "ODN"
        state.device.connection_type = "WIFI"
        state.carrier.name = "Verizon"
        state.carrier.hni = 311390
        seed = hmac.new(
            key=uuid.uuid4().encode("utf-8"),
            msg=uuid.uuid4().encode("utf-8"),
            digestmod=hashlib.sha256,
        ).digest()
        state.generate(seed)
        return state

    if os.path.exists(credentials_filename):
        print("Retrieving saved credentials...")
        with open(credentials_filename) as f:
            state = AndroidState.parse_json(f.read())
            api = AndroidAPI(
                state,
                proxy_handler=ProxyHandler(None),
            )
    else:
        state = generate_state()
        api = AndroidAPI(
            state,
            proxy_handler=ProxyHandler(None),
        )

        print("Generating config...")
        await api.mobile_config_sessionless()

        print("Logging in...")
        username = input("Enter username: ")
        password = getpass.getpass("Enter password: ")
        await api.login(
            username,
            password,
        )

        with open(credentials_filename, "w") as f:
            f.write(state.json())
        print(
            f"Logged in, credentials saved at `{credentials_filename}`. Delete this file to log in again."
        )
    
    return state, api


async def reupload_fb_file(
    client: AndroidAPI,
    url: str,
    filename: str,
    webhook_url: str,
    *,
    referer: str = "messenger_thread_photo"
) -> None | tuple[str, str]:
    def parse_ratelimit_header(request: Any, *, use_clock: bool = False) -> float:
        reset: Optional[str] = request.headers.get("X-Ratelimit-Reset")
        reset_after: Optional[str] = request.headers.get('X-Ratelimit-Reset-After')
        if not reset:
            # We raped Discord's servers too hard
            return 60.0
        if use_clock or not reset_after:
            utc = datetime.timezone.utc
            now = datetime.datetime.now(utc)
            reset = datetime.datetime.fromtimestamp(float(reset), utc)
            return (reset - now).total_seconds()
        else:
            return float(reset_after)
        
    backoff = 1
    while True:
        if backoff > 10:
            print(f"[ERROR] Could not download attachment {filename} with URL {url}")
            return None

        try:
            async with client.raw_http_get(
                url, 
                headers={"referer": f"fbapp://{client.state.application.client_id}/{referer}"},
                sandbox=False,
            ) as resp:
                length = int(resp.headers["Content-Length"])
                if length > 25_000_000: # 25 MiB being maximum upload size for Discord
                    return None

                attachment_data = await resp.read()
                break
        except (ClientPayloadError, ClientOSError, asyncio.TimeoutError):
            await asyncio.sleep(backoff)
            backoff += 1
            continue

    form_data = aiohttp.FormData(quote_fields=False)
    form_data.add_field("files[0]", attachment_data, filename=filename, content_type="application/octet-stream")
    form_data.add_field("payload_json", json.dumps({
        "attachments": [
            {
                "id": 0,
                "filename": filename,
            }
        ],
        "content": "",
    }))

    multipart_writer = form_data._gen_form_data()
    
    while True:
        resp = await client.http_post(webhook_url, data=multipart_writer)
        reset_after = parse_ratelimit_header(resp)
        try:
            data = await resp.json()
        except ContentTypeError:
            await asyncio.sleep(reset_after)
            continue

        if "attachments" not in data:
            if "retry_after" in data:
                await asyncio.sleep(reset_after)
                continue
            else:
                return None
        return data["attachments"][0]["filename"], data["attachments"][0]["url"]
    

async def convert_sticker(
    client: AndroidAPI,
    sticker: MinimalSticker,
    webhook_url: str,
) -> None | dict[str, Any]:
    try:
        resp = await client.fetch_stickers([int(sticker.id)], sticker_labels_enabled=True)
    except ResponseTypeError:
       return None 
    sticker = resp.nodes[0]

    if sticker.animated_image:
        image = sticker.animated_image
        extension = "gif"
    else:
        image = sticker.thread_image
        extension = "png"
    url = image.uri

    reuploaded_url = await reupload_fb_file(client, url, f"sticker-{sticker.id}.{extension}", webhook_url)
    return {
        "url": reuploaded_url[1],
        "name": reuploaded_url[0],
        "width": image.width,
        "height": image.height,
    } if reuploaded_url else None


async def convert_attachment(
    client: AndroidAPI,
    attachment: Attachment,
    webhook_url: str,
    *,
    thread_id: str | int,
    message_id: str,
) -> dict[str, Any] | None:
    filename = attachment.filename
    if attachment.mimetype and "." not in filename:
        filename += mimetypes.guess_extension(attachment.mimetype)

    if attachment.typename in (AttachmentType.IMAGE, AttachmentType.ANIMATED_IMAGE):
        if attachment.typename == AttachmentType.IMAGE:
            full_screen = attachment.image_full_screen
            width = attachment.original_dimensions.x
            height = attachment.original_dimensions.y
            attachment_type = "image"
        else:
            full_screen = attachment.animated_image_full_screen
            width = attachment.animated_image_original_dimensions.x
            height = attachment.animated_image_original_dimensions.y
            attachment_type = "gif"
        url = full_screen.uri
        if (width, height) > full_screen.dimensions:
            url = await client.get_image_url(message_id, attachment.attachment_fbid) or url
    elif attachment.typename == AttachmentType.AUDIO:
        url = attachment.playable_url
        attachment_type = "audioclip"
    elif attachment.typename == AttachmentType.VIDEO:
        url = attachment.attachment_video_url
        attachment_type = "video"
    elif attachment.typename == AttachmentType.FILE:
        url = await client.get_file_url(thread_id, message_id, attachment.attachment_fbid)
        attachment_type = "file"
    else: 
        print(f"[WARN] Unsupported attachment type {attachment.typename}")
        return None

    reuploaded_url = await reupload_fb_file(client, url, filename, webhook_url)
    return {
        "url": reuploaded_url[1],
        "name": reuploaded_url[0],
        "type": attachment_type,
        "id": attachment.id,
    } if reuploaded_url else None
    

async def convert_message(
    client: AndroidAPI,
    message: Message,
    *,
    thread_id: str | int,
    webhook_urls: list[str],
) -> dict[str, Any]:
    msg_text = ""
    if message.message:
        msg_text = utf16_surrogate.add(message.message.text)
        for m in reversed(message.message.ranges):
            offset = m.offset
            leng = m.length
            if not m.entity or not m.entity.id:
                continue
            msg_text = f"{msg_text[:offset]}<@{m.entity.id}>{msg_text[offset + leng:]}"
        msg_text = escape_markdown(utf16_surrogate.remove(msg_text))

    result = {
        "users": [
            (
                message.message_sender.id,
                message.message_sender.messaging_actor.name or "Facebook user",
                ""
            )
        ],
        "message": (
            message.message_id,
            message.message_sender.id,
            int(thread_id),
            msg_text,
            message.timestamp,
            message.unsent_timestamp,
        ),
    }

    if (
        message.replied_to_message 
        and message.replied_to_message.message
        and (replied_to_id := message.replied_to_message.message.message_id)
    ):
        result["replied_to"] = (message.message_id, replied_to_id)
    
    if len(message.message_reactions) > 0:
        reactions_grouped_by_emoji = itertools.groupby(
            message.message_reactions,
            lambda x: x.reaction
        )
        result["reactions"] = [
            (
                message.message_id,
                reaction,
                len(list(group)),
            )
            for reaction, group in reactions_grouped_by_emoji
        ]
    
    if len(webhook_urls) > 0:
        if message.sticker:
            if "attachments" not in result:
                result["attachments"] = []

            converted_sticker = await convert_sticker(
                client,
                message.sticker,
                random.choice(webhook_urls),
            )
            
            result["attachments"].append(
                (
                    message.sticker.id,
                    message.message_id,
                    converted_sticker["name"],
                    "sticker",
                    converted_sticker["url"],
                    converted_sticker["width"],
                    converted_sticker["height"],
                )
            )
        
        if len(message.blob_attachments) > 0:
            if "attachments" not in result:
                result["attachments"] = []
            
            attachments = await asyncio.gather(
                *[
                    convert_attachment(
                        client,
                        attachment,
                        random.choice(webhook_urls),
                        thread_id=thread_id,
                        message_id=message.message_id,
                    )
                    for attachment in message.blob_attachments
                ]
            )
            for attachment in attachments:
                if not attachment:
                    continue
                url = attachment["url"]
                name = attachment["name"]
                attachment_type = attachment["type"]
                attachment_id = attachment["id"]
                result["attachments"].append(
                    (
                        attachment_id,
                        message.message_id,
                        name,
                        attachment_type,
                        url,
                        None,
                        None,
                    )
                )
            
    return result


async def execute(args):
    if len(args.webhook) == 0:
        print("[WARN] Webhooks were not provided. Not uploading attachments.")

    schema_path = os.path.join(
        os.path.dirname(
            os.path.dirname(__file__)
        ),
        "database",
        "schema.sql"
    )

    async with aiosqlite.connect(args.database) as conn:
        with open(schema_path) as f:
            await conn.executescript(f.read())

        state, api = await get_credentials(args.credentials)
        for thread_id in args.id:
            real_thread_id = thread_id
            
            thread_info = await api.fetch_thread_info(thread_id)
            if not thread_info:
                print(
                    f"[ERROR] Could not retrieve thread information for ID {thread_id}"
                )
                continue
            elif thread_info[0].thread_key.id != thread_id:
                print(
                    f"[WARN] Response contained different ID "
                    f"({thread_info[0].thread_key.id}) than expected {thread_id}"
                )
                real_thread_id = thread_info[0].thread_key.id
            
            if real_thread_id is None:
                print(
                    "[ERROR] Received thread ID was null??? Not dumping this channel."
                )
                continue
            
            info = thread_info[0]
            await conn.execute(
                "INSERT INTO channels (id, name) VALUES (?, ?) ON CONFLICT DO UPDATE SET name=excluded.name",
                (real_thread_id, info.name or "No name")
            )
            await conn.commit()

            print(f"[INFO] Fetching users for thread {info.name} ({real_thread_id})")
            async def user_data_worker(pcp):
                actor = pcp.messaging_actor
                name = (
                    actor.structured_name.text 
                    if actor.structured_name
                    else (
                        actor.nickname_for_viewer 
                        or actor.username 
                        or "Facebook user"
                    )
                )
                profile_picture = None,
                if (fb_profile_pic := (
                    actor.profile_pic_large 
                    or actor.profile_pic_medium
                    or actor.profile_pic_small
                )) and len(args.webhook) > 0:
                    url = fb_profile_pic.uri
                    reuploaded = await reupload_fb_file(
                        api,
                        url,
                        f"profile_picture-{pcp.id}.jpg",
                        random.choice(args.webhook)
                    )
                    profile_picture = reuploaded[1] if reuploaded else None
                return int(pcp.id), name, profile_picture

            users_rows = await asyncio.gather(
                *[user_data_worker(pcp) for pcp in info.all_participants.nodes]
            )
            await conn.executemany(
                (
                    "INSERT INTO users(id, name, avatar_url) VALUES (?, ?, ?) "
                    "ON CONFLICT DO UPDATE SET name=excluded.name, "
                    "avatar_url=coalesce(excluded.avatar_url, avatar_url)"
                ),
                users_rows
            )
            await conn.commit()

            async with conn.execute(
                "SELECT COUNT(*) FROM messages WHERE channel_id = ?",
                (real_thread_id,)
            ) as cursor:
                dumped_message_count = (await cursor.fetchone())[0]
            
            if dumped_message_count > 0 and not args.latest:
                async with conn.execute(
                    "SELECT timestamp FROM messages WHERE channel_id = ? ORDER BY timestamp ASC LIMIT 1",
                    (real_thread_id,)
                ) as cursor:
                    earliest_timestamp = (await cursor.fetchone())[0]
                    before_time_ms = earliest_timestamp
                    print(f"[INFO] Continuing from timestamp {before_time_ms}")
            else:
                before_time_ms = int(time.time() * 1000)
                print("[INFO] Starting from newest message")
                

            print(f"[INFO] Fetching messages for {info.name} ({real_thread_id})")
            backfill_more = True
            
            semaphore = asyncio.Semaphore(10)
            async def rate_limited_message_task(
                message: Message,
                queue: asyncio.Queue,
            ):
                async with semaphore:
                    result = await convert_message(
                        api,
                        message,
                        thread_id=real_thread_id,
                        webhook_urls=args.webhook,
                    )  
                    return queue.put_nowait(result)
            
            async def db_worker(
                conn: aiosqlite.Connection,
                queue: asyncio.Queue,
                pbar: tqdm,
            ):
                while True:
                    result = await queue.get()
                    
                    # not updating existing users, since MinimalParticipants are less
                    # complete.
                    await conn.executemany(
                        (
                            "INSERT INTO users(id, name, avatar_url) VALUES (?, ?, ?) "
                            "ON CONFLICT DO NOTHING"
                        ),
                        result["users"],
                    )
                    await conn.execute(
                        (
                            "INSERT INTO messages(id, sender_id, channel_id, text, timestamp, unsent_timestamp) "
                            "VALUES (?, ?, ?, ?, ? ,?) "
                            "ON CONFLICT DO UPDATE SET "
                            "    sender_id=coalesce(excluded.sender_id, sender_id),"
                            "    channel_id=coalesce(excluded.channel_id, channel_id),"
                            "    text=coalesce(excluded.text, text),"
                            "    timestamp=coalesce(excluded.timestamp, timestamp),"
                            "    unsent_timestamp=coalesce(excluded.unsent_timestamp, unsent_timestamp)"
                        ),
                        result["message"],
                    )
                    
                    if "replied_to" in result:
                        await conn.execute(
                            (
                                "INSERT INTO replied_to(message_id, replied_to_id) VALUES (?, ?)"
                                "ON CONFLICT DO UPDATE SET replied_to_id=coalesce(excluded.replied_to_id, replied_to_id)"
                            ),
                            result["replied_to"],
                        )
                    
                    if "attachments" in result:
                        await conn.executemany(
                            (
                                "INSERT INTO attachments(id, message_id, name, type, url, width, height) VALUES(?, ?, ?, ?, ?, ?, ?) "
                                "ON CONFLICT DO NOTHING"
                            ),
                            result["attachments"],
                        )
                    
                    if "reactions" in result:
                        await conn.executemany(
                            (
                                "INSERT INTO reactions(message_id, emoji, count) VALUES (?, ?, ?) "
                                "ON CONFLICT (message_id, emoji) DO UPDATE SET count=excluded.count"
                            ),
                            result["reactions"]
                        )  

                    await conn.commit()
                    pbar.update(1)
                    queue.task_done()
            
            pbar = tqdm(total=info.messages_count - dumped_message_count)
            db_queue = asyncio.Queue()
            db_task = asyncio.create_task(db_worker(conn, db_queue, pbar))

            while backfill_more:
                tasks = []
                try:
                    resp = await api.fetch_messages(thread_id, before_time_ms, msg_count=95)
                    messages = resp.nodes
                except RateLimitExceeded:
                    print("[WARN] Rate limited. Waiting for 300 seconds before resuming.")
                    await asyncio.sleep(300)
                    continue
                
                if len(messages) == 0 or not messages:
                    print("[INFO] Nothing left to fetch.")
                    backfill_more = False
                    break

                tasks = [
                    rate_limited_message_task(message, db_queue)
                    for message in messages
                ]

                await asyncio.gather(*tasks)
                
                before_time_ms = messages[0].timestamp - 1
            
            await db_queue.join()
            db_task.cancel()
            try:
                await db_task
            except asyncio.CancelledError:
                pass
