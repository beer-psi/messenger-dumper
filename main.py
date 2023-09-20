import asyncio
import aiohttp
import argparse
import hashlib
import hmac
import json
import getpass
import mimetypes
import os
import time
import re
from typing import Any

from maufbapi import AndroidAPI, AndroidState
from maufbapi.http.errors import RateLimitExceeded
from maufbapi.types.graphql import Message, MinimalSticker, Attachment, AttachmentType
from mautrix.util.proxy import ProxyHandler
from tqdm import tqdm


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


def generate_state() -> AndroidState:
    state = AndroidState()
    state.session.region_hint = "ODN"
    state.device.connection_type = "WIFI"
    state.carrier.name = "Verizon"
    state.carrier.hni = 311390
    seed = hmac.new(
        key="generate".encode("utf-8"),
        msg="trolleyy".encode("utf-8"),
        digestmod=hashlib.sha256,
    ).digest()
    state.generate(seed)
    return state


async def get_credentials(credentials_filename) -> tuple[AndroidState, AndroidAPI]:
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
    async with client.raw_http_get(
        url, 
        headers={"referer": f"fbapp://{client.state.application.client_id}/{referer}"},
        sandbox=False,
    ) as resp:
        length = int(resp.headers["Content-Length"])
        if length > 25_000_000: # 25 MiB being maximum upload size for Discord
            return None

        attachment_data = await resp.read()

    while True:
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

        resp = await client.http_post(webhook_url, data=form_data)
        data = await resp.json()

        if "attachments" not in data:
            if "retry_after" in data:
                await asyncio.sleep(int(data["retry_after"]) + 1)
                continue
            else:
                return None
        return data["attachments"][0]["filename"], data["attachments"][0]["url"]


async def convert_sticker(
    client: AndroidAPI,
    sticker: MinimalSticker,
    webhook_url: str,
) -> None | dict[str, Any]:
    resp = await client.fetch_stickers([int(sticker.id)], sticker_labels_enabled=True)
    sticker = resp.nodes[0]
    image = sticker.animated_image or sticker.thread_image
    url = image.uri

    reuploaded_url = await reupload_fb_file(client, url, f"{sticker.id}.png", webhook_url)
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
        else:
            full_screen = attachment.animated_image_full_screen
            width = attachment.animated_image_original_dimensions.x
            height = attachment.animated_image_original_dimensions.y
        
        url = full_screen.uri
        if (width, height) > full_screen.dimensions:
            url = await client.get_image_url(message_id, attachment.attachment_fbid) or url
    elif attachment.typename == AttachmentType.AUDIO:
        url = attachment.playable_url
    elif attachment.typename == AttachmentType.VIDEO:
        url = attachment.attachment_video_url
    elif attachment.typename == AttachmentType.FILE:
        url = await client.get_file_url(thread_id, message_id, attachment.attachment_fbid)
    else: 
        print(f"[WARN] Unsupported attachment type {attachment.typename}")
        return None

    reuploaded_url = await reupload_fb_file(client, url, filename, webhook_url)
    return {
        "url": reuploaded_url[1],
        "name": reuploaded_url[0],
    } if reuploaded_url else None


async def convert_message(
    client: AndroidAPI,
    message: Message,
    userindex: list[str],
    users: dict[str, Any],
    *,
    thread_id: str | int,
    webhook_url: str | None = None,
) -> tuple[str, dict[str, Any]]:
    numeric_id = message.message_id.split(".")[1]

    try:
        converted = {
            "u": userindex.index(message.message_sender.id),
            "t": message.timestamp,
        }
    except ValueError:
        # User not found, probably was kicked?
        userindex.append(message.message_sender.id)
        users[message.message_sender.id] = {
            "name": message.message_sender.messaging_actor.name or "Facebook user",
            "avatar": "",
            "tag": "0",
        }
        converted = {
            "u": len(userindex) - 1,
            "t": message.timestamp,
        }

    if webhook_url:
        if message.sticker:
            if "a" not in converted:
                converted["a"] = []
            uploaded_sticker = await convert_sticker(
                client,
                message.sticker,
                webhook_url,
            )
            if uploaded_sticker:
                converted["a"].append(uploaded_sticker)
        if len(message.blob_attachments) > 0:
            if "a" not in converted:
                converted["a"] = []
            
            for attachment in message.blob_attachments:
                content = await convert_attachment(
                    client,
                    attachment,
                    webhook_url,
                    thread_id=thread_id,
                    message_id=message.message_id,
                )
                if content:
                    converted["a"].append(content)
    if message.message:
        converted["m"] = escape_markdown(message.message.text)
    if (
        message.replied_to_message 
        and message.replied_to_message.message
        and message.replied_to_message.message.message_id
    ):
        replied_numeric_id = message.replied_to_message.message.message_id.split(".")[1]
        converted["r"] = replied_numeric_id

    return numeric_id, converted


async def main(args):
    state, api = await get_credentials(args.credentials)
    
    if os.path.exists("dht.json"):
        with open("dht.json") as f:
            dump = json.load(f)
    else:
        dump = {
            "meta": {
                "users": {},
                "userindex": [],
                "servers": {
                    "name": "Default Server",
                    "type": "server",
                },
                "channels": {},
            },
            "data": {},
        }

    for thread_id in args.id:
        str_thread_id = str(thread_id)
        real_thread_id = thread_id
        
        thread_info = await api.fetch_thread_info(thread_id)
        if not thread_info:
            print(f"[ERROR] Could not retrieve thread information for ID {thread_id}")
            continue
        elif thread_info[0].thread_key.id != thread_id:
            print(
                f"[WARN] Response contained different ID ({thread_info[0].thread_key.id}) than expected {thread_id}"
            )
            real_thread_id = thread_info[0].thread_key.id
        
        info = thread_info[0]

        if str_thread_id not in dump["meta"]["channels"]:
            dump["meta"]["channels"][str_thread_id] = {
                "server": 0,
                "name": info.name or "No Name",
                "nsfw": False,
            }
        if str_thread_id not in dump["data"]:
            dump["data"][str_thread_id] = {}

        print(f"[INFO] Fetching users for thread {info.name} ({thread_id})")
        for pcp in info.all_participants.nodes:
            if pcp.id in dump["meta"]["userindex"]:
                continue

            dump["meta"]["userindex"].append(pcp.id)
            dump["meta"]["users"][pcp.id] = {
                "name": (
                    pcp.messaging_actor.structured_name.text 
                    if pcp.messaging_actor.structured_name
                    else (pcp.messaging_actor.nickname_for_viewer or pcp.messaging_actor.username or "Facebook user")
                ),
                "avatar": "",
                "tag": "0",
            }
        
        print(f"[INFO] Fetching messages for thread {info.name} ({thread_id})")
        dumped_message_count = len(dump["data"][str_thread_id])
        if dumped_message_count > 0:
            before_time_ms = min(
                x["t"] for x in dump["data"][str_thread_id].values()
            ) - 1
            print(f"[INFO] Continuing from timestamp {before_time_ms}")
        else:
            before_time_ms = int(time.time() * 1000)
            print("[INFO] Starting from newest message")

        backfill_more = True
        pbar = tqdm(total=info.messages_count - dumped_message_count)
        while backfill_more:
            # print(f"[INFO] Fetching messages before {before_time_ms}")
            try:
                resp = await api.fetch_messages(thread_id, before_time_ms, msg_count=50)
                messages = resp.nodes
            except RateLimitExceeded:
                print("[WARN] Rate limited. Waiting for 300 seconds before resuming.")
                await asyncio.sleep(300)
                continue
            
            if len(messages) == 0 or not resp.nodes:
                print("[INFO] Nothing left to fetch.")
                backfill_more = False
                break

            for message in messages:
                pbar.update(1)
                numeric_id, data = await convert_message(
                    api,
                    message,
                    dump["meta"]["userindex"],
                    dump["meta"]["users"],
                    webhook_url=args.webhook,
                    thread_id=real_thread_id,
                )
                dump["data"][str_thread_id][numeric_id] = data

            before_time_ms = messages[0].timestamp - 1
            with open("dht.json", "w") as f:
                json.dump(dump, f, ensure_ascii=False, indent=4)
        pbar.close()

        dump["data"][str_thread_id] = dict(
            sorted(
                dump["data"][str_thread_id].items(),
                key=lambda x: x[1]["t"],
            )
        )

        with open("dht.json", "w") as f:
            json.dump(dump, f, ensure_ascii=False, indent=4)
    
    with open("template.html", "r") as f:
        template = f.read()
        filled_template = template.replace(
            '"/*[ARCHIVE]*/"',
            json.dumps(json.dumps(dump))
        )
    
    with open(f"archive_{int(time.time())}.html", "w") as f:
        f.write(filled_template)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-i",
        "--id",
        type=int,
        nargs="+",
        required=True,
        help="Thread IDs to dump messages from (the long string of number in the chat URL)",
    )
    parser.add_argument(
        "-c",
        "--credentials",
        type=str,
        nargs="?",
        default=".credentials",
        help="File to save/read credentials",
    )
    parser.add_argument(
        "-w",
        "--webhook",
        type=str,
        nargs="?",
        help="Discord webhook URL (for preserving attachments)",
    )

    args = parser.parse_args()
    asyncio.run(main(args))
