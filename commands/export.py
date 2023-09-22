import json
import os
import time

import aiosqlite
from multidict import MultiDict
from tqdm import tqdm


def add_command(subparsers):
    export_parser = subparsers.add_parser(
        "export",
        help="export chat logs to viewer"
    )
    export_parser.add_argument(
        "-i",
        "--id",
        type=int,
        nargs="+",
        required=True,
        help="IDs of threads to export (the long string of number in the chat URL)",
    )
    return export_parser


async def get_all_attachments(
    connection: aiosqlite.Connection
) -> MultiDict:
    attachments = MultiDict()
    async with connection.execute(
        "SELECT message_id, name, type, url, width, height FROM attachments"
    ) as cursor:
        async for attachment in cursor:
            (
                message_id,
                name,
                attachment_type, 
                url,
                width,
                height
            ) = attachment

            if not url or not name:
                continue

            dumped_attachment = {
                "url": url,
                "name": name,
            }

            if width:
                dumped_attachment["width"] = width
            if height:
                dumped_attachment["height"] = height
            attachments.add(message_id, dumped_attachment)
    
    return attachments


async def get_all_reactions(
    connection: aiosqlite.Connection
) -> MultiDict:
    reactions = MultiDict()
    async with connection.execute(
        "SELECT message_id, emoji, count FROM reactions"
    ) as cursor:
        async for reaction in cursor:
            (
                message_id,
                emoji,
                count
            ) = reaction

            if not emoji or not count:
                continue

            reactions.add(
                message_id,
                {
                    "n": emoji,
                    "c": count,
                },
            )
    
    return reactions


async def execute(args):
    if not os.path.exists(args.database):
        print("[ERROR] No database file found.")
        exit(1)

    async with aiosqlite.connect(args.database) as conn:
        dump = {
            "meta": {
                "users": {},
                "userindex": [],
                "servers": [{
                    "name": "\u200B",
                    "type": "server"
                }],
                "channels": {},
            },
            "data": {},
        }

        all_attachments = await get_all_attachments(conn)
        all_reactions = await get_all_reactions(conn)
        
        for thread_id in args.id:
            str_thread_id = str(thread_id)

            async with conn.execute("SELECT name FROM channels WHERE id = ?", (thread_id,)) as cursor:
                name = (await cursor.fetchone())[0]
            
            dump["meta"]["channels"][str_thread_id] = {
                "server": 0,
                "name": name,
                "nsfw": False,
            }

            async with conn.execute(
                (
                    "SELECT DISTINCT sender_id, name, avatar_url "
                    "FROM messages "
                    "LEFT JOIN users ON messages.sender_id = users.id "
                    "WHERE channel_id = ?"
                ),
                (thread_id,),
            ) as cursor:
                async for user in cursor:
                    id, name, avatar_url = user
                    str_id = str(id)
                    dump["meta"]["userindex"].append(str_id)
                    dump["meta"]["users"][str_id] = {
                        "name": name,
                        "avatar": avatar_url,
                        "tag": "0",
                    }
            
            dump["data"][str_thread_id] = {}
            async with conn.execute(
                (
                    "SELECT messages.id, sender_id, text, timestamp, unsent_timestamp, replied_to_id "
                    "FROM messages "
                    "LEFT JOIN replied_to ON replied_to.message_id = messages.id "
                    "WHERE channel_id = ?"
                ),
                (thread_id,)   
            ) as cursor:
                rows = await cursor.fetchall()
                for message in tqdm(rows):
                    (
                        message_id,
                        sender_id,
                        text,
                        timestamp,
                        unsent_timestamp,
                        replied_to_id,
                    ) = message

                    dumped_message = dump["data"][str_thread_id][message_id] = {
                        "u": dump["meta"]["userindex"].index(str(sender_id)),
                        "t": timestamp,
                    }

                    if text:
                        dumped_message["m"] = text
                    
                    if unsent_timestamp:
                        dumped_message["tu"] = unsent_timestamp
                    
                    if replied_to_id:
                        dumped_message["r"] = replied_to_id

                    if (reactions := all_reactions.getall(message_id, None)):
                        dumped_message["re"] = reactions
                    
                    if (attachments := all_attachments.getall(message_id, None)):
                        dumped_message["a"] = attachments

    output_filename = f"archive-{int(time.time())}"            
    
    with open(f"{output_filename}.json", "w") as f:
        json.dump(dump, f, indent=4, ensure_ascii=False)
    
    print(f"Raw message data dumped to {output_filename}.json")
    
    with open("template.html") as f:
        template = f.read()
    
    template = template.replace(
        '"/*[ARCHIVE]*/"',
        json.dumps(
            json.dumps(
                dump,
                ensure_ascii=False
            ),
            ensure_ascii=False
        )
    )

    with open(f"{output_filename}.html", "w") as f:
        f.write(template)
    print(f"Viewer exported to {output_filename}.html")

                
