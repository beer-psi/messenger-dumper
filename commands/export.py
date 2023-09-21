import json
import os
import time

import aiosqlite
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
                    "SELECT messages.id, sender_id, text, timestamp, unsent_timestamp, replied_to_id, emoji, count, name, url, width, height "
                    "FROM messages "
                    "LEFT JOIN replied_to ON replied_to.message_id = messages.id "
                    "LEFT JOIN attachments ON attachments.message_id = messages.id "
                    "LEFT JOIN reactions ON reactions.message_id = messages.id "
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
                        reaction_emoji,
                        reaction_count,
                        attachment_name,
                        attachment_url,
                        attachment_width,
                        attachment_height,
                    ) = message

                    if message_id not in dump["data"][str_thread_id]:
                        dumped_message = dump["data"][str_thread_id][message_id] = {
                            "u": dump["meta"]["userindex"].index(str(sender_id)),
                            "t": timestamp,
                        }
                    else:
                        dumped_message = dump["data"][str_thread_id][message_id]

                    if "m" not in dumped_message:
                        if text:
                            dumped_message["m"] = text
                        elif unsent_timestamp:
                            dumped_message["m"] = "*Unsent*"
                    
                    if replied_to_id and "r" not in dumped_message:
                        dumped_message["r"] = replied_to_id
                    
                    if reaction_emoji and reaction_count:
                        if "re" not in dumped_message:
                            dumped_message["re"] = []
                        dumped_message["re"].append({
                            "n": reaction_emoji,
                            "c": reaction_count,
                        })
                    
                    if attachment_name and attachment_url:
                        if "a" not in dumped_message:
                            dumped_message["a"] = []
                        
                        dumped_attachment = {
                            "name": attachment_name,
                            "url": attachment_url,
                        }
                        if attachment_height:
                            dumped_attachment["height"] = attachment_height
                        if attachment_width:
                            dumped_attachment["width"] = attachment_width
                        dumped_message["a"].append(dumped_attachment)

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

                
