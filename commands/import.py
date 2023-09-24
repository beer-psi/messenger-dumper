import json
import os

import aiosqlite


def add_command(subparsers):
    import_parser = subparsers.add_parser(
        "import",
        help="import from dht.json to database"
    )
    import_parser.add_argument(
        "file",
        help="dumps to import from",
        nargs="+",
        type=str,
    )
    return import_parser


async def execute(args):
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
        
        for file in args.file:
            with open(file) as f:
                try:
                    data = json.load(f)
                except:
                    print(f"[ERROR] {file} was not valid JSON.")
                    continue
            
            users_rows = []
            for id, user in data["meta"]["users"].items():
                users_rows.append((int(id), user["name"], user.get("avatar", "")))
            
            channels_rows = []
            messages_rows = []
            attachments_rows = []
            replied_to_rows = []
            for id, channel in data["meta"]["channels"].items():
                channel_id = int(id)
                channels_rows.append((channel_id, channel["name"]))

                for message_id, message in data["data"][id].items():
                    if not message_id.startswith("mid."):
                        message_id = "mid." + message_id
                    
                    messages_rows.append((
                        message_id,
                        int(data["meta"]["userindex"][message["u"]]),
                        channel_id,
                        message.get("m", ""),
                        message["t"],
                        None,
                    ))

                    if (replied_to_id := message.get("r")):
                        if not replied_to_id.startswith("mid."):
                            replied_to_id = "mid." + replied_to_id
                        replied_to_rows.append((
                            message_id,
                            replied_to_id,
                        ))

                    for attachment in message.get("a", []):
                        if "-" in attachment["name"]:
                            attachment_type = attachment["name"].split("-", 1)[0]
                            attachment_id = (
                                attachment["name"]
                                .split("-", 1)[1]
                                .split(".", 1)[0]
                            )
                        else:
                            attachment_type = "sticker"
                            attachment_id = attachment["name"].split(".")[0]
                        attachments_rows.append((
                            attachment_id,
                            message_id,
                            attachment["name"],
                            attachment_type,
                            attachment["url"],
                            attachment.get("width"),
                            attachment.get("height"),
                        ))

            await conn.executemany(
                "INSERT INTO users(id, name, avatar_url) VALUES (?, ?, ?) ON CONFLICT DO NOTHING",
                users_rows
            )
            await conn.executemany(
                "INSERT INTO channels(id, name) VALUES (?, ?) ON CONFLICT DO NOTHING",
                channels_rows
            )
            await conn.executemany(
                (
                    "INSERT INTO messages(id, sender_id, channel_id, text, timestamp, unsent_timestamp) "
                    "VALUES (?, ?, ?, ?, ? ,?) ON CONFLICT DO NOTHING"
                ),
                messages_rows,
            )
            await conn.executemany(
                (
                    "INSERT INTO attachments(id, message_id, name, type, url, width, height) VALUES(?, ?, ?, ?, ?, ?, ?) "
                    "ON CONFLICT DO NOTHING"
                ),
                attachments_rows,
            )
            await conn.executemany(
                "INSERT INTO replied_to(message_id, replied_to_id) VALUES (?, ?) ON CONFLICT DO NOTHING",
                replied_to_rows,
            )
            await conn.commit()
            
            print(f"[INFO] Imported {file} to database.")


