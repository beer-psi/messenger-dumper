PRAGMA journal_mode=WAL;

CREATE TABLE IF NOT EXISTS channels(
    id BIGINT PRIMARY KEY NOT NULL,
    `name` TEXT
);

CREATE TABLE IF NOT EXISTS users(
    id BIGINT PRIMARY KEY NOT NULL,
    `name` TEXT NOT NULL,
    avatar_url TEXT
);

CREATE TABLE IF NOT EXISTS messages(
    id TEXT PRIMARY KEY NOT NULL,  -- mid.$abcxyz
    sender_id BIGINT NOT NULL,
    channel_id BIGINT NOT NULL,
    `text` TEXT,
    `timestamp` BIGINT NOT NULL,
    `unsent_timestamp` BIGINT,
    FOREIGN KEY (sender_id) REFERENCES users(id),
    FOREIGN KEY (channel_id) REFERENCES channels(id)
);

CREATE TABLE IF NOT EXISTS replied_to (
    message_id TEXT PRIMARY KEY NOT NULL,
    replied_to_id TEXT NOT NULL,
    FOREIGN KEY (message_id) REFERENCES messages(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS attachments(
    id TEXT PRIMARY KEY, -- videos can be UUIDs
    message_id TEXT NOT NULL,
    `name` TEXT NOT NULL,
    `type` TEXT,
    `url` TEXT NOT NULL,
    width INTEGER,
    height INTEGER,
    FOREIGN KEY (message_id) REFERENCES messages(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS reactions(
    message_id TEXT NOT NULL,
    emoji TEXT NOT NULL,
    count INTEGER,
    FOREIGN KEY (message_id) REFERENCES messages(id) ON DELETE CASCADE,
    UNIQUE(message_id, emoji)
);
