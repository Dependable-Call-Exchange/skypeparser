"""
Database models for clean Skype data storage.
"""

CREATE_CLEAN_TABLES_SQL = """
-- Conversations table
CREATE TABLE IF NOT EXISTS clean_skype_conversations (
    id SERIAL PRIMARY KEY,
    conversation_id VARCHAR(255) UNIQUE NOT NULL,
    display_name VARCHAR(255),
    raw_export_id INTEGER NOT NULL,
    first_message_time TIMESTAMP,
    last_message_time TIMESTAMP,
    message_count INTEGER DEFAULT 0,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    CONSTRAINT fk_raw_export
        FOREIGN KEY(raw_export_id)
        REFERENCES raw_skype_exports(id)
        ON DELETE CASCADE
);

-- Messages table
CREATE TABLE IF NOT EXISTS clean_skype_messages (
    id SERIAL PRIMARY KEY,
    message_id VARCHAR(255) UNIQUE,
    conversation_id VARCHAR(255) NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    sender_id VARCHAR(255) NOT NULL,
    sender_name VARCHAR(255),
    message_type VARCHAR(50),
    content TEXT,
    raw_content TEXT,
    is_edited BOOLEAN DEFAULT FALSE,
    structured_data JSONB,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    CONSTRAINT fk_conversation
        FOREIGN KEY(conversation_id)
        REFERENCES clean_skype_conversations(conversation_id)
        ON DELETE CASCADE
);

-- Message media table for storing media-specific data
CREATE TABLE IF NOT EXISTS clean_skype_message_media (
    id SERIAL PRIMARY KEY,
    message_id VARCHAR(255) UNIQUE NOT NULL,
    media_filename VARCHAR(255),
    media_filesize BIGINT,
    media_filetype VARCHAR(100),
    media_url TEXT,
    media_thumbnail_url TEXT,
    media_width INTEGER,
    media_height INTEGER,
    media_duration VARCHAR(50),
    media_description TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    CONSTRAINT fk_message
        FOREIGN KEY(message_id)
        REFERENCES clean_skype_messages(message_id)
        ON DELETE CASCADE
);

-- Message poll table for storing poll-specific data
CREATE TABLE IF NOT EXISTS clean_skype_message_polls (
    id SERIAL PRIMARY KEY,
    message_id VARCHAR(255) UNIQUE NOT NULL,
    poll_question TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    CONSTRAINT fk_message
        FOREIGN KEY(message_id)
        REFERENCES clean_skype_messages(message_id)
        ON DELETE CASCADE
);

-- Poll options table
CREATE TABLE IF NOT EXISTS clean_skype_poll_options (
    id SERIAL PRIMARY KEY,
    poll_id INTEGER NOT NULL,
    option_text TEXT NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    CONSTRAINT fk_poll
        FOREIGN KEY(poll_id)
        REFERENCES clean_skype_message_polls(id)
        ON DELETE CASCADE
);

-- Message location table for storing location-specific data
CREATE TABLE IF NOT EXISTS clean_skype_message_locations (
    id SERIAL PRIMARY KEY,
    message_id VARCHAR(255) UNIQUE NOT NULL,
    latitude VARCHAR(50),
    longitude VARCHAR(50),
    address TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    CONSTRAINT fk_message
        FOREIGN KEY(message_id)
        REFERENCES clean_skype_messages(message_id)
        ON DELETE CASCADE
);

-- Create indexes for faster querying
CREATE INDEX IF NOT EXISTS idx_conversation_id ON clean_skype_messages(conversation_id);
CREATE INDEX IF NOT EXISTS idx_timestamp ON clean_skype_messages(timestamp);
CREATE INDEX IF NOT EXISTS idx_sender_id ON clean_skype_messages(sender_id);
CREATE INDEX IF NOT EXISTS idx_message_type ON clean_skype_messages(message_type);
CREATE INDEX IF NOT EXISTS idx_conv_first_msg ON clean_skype_conversations(first_message_time);
CREATE INDEX IF NOT EXISTS idx_conv_last_msg ON clean_skype_conversations(last_message_time);
CREATE INDEX IF NOT EXISTS idx_conv_raw_export ON clean_skype_conversations(raw_export_id);
CREATE INDEX IF NOT EXISTS idx_structured_data ON clean_skype_messages USING GIN (structured_data);
"""

# SQL for inserting conversation data
INSERT_CONVERSATION_SQL = """
INSERT INTO clean_skype_conversations (
    conversation_id, display_name, raw_export_id,
    first_message_time, last_message_time, message_count
)
VALUES (%s, %s, %s, %s, %s, %s)
ON CONFLICT (conversation_id)
DO UPDATE SET
    display_name = EXCLUDED.display_name,
    raw_export_id = EXCLUDED.raw_export_id,
    first_message_time = EXCLUDED.first_message_time,
    last_message_time = EXCLUDED.last_message_time,
    message_count = EXCLUDED.message_count,
    updated_at = NOW()
RETURNING id;
"""

# SQL for inserting message data
INSERT_MESSAGE_SQL = """
INSERT INTO clean_skype_messages (
    message_id, conversation_id, timestamp, sender_id,
    sender_name, message_type, content, raw_content, is_edited, structured_data
)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (message_id)
DO NOTHING
RETURNING id;
"""

# SQL for inserting media data
INSERT_MEDIA_SQL = """
INSERT INTO clean_skype_message_media (
    message_id, media_filename, media_filesize, media_filetype,
    media_url, media_thumbnail_url, media_width, media_height,
    media_duration, media_description
)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (message_id)
DO UPDATE SET
    media_filename = EXCLUDED.media_filename,
    media_filesize = EXCLUDED.media_filesize,
    media_filetype = EXCLUDED.media_filetype,
    media_url = EXCLUDED.media_url,
    media_thumbnail_url = EXCLUDED.media_thumbnail_url,
    media_width = EXCLUDED.media_width,
    media_height = EXCLUDED.media_height,
    media_duration = EXCLUDED.media_duration,
    media_description = EXCLUDED.media_description
RETURNING id;
"""

# SQL for inserting poll data
INSERT_POLL_SQL = """
INSERT INTO clean_skype_message_polls (
    message_id, poll_question
)
VALUES (%s, %s)
ON CONFLICT (message_id)
DO UPDATE SET
    poll_question = EXCLUDED.poll_question
RETURNING id;
"""

# SQL for inserting poll option
INSERT_POLL_OPTION_SQL = """
INSERT INTO clean_skype_poll_options (
    poll_id, option_text
)
VALUES (%s, %s)
RETURNING id;
"""

# SQL for inserting location data
INSERT_LOCATION_SQL = """
INSERT INTO clean_skype_message_locations (
    message_id, latitude, longitude, address
)
VALUES (%s, %s, %s, %s)
ON CONFLICT (message_id)
DO UPDATE SET
    latitude = EXCLUDED.latitude,
    longitude = EXCLUDED.longitude,
    address = EXCLUDED.address
RETURNING id;
"""

# SQL for retrieving conversations
GET_CONVERSATIONS_SQL = """
SELECT
    id, conversation_id, display_name, raw_export_id,
    first_message_time, last_message_time, message_count,
    created_at, updated_at
FROM clean_skype_conversations
WHERE raw_export_id = %s
ORDER BY last_message_time DESC;
"""

# SQL for retrieving messages for a conversation
GET_MESSAGES_SQL = """
SELECT
    id, message_id, conversation_id, timestamp, sender_id,
    sender_name, message_type, content, raw_content, is_edited, structured_data, created_at
FROM clean_skype_messages
WHERE conversation_id = %s
ORDER BY timestamp ASC;
"""

# SQL for retrieving conversation by ID
GET_CONVERSATION_BY_ID_SQL = """
SELECT
    id, conversation_id, display_name, raw_export_id,
    first_message_time, last_message_time, message_count,
    created_at, updated_at
FROM clean_skype_conversations
WHERE conversation_id = %s;
"""

# SQL for retrieving message by ID
GET_MESSAGE_BY_ID_SQL = """
SELECT
    id, message_id, conversation_id, timestamp, sender_id,
    sender_name, message_type, content, raw_content, is_edited, structured_data, created_at
FROM clean_skype_messages
WHERE message_id = %s;
"""

# SQL for retrieving media data for a message
GET_MEDIA_BY_MESSAGE_ID_SQL = """
SELECT
    id, message_id, media_filename, media_filesize, media_filetype,
    media_url, media_thumbnail_url, media_width, media_height,
    media_duration, media_description, created_at
FROM clean_skype_message_media
WHERE message_id = %s;
"""

# SQL for retrieving poll data for a message
GET_POLL_BY_MESSAGE_ID_SQL = """
SELECT
    p.id, p.message_id, p.poll_question, p.created_at,
    array_agg(o.option_text) as options
FROM clean_skype_message_polls p
LEFT JOIN clean_skype_poll_options o ON p.id = o.poll_id
WHERE p.message_id = %s
GROUP BY p.id, p.message_id, p.poll_question, p.created_at;
"""

# SQL for retrieving location data for a message
GET_LOCATION_BY_MESSAGE_ID_SQL = """
SELECT
    id, message_id, latitude, longitude, address, created_at
FROM clean_skype_message_locations
WHERE message_id = %s;
"""