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
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    CONSTRAINT fk_conversation
        FOREIGN KEY(conversation_id)
        REFERENCES clean_skype_conversations(conversation_id)
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
    sender_name, message_type, content, raw_content, is_edited
)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (message_id)
DO NOTHING
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
    sender_name, message_type, content, raw_content, is_edited, created_at
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
    sender_name, message_type, content, raw_content, is_edited, created_at
FROM clean_skype_messages
WHERE message_id = %s;
"""