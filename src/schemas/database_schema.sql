-- Skype Parser Database Schema
-- This file defines the database schema for the Skype Parser application
-- It includes tables for raw exports, conversations, messages, and related entities

-- Raw exports table - stores the original export data
CREATE TABLE IF NOT EXISTS public.skype_raw_exports (
    export_id SERIAL PRIMARY KEY,
    user_id TEXT NOT NULL,
    export_date TIMESTAMP NOT NULL,
    raw_data JSONB NOT NULL,
    file_source TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    processed_at TIMESTAMP
);

-- Conversations table - stores conversation metadata
CREATE TABLE IF NOT EXISTS public.skype_conversations (
    conversation_id TEXT PRIMARY KEY,
    display_name TEXT,
    export_id INTEGER REFERENCES public.skype_raw_exports(export_id) ON DELETE CASCADE,
    first_message_time TIMESTAMP,
    last_message_time TIMESTAMP,
    message_count INTEGER,
    participant_count INTEGER,
    conversation_type TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Messages table - stores individual messages
CREATE TABLE IF NOT EXISTS public.skype_messages (
    message_id SERIAL PRIMARY KEY,
    conversation_id TEXT REFERENCES public.skype_conversations(conversation_id) ON DELETE CASCADE,
    timestamp TIMESTAMP NOT NULL,
    sender_id TEXT NOT NULL,
    sender_name TEXT,
    content TEXT,
    html_content TEXT,
    message_type TEXT NOT NULL,
    is_edited BOOLEAN DEFAULT FALSE,
    is_deleted BOOLEAN DEFAULT FALSE,
    reactions JSONB,
    attachments JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Participants table - stores conversation participants
CREATE TABLE IF NOT EXISTS public.skype_participants (
    participant_id SERIAL PRIMARY KEY,
    conversation_id TEXT REFERENCES public.skype_conversations(conversation_id) ON DELETE CASCADE,
    user_id TEXT NOT NULL,
    display_name TEXT,
    is_self BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(conversation_id, user_id)
);

-- Attachments table - stores attachment metadata
CREATE TABLE IF NOT EXISTS public.skype_attachments (
    attachment_id SERIAL PRIMARY KEY,
    message_id INTEGER REFERENCES public.skype_messages(message_id) ON DELETE CASCADE,
    file_name TEXT,
    file_size BIGINT,
    content_type TEXT,
    url TEXT,
    local_path TEXT,
    thumbnail_path TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Processing jobs table - tracks ETL processing jobs
CREATE TABLE IF NOT EXISTS public.skype_processing_jobs (
    job_id TEXT PRIMARY KEY,
    export_id INTEGER REFERENCES public.skype_raw_exports(export_id) ON DELETE CASCADE,
    status TEXT NOT NULL,
    start_time TIMESTAMP NOT NULL,
    end_time TIMESTAMP,
    error_message TEXT,
    metrics JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for better query performance

-- Indexes for skype_raw_exports
CREATE INDEX IF NOT EXISTS idx_raw_exports_user_id ON public.skype_raw_exports(user_id);
CREATE INDEX IF NOT EXISTS idx_raw_exports_export_date ON public.skype_raw_exports(export_date);

-- Indexes for skype_conversations
CREATE INDEX IF NOT EXISTS idx_conversations_export_id ON public.skype_conversations(export_id);
CREATE INDEX IF NOT EXISTS idx_conversations_first_message_time ON public.skype_conversations(first_message_time);
CREATE INDEX IF NOT EXISTS idx_conversations_last_message_time ON public.skype_conversations(last_message_time);
CREATE INDEX IF NOT EXISTS idx_conversations_display_name ON public.skype_conversations(display_name);

-- Indexes for skype_messages
CREATE INDEX IF NOT EXISTS idx_messages_conversation_id ON public.skype_messages(conversation_id);
CREATE INDEX IF NOT EXISTS idx_messages_timestamp ON public.skype_messages(timestamp);
CREATE INDEX IF NOT EXISTS idx_messages_sender_id ON public.skype_messages(sender_id);
CREATE INDEX IF NOT EXISTS idx_messages_message_type ON public.skype_messages(message_type);
CREATE INDEX IF NOT EXISTS idx_messages_conversation_timestamp ON public.skype_messages(conversation_id, timestamp);
CREATE INDEX IF NOT EXISTS idx_messages_content_gin ON public.skype_messages USING gin(to_tsvector('english', content))
    WITH (fastupdate = off) WHERE content IS NOT NULL AND length(content) > 0;

-- Indexes for skype_participants
CREATE INDEX IF NOT EXISTS idx_participants_conversation_id ON public.skype_participants(conversation_id);
CREATE INDEX IF NOT EXISTS idx_participants_user_id ON public.skype_participants(user_id);

-- Indexes for skype_attachments
CREATE INDEX IF NOT EXISTS idx_attachments_message_id ON public.skype_attachments(message_id);
CREATE INDEX IF NOT EXISTS idx_attachments_content_type ON public.skype_attachments(content_type);

-- Indexes for skype_processing_jobs
CREATE INDEX IF NOT EXISTS idx_processing_jobs_export_id ON public.skype_processing_jobs(export_id);
CREATE INDEX IF NOT EXISTS idx_processing_jobs_status ON public.skype_processing_jobs(status);