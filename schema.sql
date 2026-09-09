CREATE TABLE IF NOT EXISTS messages (
    source_message_id BIGINT PRIMARY KEY,
    status TEXT NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending','processing','completed','failed')),
    destination_message_id BIGINT,
    account_no INTEGER,
    attempts INTEGER NOT NULL DEFAULT 0,
    last_error TEXT,
    locked_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_messages_pending
ON messages (status, source_message_id);

CREATE TABLE IF NOT EXISTS account_state (
    account_no INTEGER PRIMARY KEY,
    cooldown_until TIMESTAMPTZ,
    last_error TEXT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
