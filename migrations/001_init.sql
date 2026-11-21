CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE IF NOT EXISTS tasks (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name VARCHAR(160) NOT NULL,
    cron_expression VARCHAR(100) NOT NULL,
    payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    timeout_seconds INT NOT NULL DEFAULT 60 CHECK (timeout_seconds > 0),
    max_retries INT NOT NULL DEFAULT 3 CHECK (max_retries BETWEEN 0 AND 10),
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    next_run_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_tasks_due ON tasks(enabled,next_run_at);

CREATE TABLE IF NOT EXISTS executions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    task_id UUID NOT NULL REFERENCES tasks(id),
    parent_execution_id UUID REFERENCES executions(id),
    status VARCHAR(16) NOT NULL DEFAULT 'PENDING' CHECK (status IN ('PENDING','RUNNING','SUCCEEDED','FAILED','CANCELLED')),
    attempt INT NOT NULL DEFAULT 0,
    worker_id VARCHAR(160),
    scheduled_at TIMESTAMPTZ NOT NULL,
    started_at TIMESTAMPTZ,
    heartbeat_at TIMESTAMPTZ,
    finished_at TIMESTAMPTZ,
    output TEXT,
    error_message TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_executions_claim ON executions(status,scheduled_at);
CREATE INDEX IF NOT EXISTS idx_executions_task ON executions(task_id,created_at DESC);

CREATE TABLE IF NOT EXISTS workers (
    id VARCHAR(160) PRIMARY KEY,
    labels JSONB NOT NULL DEFAULT '{}'::jsonb,
    capacity INT NOT NULL DEFAULT 1,
    last_seen_at TIMESTAMPTZ NOT NULL
);
