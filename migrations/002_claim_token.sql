-- Apply explicitly to existing databases. Container init scripts only run on empty data volumes.
ALTER TABLE executions ADD COLUMN IF NOT EXISTS claim_token uuid;
