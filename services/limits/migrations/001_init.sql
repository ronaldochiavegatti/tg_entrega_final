CREATE TABLE IF NOT EXISTS limit_config (
    year INT PRIMARY KEY,
    annual_limit NUMERIC NOT NULL,
    warn_threshold NUMERIC NOT NULL DEFAULT 0.8,
    critical_threshold NUMERIC NOT NULL DEFAULT 1.0
);

CREATE TABLE IF NOT EXISTS limits_snapshots (
    tenant_id TEXT NOT NULL,
    year INT NOT NULL,
    month INT NOT NULL,
    accumulated NUMERIC NOT NULL,
    forecast NUMERIC NOT NULL,
    state TEXT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (tenant_id, year, month)
);

CREATE TABLE IF NOT EXISTS audit_field_changes (
    id SERIAL PRIMARY KEY,
    doc_id TEXT NOT NULL,
    user_id TEXT,
    ts TIMESTAMPTZ DEFAULT NOW(),
    field TEXT NOT NULL,
    old_value JSONB,
    new_value JSONB,
    source TEXT NOT NULL
);
