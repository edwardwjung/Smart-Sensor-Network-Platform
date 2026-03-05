CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE IF NOT EXISTS nodes (
  node_id TEXT PRIMARY KEY,
  status TEXT NOT NULL DEFAULT 'online',
  last_seen TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE TABLE IF NOT EXISTS events (
  event_id UUID PRIMARY KEY,
  node_id TEXT NOT NULL REFERENCES nodes(node_id),
  sensor_type TEXT NOT NULL,
  event_type TEXT NOT NULL,
  ts_device TIMESTAMPTZ NOT NULL,
  ts_ingest TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  confidence DOUBLE PRECISION NOT NULL,
  location_lat DOUBLE PRECISION,
  location_lon DOUBLE PRECISION,
  payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  media_ref TEXT,
  trace_id UUID,
  acked BOOLEAN NOT NULL DEFAULT FALSE,
  processed BOOLEAN NOT NULL DEFAULT FALSE
);

CREATE INDEX IF NOT EXISTS idx_events_ts_ingest ON events (ts_ingest DESC);
CREATE INDEX IF NOT EXISTS idx_events_node_id ON events (node_id);
CREATE INDEX IF NOT EXISTS idx_events_event_type ON events (event_type);
CREATE INDEX IF NOT EXISTS idx_events_processed ON events (processed, ts_ingest);

CREATE TABLE IF NOT EXISTS alert_rules (
  id BIGSERIAL PRIMARY KEY,
  name TEXT NOT NULL,
  event_type TEXT NOT NULL,
  min_confidence DOUBLE PRECISION NOT NULL DEFAULT 0.0,
  cooldown_seconds INTEGER NOT NULL DEFAULT 60,
  webhook_url TEXT,
  email_to TEXT,
  enabled BOOLEAN NOT NULL DEFAULT TRUE,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  last_triggered_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS alerts (
  id BIGSERIAL PRIMARY KEY,
  event_id UUID NOT NULL REFERENCES events(event_id),
  rule_id BIGINT REFERENCES alert_rules(id),
  status TEXT NOT NULL DEFAULT 'fired',
  message TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

INSERT INTO alert_rules (name, event_type, min_confidence, cooldown_seconds, webhook_url, email_to, enabled)
SELECT
  'default_person_rule',
  'person_detected',
  0.8,
  30,
  NULL,
  NULL,
  TRUE
WHERE NOT EXISTS (SELECT 1 FROM alert_rules WHERE name = 'default_person_rule');
