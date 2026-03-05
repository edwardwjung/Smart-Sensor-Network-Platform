import json
import os
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Any
from uuid import UUID

from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool


def _dsn() -> str:
    return (
        f"dbname={os.getenv('POSTGRES_DB', 'sensor_platform')} "
        f"user={os.getenv('POSTGRES_USER', 'sensor')} "
        f"password={os.getenv('POSTGRES_PASSWORD', 'sensor')} "
        f"host={os.getenv('POSTGRES_HOST', 'postgres')} "
        f"port={os.getenv('POSTGRES_PORT', '5432')}"
    )


pool = ConnectionPool(conninfo=_dsn(), min_size=1, max_size=10, kwargs={"row_factory": dict_row})


@contextmanager
def get_conn():
    with pool.connection() as conn:
        yield conn


def upsert_node(conn, node_id: str, metadata: dict[str, Any] | None = None) -> None:
    conn.execute(
        """
        INSERT INTO nodes (node_id, metadata, last_seen)
        VALUES (%s, %s::jsonb, NOW())
        ON CONFLICT (node_id)
        DO UPDATE SET
          last_seen = NOW(),
          status = 'online',
          metadata = COALESCE(nodes.metadata, '{}'::jsonb) || EXCLUDED.metadata
        """,
        (node_id, json.dumps(metadata or {})),
    )


def insert_event(conn, event: dict[str, Any]) -> None:
    location = event.get("location") or {}
    payload = event.get("payload") or {}
    ts_device = event.get("ts_device")
    if isinstance(ts_device, str):
        ts_device = datetime.fromisoformat(ts_device.replace("Z", "+00:00"))
    if ts_device is None:
        ts_device = datetime.now(timezone.utc)

    trace_id = event.get("trace_id")
    conn.execute(
        """
        INSERT INTO events (
          event_id,
          node_id,
          sensor_type,
          event_type,
          ts_device,
          confidence,
          location_lat,
          location_lon,
          payload,
          media_ref,
          trace_id
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s, %s)
        ON CONFLICT (event_id) DO NOTHING
        """,
        (
            UUID(str(event["event_id"])),
            event["node_id"],
            event["sensor_type"],
            event["event_type"],
            ts_device,
            float(event["confidence"]),
            location.get("lat"),
            location.get("lon"),
            json.dumps(payload),
            event.get("media_ref"),
            UUID(str(trace_id)) if trace_id else None,
        ),
    )


def list_events(conn, limit: int, node_id: str | None, event_type: str | None):
    clauses = []
    params: list[Any] = []
    if node_id:
        clauses.append("node_id = %s")
        params.append(node_id)
    if event_type:
        clauses.append("event_type = %s")
        params.append(event_type)

    query = "SELECT * FROM events"
    if clauses:
        query += " WHERE " + " AND ".join(clauses)
    query += " ORDER BY ts_ingest DESC LIMIT %s"
    params.append(limit)
    return conn.execute(query, params).fetchall()


def list_nodes(conn):
    return conn.execute("SELECT node_id, status, last_seen, metadata FROM nodes ORDER BY node_id").fetchall()


def ack_event(conn, event_id: UUID) -> bool:
    result = conn.execute("UPDATE events SET acked = TRUE WHERE event_id = %s", (event_id,))
    return result.rowcount > 0


def create_rule(conn, rule: dict[str, Any]) -> dict[str, Any]:
    return conn.execute(
        """
        INSERT INTO alert_rules (name, event_type, min_confidence, cooldown_seconds, webhook_url, email_to, enabled)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        RETURNING *
        """,
        (
            rule["name"],
            rule["event_type"],
            rule.get("min_confidence", 0.0),
            rule.get("cooldown_seconds", 60),
            rule.get("webhook_url"),
            rule.get("email_to"),
            rule.get("enabled", True),
        ),
    ).fetchone()


def get_latest_events(conn, n: int = 50):
    return conn.execute(
        """
        SELECT event_id, node_id, sensor_type, event_type, confidence, ts_ingest, acked
        FROM events
        ORDER BY ts_ingest DESC
        LIMIT %s
        """,
        (n,),
    ).fetchall()
