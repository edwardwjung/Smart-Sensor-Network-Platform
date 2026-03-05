import os
import time
from datetime import datetime, timezone

import requests
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool

POSTGRES_DSN = (
    f"dbname={os.getenv('POSTGRES_DB', 'sensor_platform')} "
    f"user={os.getenv('POSTGRES_USER', 'sensor')} "
    f"password={os.getenv('POSTGRES_PASSWORD', 'sensor')} "
    f"host={os.getenv('POSTGRES_HOST', 'postgres')} "
    f"port={os.getenv('POSTGRES_PORT', '5432')}"
)

DEFAULT_WEBHOOK = os.getenv("ALERT_DEFAULT_WEBHOOK")
DEFAULT_EMAIL = os.getenv("ALERT_DEFAULT_EMAIL")

pool = ConnectionPool(conninfo=POSTGRES_DSN, min_size=1, max_size=5, kwargs={"row_factory": dict_row})


def fetch_batch(conn, limit: int = 200):
    return conn.execute(
        """
        SELECT * FROM events
        WHERE processed = FALSE
        ORDER BY ts_ingest ASC
        LIMIT %s
        """,
        (limit,),
    ).fetchall()


def fetch_rules(conn):
    return conn.execute(
        "SELECT * FROM alert_rules WHERE enabled = TRUE"
    ).fetchall()


def on_cooldown(rule: dict) -> bool:
    last = rule.get("last_triggered_at")
    if not last:
        return False
    elapsed = (datetime.now(timezone.utc) - last).total_seconds()
    return elapsed < rule["cooldown_seconds"]


def send_alert(event: dict, rule: dict):
    payload = {
        "rule": rule["name"],
        "event_id": str(event["event_id"]),
        "node_id": event["node_id"],
        "event_type": event["event_type"],
        "confidence": event["confidence"],
        "ts": datetime.now(timezone.utc).isoformat(),
    }

    webhook = rule.get("webhook_url") or DEFAULT_WEBHOOK
    if webhook:
        try:
            requests.post(webhook, json=payload, timeout=3)
        except Exception as exc:
            print(f"webhook failed: {exc}")

    email_to = rule.get("email_to") or DEFAULT_EMAIL
    if email_to:
        print(f"email alert -> {email_to}: {payload}")


def process():
    while True:
        with pool.connection() as conn:
            events = fetch_batch(conn)
            if not events:
                conn.commit()
                time.sleep(2)
                continue

            rules = fetch_rules(conn)
            for event in events:
                for rule in rules:
                    if event["event_type"] != rule["event_type"]:
                        continue
                    if event["confidence"] < rule["min_confidence"]:
                        continue
                    if on_cooldown(rule):
                        continue

                    send_alert(event, rule)
                    conn.execute(
                        """
                        INSERT INTO alerts (event_id, rule_id, status, message)
                        VALUES (%s, %s, 'fired', %s)
                        """,
                        (event["event_id"], rule["id"], f"Rule {rule['name']} fired"),
                    )
                    conn.execute(
                        "UPDATE alert_rules SET last_triggered_at = NOW() WHERE id = %s",
                        (rule["id"],),
                    )

                conn.execute(
                    "UPDATE events SET processed = TRUE WHERE event_id = %s",
                    (event["event_id"],),
                )
            conn.commit()


if __name__ == "__main__":
    process()
