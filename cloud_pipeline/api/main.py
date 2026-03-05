import json
import logging
import os
import threading
from datetime import datetime, timezone
from uuid import UUID, uuid4

import paho.mqtt.client as mqtt
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field

from db import ack_event, create_rule, get_conn, get_latest_events, insert_event, list_events, list_nodes, upsert_node

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("cloud_api")

MQTT_HOST = os.getenv("MQTT_HOST", "mosquitto")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))

app = FastAPI(title="Smart Sensor Network Cloud API", version="0.1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


class Location(BaseModel):
    lat: float | None = None
    lon: float | None = None


class EventIn(BaseModel):
    event_id: UUID = Field(default_factory=uuid4)
    node_id: str
    sensor_type: str
    event_type: str
    ts_device: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    confidence: float = Field(ge=0.0, le=1.0)
    location: Location | None = None
    payload: dict = Field(default_factory=dict)
    media_ref: str | None = None
    trace_id: UUID | None = None


class AlertRuleIn(BaseModel):
    name: str
    event_type: str
    min_confidence: float = Field(default=0.0, ge=0.0, le=1.0)
    cooldown_seconds: int = Field(default=60, ge=0)
    webhook_url: str | None = None
    email_to: str | None = None
    enabled: bool = True


def _handle_event(event_payload: dict) -> None:
    with get_conn() as conn:
        upsert_node(conn, event_payload["node_id"]) 
        insert_event(conn, event_payload)
        conn.commit()


def _on_connect(client, _userdata, _flags, rc, _properties=None):
    if rc == 0:
        logger.info("MQTT connected")
        client.subscribe("nodes/+/events")
        client.subscribe("nodes/+/health")
    else:
        logger.error("MQTT connection failed: %s", rc)


def _on_message(_client, _userdata, msg):
    try:
        payload = json.loads(msg.payload.decode("utf-8"))
    except json.JSONDecodeError:
        logger.warning("Dropped non-JSON payload from %s", msg.topic)
        return

    try:
        if msg.topic.endswith("/health"):
            node_id = payload.get("node_id") or msg.topic.split("/")[1]
            with get_conn() as conn:
                upsert_node(conn, node_id, {"health": payload})
                conn.commit()
            return

        if "event_id" not in payload:
            payload["event_id"] = str(uuid4())
        if "ts_device" not in payload:
            payload["ts_device"] = datetime.now(timezone.utc).isoformat()

        _handle_event(payload)
    except Exception as exc:
        logger.exception("Failed to process MQTT message (%s): %s", msg.topic, exc)


def _run_mqtt():
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    username = os.getenv("MQTT_USERNAME")
    password = os.getenv("MQTT_PASSWORD")
    if username:
        client.username_pw_set(username=username, password=password)
    client.on_connect = _on_connect
    client.on_message = _on_message
    client.connect(MQTT_HOST, MQTT_PORT, keepalive=30)
    client.loop_forever()


@app.on_event("startup")
def startup_event():
    thread = threading.Thread(target=_run_mqtt, daemon=True)
    thread.start()
    logger.info("Started MQTT subscriber thread")


@app.get("/health")
def health():
    return {"ok": True}


@app.get("/dashboard")
def dashboard():
    return FileResponse("static/dashboard.html")


@app.post("/v1/events:ingest")
def ingest_event(event: EventIn):
    event_payload = event.model_dump(mode="json")
    with get_conn() as conn:
        upsert_node(conn, event.node_id)
        insert_event(conn, event_payload)
        conn.commit()
    return {"accepted": True, "event_id": str(event.event_id)}


@app.get("/v1/events")
def get_events(
    limit: int = Query(default=100, ge=1, le=1000),
    node_id: str | None = None,
    event_type: str | None = None,
):
    with get_conn() as conn:
        rows = list_events(conn, limit=limit, node_id=node_id, event_type=event_type)
    return rows


@app.get("/v1/nodes")
def get_nodes():
    with get_conn() as conn:
        rows = list_nodes(conn)
    return rows


@app.post("/v1/events/{event_id}/ack")
def post_ack(event_id: UUID):
    with get_conn() as conn:
        updated = ack_event(conn, event_id)
        conn.commit()
    if not updated:
        raise HTTPException(status_code=404, detail="event not found")
    return {"acked": True}


@app.post("/v1/alerts/rules")
def post_rule(rule: AlertRuleIn):
    with get_conn() as conn:
        created = create_rule(conn, rule.model_dump())
        conn.commit()
    return created


@app.post("/v1/alerts/test")
def test_alert(payload: dict | None = None):
    return {
        "ok": True,
        "message": "test alert received",
        "payload": payload or {},
        "ts": datetime.now(timezone.utc).isoformat(),
    }


@app.get("/v1/dashboard/latest")
def latest_dashboard_events(limit: int = Query(default=50, ge=1, le=200)):
    with get_conn() as conn:
        rows = get_latest_events(conn, n=limit)
    return rows
