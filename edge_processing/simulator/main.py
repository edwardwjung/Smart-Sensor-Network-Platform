import json
import os
import random
import time
from datetime import datetime, timezone
from uuid import uuid4

import paho.mqtt.client as mqtt

MQTT_HOST = os.getenv("MQTT_HOST", "mosquitto")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
NODE_COUNT = int(os.getenv("SIM_NODE_COUNT", "5"))
INTERVAL = float(os.getenv("SIM_PUBLISH_INTERVAL_SECONDS", "1"))

EVENT_TYPES = [
    ("person_detected", "camera"),
    ("vehicle_detected", "camera"),
    ("temperature_spike", "environmental"),
    ("humidity_alert", "environmental"),
]


def make_event(node_id: str) -> dict:
    event_type, sensor_type = random.choice(EVENT_TYPES)
    confidence = round(random.uniform(0.55, 0.99), 3)
    payload = {
        "class": event_type.replace("_detected", ""),
        "temperature_c": round(random.uniform(18, 37), 2) if sensor_type == "environmental" else None,
        "bbox": [
            random.randint(10, 200),
            random.randint(10, 200),
            random.randint(220, 480),
            random.randint(220, 480),
        ] if sensor_type == "camera" else None,
    }
    return {
        "event_id": str(uuid4()),
        "node_id": node_id,
        "sensor_type": sensor_type,
        "event_type": event_type,
        "ts_device": datetime.now(timezone.utc).isoformat(),
        "confidence": confidence,
        "location": {
            "lat": round(37.70 + random.random() * 0.2, 6),
            "lon": round(-122.52 + random.random() * 0.2, 6),
        },
        "payload": payload,
        "media_ref": None,
        "trace_id": str(uuid4()),
    }


def publish_health(client: mqtt.Client, node_id: str):
    health = {
        "node_id": node_id,
        "cpu": random.randint(10, 85),
        "memory": random.randint(15, 90),
        "uptime_sec": random.randint(60, 200000),
        "ts": datetime.now(timezone.utc).isoformat(),
    }
    client.publish(f"nodes/{node_id}/health", json.dumps(health), qos=1)


def main():
    nodes = [f"node-{i:03d}" for i in range(1, NODE_COUNT + 1)]

    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    username = os.getenv("MQTT_USERNAME")
    password = os.getenv("MQTT_PASSWORD")
    if username:
        client.username_pw_set(username=username, password=password)

    client.connect(MQTT_HOST, MQTT_PORT, keepalive=30)
    client.loop_start()

    tick = 0
    while True:
        for node_id in nodes:
            event = make_event(node_id)
            client.publish(f"nodes/{node_id}/events", json.dumps(event), qos=1)
            if tick % 15 == 0:
                publish_health(client, node_id)
        tick += 1
        time.sleep(INTERVAL)


if __name__ == "__main__":
    main()
