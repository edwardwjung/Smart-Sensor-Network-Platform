# Smart Sensor Network Platform

This project is an end-to-end IoT platform from edge sensor events to cloud ingestion, alerting, and dashboard operations.

## Highlights
- Designed the pipeline across edge simulators, MQTT transport, API ingestion, alert worker, and storage.
- Built operational endpoints and event processing flows for live monitoring.
- Structured reliability patterns for idempotent handling and replay-safe behavior.
- Multi-node edge-to-cloud architecture using MQTT and API contracts.
- Rule-based alerting with cooldown logic to reduce noisy notifications.
- Resilient processing patterns for duplicate/retry scenarios.
- PostgreSQL-backed persistence for operational queries and alert workflows.
- Docker Compose setup enables repeatable full-stack local runs.
