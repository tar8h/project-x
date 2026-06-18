import json
import os
import signal
import time
from datetime import datetime, timezone
from typing import Any

import psycopg2
import requests
from confluent_kafka import Consumer


STOP = False


def handle_signal(_signum, _frame) -> None:
    global STOP
    STOP = True


def env(name: str, default: str) -> str:
    return os.getenv(name, default)


def parse_timestamp(value: Any) -> datetime:
    if value is None:
        return datetime.now(timezone.utc)
    if isinstance(value, (int, float)):
        if value > 10_000_000_000_000:
            return datetime.fromtimestamp(value / 1_000_000_000, tz=timezone.utc)
        if value > 10_000_000_000:
            return datetime.fromtimestamp(value / 1000, tz=timezone.utc)
        return datetime.fromtimestamp(value, tz=timezone.utc)
    text = str(value)
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
        return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
    except ValueError:
        return datetime.now(timezone.utc)


def parse_message(value: bytes) -> dict[str, Any] | None:
    payload = json.loads(value.decode("utf-8", errors="replace"))
    fields = payload.get("fields") or payload
    tags = payload.get("tags") or payload

    machine_id = tags.get("machine_id") or payload.get("machine_id") or fields.get("machine_id")
    if not machine_id:
        return None

    try:
        return {
            "ts": parse_timestamp(payload.get("timestamp") or payload.get("ts") or fields.get("ts")),
            "site": tags.get("site") or payload.get("site") or fields.get("site") or "unknown",
            "line": tags.get("line") or payload.get("line") or fields.get("line") or "unknown",
            "machine_id": machine_id,
            "temperature": float(fields.get("temperature")),
            "vibration": float(fields.get("vibration")),
            "voltage": float(fields.get("voltage")),
            "raw": payload,
        }
    except (TypeError, ValueError):
        return None


def make_consumer() -> Consumer:
    return Consumer(
        {
            "bootstrap.servers": env("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
            "group.id": env("KAFKA_GROUP_ID", "kafka-to-timescale"),
            "auto.offset.reset": "latest",
            "enable.auto.commit": True,
        }
    )


def make_connection():
    while not STOP:
        try:
            conn = psycopg2.connect(
                host=env("POSTGRES_HOST", "timescaledb"),
                port=int(env("POSTGRES_PORT", "5432")),
                dbname=env("POSTGRES_DB", "maintenance"),
                user=env("POSTGRES_USER", "maintenance"),
                password=env("POSTGRES_PASSWORD", "maintenance"),
            )
            conn.autocommit = True
            return conn
        except psycopg2.Error as exc:
            print(f"waiting for postgres: {exc}", flush=True)
            time.sleep(3)
    raise RuntimeError("stopped before postgres connection")


def score(row: dict[str, Any]) -> dict[str, Any]:
    response = requests.post(
        env("INFERENCE_URL", "http://inference-api:8080/predict"),
        json={
            "machine_id": row["machine_id"],
            "temperature": row["temperature"],
            "vibration": row["vibration"],
            "voltage": row["voltage"],
        },
        timeout=3,
    )
    response.raise_for_status()
    return response.json()


def store(conn, row: dict[str, Any], prediction: dict[str, Any] | None) -> None:
    with conn.cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO sensor_readings
                (ts, site, line, machine_id, temperature, vibration, voltage, raw)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s::jsonb)
            """,
            (
                row["ts"],
                row["site"],
                row["line"],
                row["machine_id"],
                row["temperature"],
                row["vibration"],
                row["voltage"],
                json.dumps(row["raw"]),
            ),
        )
        if prediction is not None:
            cursor.execute(
                """
                INSERT INTO failure_predictions
                    (ts, machine_id, anomaly_score, failure_probability, model_name, model_version, features)
                VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb)
                """,
                (
                    row["ts"],
                    row["machine_id"],
                    prediction["anomaly_score"],
                    prediction["failure_probability"],
                    prediction["model_name"],
                    prediction["model_version"],
                    json.dumps(
                        {
                            "temperature": row["temperature"],
                            "vibration": row["vibration"],
                            "voltage": row["voltage"],
                        }
                    ),
                ),
            )


def main() -> None:
    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    consumer = make_consumer()
    conn = make_connection()
    topic = env("KAFKA_TOPIC", "sensor.raw")
    consumer.subscribe([topic])
    print(f"consuming {topic}", flush=True)

    try:
        while not STOP:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"kafka warning: {msg.error()}", flush=True)
                time.sleep(2)
                continue

            row = parse_message(msg.value())
            if row is None:
                continue

            prediction = None
            try:
                prediction = score(row)
            except requests.RequestException as exc:
                print(f"prediction skipped: {exc}", flush=True)
            store(conn, row, prediction)
    finally:
        consumer.close()
        conn.close()


if __name__ == "__main__":
    main()
