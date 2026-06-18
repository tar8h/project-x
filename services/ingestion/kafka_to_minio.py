import json
import os
import signal
import time
import uuid
from datetime import datetime, timezone

import boto3
from botocore.client import Config
from confluent_kafka import Consumer, KafkaException


STOP = False


def handle_signal(_signum, _frame) -> None:
    global STOP
    STOP = True


def env(name: str, default: str) -> str:
    return os.getenv(name, default)


def make_consumer() -> Consumer:
    return Consumer(
        {
            "bootstrap.servers": env("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
            "group.id": env("KAFKA_GROUP_ID", "kafka-to-minio"),
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )


def make_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=f"http://{env('MINIO_ENDPOINT', 'localhost:9000')}",
        aws_access_key_id=env("MINIO_ACCESS_KEY", "minioadmin"),
        aws_secret_access_key=env("MINIO_SECRET_KEY", "minioadmin"),
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )


def object_key() -> str:
    now = datetime.now(timezone.utc)
    return (
        f"raw/year={now:%Y}/month={now:%m}/day={now:%d}/hour={now:%H}/"
        f"sensor_raw_{now:%Y%m%dT%H%M%S}_{uuid.uuid4().hex[:8]}.ndjson"
    )


def flush_batch(s3, bucket: str, rows: list[str]) -> None:
    if not rows:
        return
    body = "\n".join(rows).encode("utf-8")
    s3.put_object(Bucket=bucket, Key=object_key(), Body=body, ContentType="application/x-ndjson")


def normalize_message(value: bytes) -> str:
    text = value.decode("utf-8", errors="replace")
    try:
        parsed = json.loads(text)
        return json.dumps(parsed, separators=(",", ":"), ensure_ascii=False)
    except json.JSONDecodeError:
        return json.dumps({"raw": text}, separators=(",", ":"), ensure_ascii=False)


def main() -> None:
    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    topic = env("KAFKA_TOPIC", "sensor.raw")
    bucket = env("MINIO_BUCKET", "sensor-raw")
    batch_size = int(env("BATCH_SIZE", "500"))
    flush_seconds = float(env("FLUSH_SECONDS", "15"))

    consumer = make_consumer()
    s3 = make_s3_client()
    consumer.subscribe([topic])

    rows: list[str] = []
    last_flush = time.monotonic()
    try:
        while not STOP:
            msg = consumer.poll(1.0)
            if msg is None:
                pass
            elif msg.error():
                print(f"kafka warning: {msg.error()}", flush=True)
                time.sleep(2)
                continue
            else:
                rows.append(normalize_message(msg.value()))

            now = time.monotonic()
            should_flush = len(rows) >= batch_size or (rows and now - last_flush >= flush_seconds)
            if should_flush:
                flush_batch(s3, bucket, rows)
                consumer.commit(asynchronous=False)
                rows.clear()
                last_flush = now
    finally:
        flush_batch(s3, bucket, rows)
        consumer.close()


if __name__ == "__main__":
    main()
