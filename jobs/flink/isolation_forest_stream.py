import json
import os
from datetime import datetime, timezone

import psycopg2
import requests
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.datastream.functions import MapFunction


def env(name: str, default: str) -> str:
    return os.getenv(name, default)


class ParseSensorMessage(MapFunction):
    def map(self, value: str):
        payload = json.loads(value)
        fields = payload.get("fields", payload)
        tags = payload.get("tags", payload)

        return {
            "ts": payload.get("timestamp") or payload.get("ts") or datetime.now(timezone.utc).isoformat(),
            "site": tags.get("site", "unknown"),
            "line": tags.get("line", "unknown"),
            "machine_id": tags.get("machine_id") or payload.get("machine_id"),
            "temperature": float(fields.get("temperature")),
            "vibration": float(fields.get("vibration")),
            "voltage": float(fields.get("voltage")),
            "raw": payload,
        }


class ScoreAndStore(MapFunction):
    def open(self, runtime_context):
        self.api_url = env("INFERENCE_URL", "http://inference-api:8080/predict")
        self.conn = psycopg2.connect(
            host=env("POSTGRES_HOST", "timescaledb"),
            port=int(env("POSTGRES_PORT", "5432")),
            dbname=env("POSTGRES_DB", "maintenance"),
            user=env("POSTGRES_USER", "maintenance"),
            password=env("POSTGRES_PASSWORD", "maintenance"),
        )
        self.conn.autocommit = True

    def map(self, row: dict):
        response = requests.post(
            self.api_url,
            json={
                "machine_id": row["machine_id"],
                "temperature": row["temperature"],
                "vibration": row["vibration"],
                "voltage": row["voltage"],
            },
            timeout=2,
        )
        response.raise_for_status()
        prediction = response.json()

        with self.conn.cursor() as cursor:
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
        return prediction

    def close(self):
        self.conn.close()


def main() -> None:
    env_settings = StreamExecutionEnvironment.get_execution_environment()
    env_settings.set_parallelism(int(env("FLINK_PARALLELISM", "1")))

    source = (
        KafkaSource.builder()
        .set_bootstrap_servers(env("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"))
        .set_topics(env("KAFKA_TOPIC_RAW", "sensor.raw"))
        .set_group_id(env("KAFKA_GROUP_ID", "flink-isolation-forest"))
        .set_starting_offsets(KafkaOffsetsInitializer.latest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    (
        env_settings.from_source(source, watermark_strategy=None, source_name="sensor.raw")
        .map(ParseSensorMessage(), output_type=Types.MAP(Types.STRING(), Types.PICKLED_BYTE_ARRAY()))
        .map(ScoreAndStore(), output_type=Types.STRING())
        .print()
    )

    env_settings.execute("project-x-isolation-forest-stream")


if __name__ == "__main__":
    main()
