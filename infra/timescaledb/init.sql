CREATE EXTENSION IF NOT EXISTS timescaledb;

CREATE TABLE IF NOT EXISTS sensor_readings (
    ts TIMESTAMPTZ NOT NULL,
    site TEXT NOT NULL,
    line TEXT NOT NULL,
    machine_id TEXT NOT NULL,
    temperature DOUBLE PRECISION,
    vibration DOUBLE PRECISION,
    voltage DOUBLE PRECISION,
    raw JSONB
);

SELECT create_hypertable('sensor_readings', 'ts', if_not_exists => TRUE);

CREATE INDEX IF NOT EXISTS ix_sensor_readings_machine_time
    ON sensor_readings (machine_id, ts DESC);

CREATE TABLE IF NOT EXISTS failure_predictions (
    ts TIMESTAMPTZ NOT NULL,
    machine_id TEXT NOT NULL,
    anomaly_score DOUBLE PRECISION NOT NULL,
    failure_probability DOUBLE PRECISION NOT NULL,
    model_name TEXT NOT NULL,
    model_version TEXT NOT NULL,
    features JSONB NOT NULL
);

SELECT create_hypertable('failure_predictions', 'ts', if_not_exists => TRUE);

CREATE INDEX IF NOT EXISTS ix_failure_predictions_machine_time
    ON failure_predictions (machine_id, ts DESC);

CREATE TABLE IF NOT EXISTS model_registry_shadow (
    model_name TEXT NOT NULL,
    model_version TEXT NOT NULL,
    registered_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    mlflow_run_id TEXT,
    artifact_uri TEXT,
    metrics JSONB,
    PRIMARY KEY (model_name, model_version)
);
