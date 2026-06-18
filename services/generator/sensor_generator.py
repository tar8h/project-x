import argparse
import json
import math
import random
import signal
import time
from dataclasses import dataclass
from datetime import datetime, timezone

import paho.mqtt.client as mqtt


SENSOR_TYPES = ("temperature", "vibration", "voltage")


@dataclass(frozen=True)
class MachineProfile:
    machine_id: str
    site: str
    line: str
    base_temperature: float
    base_vibration: float
    base_voltage: float


MACHINES = (
    MachineProfile("press-01", "plant-a", "line-1", 66.0, 0.28, 220.0),
    MachineProfile("press-02", "plant-a", "line-1", 63.0, 0.22, 221.0),
    MachineProfile("pump-01", "plant-a", "line-2", 58.0, 0.35, 219.0),
    MachineProfile("cnc-01", "plant-b", "line-1", 49.0, 0.18, 380.0),
)


class StopFlag:
    value = False


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


def degradation_factor(tick: int, machine_index: int) -> float:
    """Slow drift plus occasional synthetic failure episodes."""
    cycle = math.sin((tick + machine_index * 13) / 90.0)
    drift = max(0.0, cycle) * 0.35
    episode = 1.0 if (tick // 180 + machine_index) % 7 == 3 and tick % 180 > 120 else 0.0
    return min(1.0, drift + episode * 0.85)


def sample_values(profile: MachineProfile, tick: int, machine_index: int) -> dict[str, float]:
    deg = degradation_factor(tick, machine_index)
    load = 0.5 + 0.5 * math.sin((tick + machine_index * 8) / 25.0)

    temperature = profile.base_temperature + load * 5 + deg * 35 + random.gauss(0, 1.2)
    vibration = profile.base_vibration + load * 0.05 + deg * 1.15 + abs(random.gauss(0, 0.035))
    voltage_sag = deg * random.uniform(4.0, 18.0)
    voltage = profile.base_voltage - voltage_sag + random.gauss(0, 1.0)

    return {
        "temperature": round(temperature, 3),
        "vibration": round(vibration, 4),
        "voltage": round(voltage, 3),
        "degradation": round(deg, 4),
    }


def build_payload(profile: MachineProfile, values: dict[str, float], sensor_type: str) -> dict:
    payload = {
        "ts": utc_now(),
        "site": profile.site,
        "line": profile.line,
        "machine_id": profile.machine_id,
        "sensor_type": sensor_type,
        "value": values[sensor_type],
        "temperature": values["temperature"],
        "vibration": values["vibration"],
        "voltage": values["voltage"],
        "synthetic_degradation": values["degradation"],
    }
    payload["synthetic_failure_label"] = int(values["degradation"] > 0.78)
    return payload


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Synthetic MQTT telemetry generator.")
    parser.add_argument("--mqtt-host", default="localhost")
    parser.add_argument("--mqtt-port", type=int, default=1883)
    parser.add_argument("--rate", type=float, default=1.0, help="Messages per machine per second.")
    parser.add_argument("--qos", type=int, default=1)
    parser.add_argument("--seed", type=int, default=42)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    random.seed(args.seed)

    def handle_signal(_signum, _frame) -> None:
        StopFlag.value = True

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="synthetic-edge-generator")
    client.connect(args.mqtt_host, args.mqtt_port, keepalive=30)
    client.loop_start()

    delay = max(0.05, 1.0 / args.rate)
    tick = 0
    try:
        while not StopFlag.value:
            for idx, profile in enumerate(MACHINES):
                values = sample_values(profile, tick, idx)
                for sensor_type in SENSOR_TYPES:
                    topic = f"factory/{profile.machine_id}/{sensor_type}"
                    payload = build_payload(profile, values, sensor_type)
                    client.publish(topic, json.dumps(payload), qos=args.qos)
            tick += 1
            time.sleep(delay)
    finally:
        client.loop_stop()
        client.disconnect()


if __name__ == "__main__":
    main()
