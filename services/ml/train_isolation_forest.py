import argparse
from pathlib import Path

import joblib
import mlflow
import numpy as np
import pandas as pd
from sklearn.ensemble import IsolationForest
from sklearn.metrics import average_precision_score, roc_auc_score
from sklearn.model_selection import train_test_split


FEATURES = ["temperature", "vibration", "voltage"]


def make_synthetic_history(rows: int, seed: int) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    machine_offsets = rng.normal(0, 1.5, rows)
    degradation = rng.beta(1.2, 8.0, rows)
    failure_bursts = rng.random(rows) < 0.06
    degradation = np.where(failure_bursts, rng.uniform(0.75, 1.0, rows), degradation)

    load = rng.uniform(0.2, 1.0, rows)
    temperature = 60 + load * 7 + degradation * 38 + machine_offsets + rng.normal(0, 1.4, rows)
    vibration = 0.25 + load * 0.08 + degradation * 1.20 + np.abs(rng.normal(0, 0.04, rows))
    voltage = 220 - degradation * rng.uniform(4, 20, rows) + rng.normal(0, 1.2, rows)
    label = (degradation > 0.78).astype(int)

    return pd.DataFrame(
        {
            "temperature": temperature,
            "vibration": vibration,
            "voltage": voltage,
            "synthetic_failure_label": label,
        }
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Train Isolation Forest for predictive maintenance.")
    parser.add_argument("--rows", type=int, default=20000)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--output", default="models/isolation_forest.joblib")
    parser.add_argument("--experiment", default="predictive-maintenance")
    parser.add_argument("--contamination", type=float, default=0.08)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    data = make_synthetic_history(args.rows, args.seed)
    train, test = train_test_split(data, test_size=0.25, random_state=args.seed, stratify=data["synthetic_failure_label"])

    model = IsolationForest(
        n_estimators=200,
        contamination=args.contamination,
        random_state=args.seed,
        n_jobs=-1,
    )

    mlflow.set_experiment(args.experiment)
    with mlflow.start_run(run_name="isolation-forest-synthetic") as run:
        model.fit(train[FEATURES])
        scores = -model.decision_function(test[FEATURES])
        labels = test["synthetic_failure_label"].to_numpy()

        roc_auc = roc_auc_score(labels, scores)
        avg_precision = average_precision_score(labels, scores)

        mlflow.log_params(
            {
                "rows": args.rows,
                "seed": args.seed,
                "contamination": args.contamination,
                "features": ",".join(FEATURES),
            }
        )
        mlflow.log_metrics({"roc_auc_synthetic": roc_auc, "average_precision_synthetic": avg_precision})

        output = Path(args.output)
        output.parent.mkdir(parents=True, exist_ok=True)
        joblib.dump(model, output)
        mlflow.log_artifact(str(output))

        print(f"run_id={run.info.run_id}")
        print(f"model_path={output}")
        print(f"roc_auc_synthetic={roc_auc:.4f}")
        print(f"average_precision_synthetic={avg_precision:.4f}")


if __name__ == "__main__":
    main()
