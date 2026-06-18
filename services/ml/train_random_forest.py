import argparse
from pathlib import Path

import joblib
import mlflow
import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import classification_report, roc_auc_score
from sklearn.model_selection import train_test_split


FEATURES = ["temperature", "vibration", "voltage"]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Train Random Forest when labeled failure data exists.")
    parser.add_argument("--input", required=True, help="CSV/parquet file with feature columns and failure_label.")
    parser.add_argument("--label", default="failure_label")
    parser.add_argument("--output", default="models/random_forest.joblib")
    parser.add_argument("--experiment", default="predictive-maintenance")
    return parser.parse_args()


def read_dataset(path: str) -> pd.DataFrame:
    if path.endswith(".parquet"):
        return pd.read_parquet(path)
    return pd.read_csv(path)


def main() -> None:
    args = parse_args()
    data = read_dataset(args.input)
    missing = [col for col in FEATURES + [args.label] if col not in data.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    train, test = train_test_split(data, test_size=0.25, random_state=42, stratify=data[args.label])
    model = RandomForestClassifier(
        n_estimators=400,
        max_depth=12,
        min_samples_leaf=5,
        class_weight="balanced",
        random_state=42,
        n_jobs=-1,
    )

    mlflow.set_experiment(args.experiment)
    with mlflow.start_run(run_name="random-forest-labeled") as run:
        model.fit(train[FEATURES], train[args.label])
        probabilities = model.predict_proba(test[FEATURES])[:, 1]
        predictions = model.predict(test[FEATURES])
        roc_auc = roc_auc_score(test[args.label], probabilities)

        mlflow.log_params(model.get_params())
        mlflow.log_metric("roc_auc", roc_auc)
        mlflow.log_text(classification_report(test[args.label], predictions), "classification_report.txt")

        output = Path(args.output)
        output.parent.mkdir(parents=True, exist_ok=True)
        joblib.dump(model, output)
        mlflow.log_artifact(str(output))

        print(f"run_id={run.info.run_id}")
        print(f"model_path={output}")
        print(f"roc_auc={roc_auc:.4f}")


if __name__ == "__main__":
    main()
