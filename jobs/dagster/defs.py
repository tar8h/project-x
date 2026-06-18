from pathlib import Path

from dagster import AssetExecutionContext, Definitions, MetadataValue, ScheduleDefinition, asset, define_asset_job


ROOT = Path(__file__).resolve().parents[2]


@asset(group_name="features")
def batch_features(context: AssetExecutionContext) -> None:
    context.log.info("Run Spark batch feature engineering outside local dev if Spark is not installed.")
    context.log.info("Script: jobs/spark/batch_feature_engineering.py")


@asset(group_name="models", deps=[batch_features])
def isolation_forest_model(context: AssetExecutionContext) -> dict[str, str]:
    output = ROOT / "models" / "isolation_forest.joblib"
    command = f"python services/ml/train_isolation_forest.py --output {output}"
    context.add_output_metadata(
        {
            "training_command": MetadataValue.text(command),
            "mlflow_tracking_uri": MetadataValue.text("http://localhost:5000"),
            "model_path": MetadataValue.path(output),
        }
    )
    context.log.info("Model training is executed as a separate ML job in this local MVP.")
    context.log.info(command)
    return {"status": "ready_to_train", "command": command}


nightly_training_job = define_asset_job(
    name="nightly_training_job",
    selection=[isolation_forest_model],
)


defs = Definitions(
    assets=[batch_features, isolation_forest_model],
    jobs=[nightly_training_job],
    schedules=[
        ScheduleDefinition(
            name="nightly_model_training",
            cron_schedule="0 2 * * *",
            job=nightly_training_job,
        )
    ],
)
