import os
from pathlib import Path

import joblib
import numpy as np
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field


FEATURE_ORDER = ["temperature", "vibration", "voltage"]


class PredictionRequest(BaseModel):
    machine_id: str = Field(..., examples=["press-01"])
    temperature: float = Field(..., examples=[72.5])
    vibration: float = Field(..., examples=[0.42])
    voltage: float = Field(..., examples=[220.1])


class PredictionResponse(BaseModel):
    machine_id: str
    anomaly_score: float
    failure_probability: float
    is_alert: bool
    model_name: str
    model_version: str


def load_model():
    model_path = Path(os.getenv("MODEL_PATH", "models/isolation_forest.joblib"))
    if not model_path.exists():
        return None
    return joblib.load(model_path)


app = FastAPI(title="Predictive Maintenance Inference API", version="0.1.0")
MODEL = load_model()
THRESHOLD = float(os.getenv("ANOMALY_THRESHOLD", "0.65"))


def probability_from_isolation_score(score: float) -> float:
    # IsolationForest decision_function is higher for normal samples.
    probability = 1.0 / (1.0 + np.exp(8.0 * score))
    return float(np.clip(probability, 0.0, 1.0))


@app.get("/health")
def health() -> dict:
    return {
        "status": "ok" if MODEL is not None else "model_missing",
        "model_loaded": MODEL is not None,
    }


@app.post("/predict", response_model=PredictionResponse)
def predict(request: PredictionRequest) -> PredictionResponse:
    if MODEL is None:
        raise HTTPException(status_code=503, detail="Model is not loaded. Train it and mount MODEL_PATH.")

    features = np.array([[request.temperature, request.vibration, request.voltage]], dtype=float)
    score = float(MODEL.decision_function(features)[0])
    probability = probability_from_isolation_score(score)

    return PredictionResponse(
        machine_id=request.machine_id,
        anomaly_score=score,
        failure_probability=probability,
        is_alert=probability >= THRESHOLD,
        model_name="isolation_forest",
        model_version=os.getenv("MODEL_VERSION", "local"),
    )
