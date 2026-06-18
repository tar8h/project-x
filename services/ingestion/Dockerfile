FROM python:3.11-slim

WORKDIR /app
COPY services/ingestion/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY services/ingestion/kafka_to_minio.py .

CMD ["python", "kafka_to_minio.py"]
