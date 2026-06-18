FROM python:3.11-slim

WORKDIR /app
COPY services/stream/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY services/stream/kafka_to_timescale.py .

CMD ["python", "kafka_to_timescale.py"]
