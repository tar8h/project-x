FROM python:3.11-slim

WORKDIR /app
COPY services/generator/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY services/generator/sensor_generator.py .

CMD ["python", "sensor_generator.py"]
