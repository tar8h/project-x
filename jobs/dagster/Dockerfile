FROM python:3.11-slim

WORKDIR /app

COPY jobs/dagster/requirements.txt ./jobs/dagster/requirements.txt
RUN pip install --no-cache-dir -r jobs/dagster/requirements.txt

COPY jobs/dagster ./jobs/dagster

RUN mkdir -p /opt/dagster/dagster_home
COPY jobs/dagster/workspace.yaml /opt/dagster/dagster_home/workspace.yaml

ENV PYTHONPATH=/app
ENV DAGSTER_HOME=/opt/dagster/dagster_home

CMD ["dagster-webserver", "-h", "0.0.0.0", "-p", "3000", "-w", "/opt/dagster/dagster_home/workspace.yaml"]
