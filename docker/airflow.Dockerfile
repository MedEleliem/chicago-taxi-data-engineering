FROM apache/airflow:3.2.1-python3.12
USER root
RUN apt-get update && apt-get install -y --no-install-recommends openjdk-17-jre-headless && rm -rf /var/lib/apt/lists/*
RUN install -d -o airflow -g root /opt/chicago/data
USER airflow
COPY requirements-pipeline.txt /tmp/requirements-pipeline.txt
RUN pip install --no-cache-dir "apache-airflow==3.2.1" -r /tmp/requirements-pipeline.txt
WORKDIR /opt/chicago
COPY --chown=airflow:root src/ src/
COPY --chown=airflow:root scripts/ scripts/
COPY --chown=airflow:root dags/ dags/
COPY --chown=airflow:root config/ config/
COPY --chown=airflow:root frontend/ frontend/
COPY --chown=airflow:root tests/ tests/
