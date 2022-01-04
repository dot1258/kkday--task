FROM apache/airflow:2.2.3

USER root

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    gcc \
    g++ \
    build-essential \
    python-dev \
    python3-dev && \
    rm -rf /var/lib/apt/lists/*

USER airflow

COPY --chown=airflow:root ./requirements.txt /opt/airflow/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt