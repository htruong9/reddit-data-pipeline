FROM apache/airflow:2.8.1-python3.11

USER root

# System dependencies required by some Python packages
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        build-essential \
        libpq-dev \
    && rm -rf /var/lib/apt/lists/*

USER airflow

COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

# Copy project code into the image
COPY --chown=airflow:root dags/ /opt/airflow/dags/
COPY --chown=airflow:root etls/ /opt/airflow/etls/
COPY --chown=airflow:root pipelines/ /opt/airflow/pipelines/
COPY --chown=airflow:root utils/ /opt/airflow/utils/
COPY --chown=airflow:root config/ /opt/airflow/config/
