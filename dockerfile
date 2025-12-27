ARG AIRFLOW_VERSION=2.9.2
ARG python_version=3.10

FROM apache/airflow:${AIRFLOW_VERSION}-python${python_version}

ENV AIRFLOW_HOME=/opt/airflow

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt