FROM apache/airflow:3.1.6-python3.11

USER root

RUN apt-get update \
    && apt-get install --yes --no-install-recommends openjdk-17-jre-headless \
    && rm -rf /var/lib/apt/lists/* \
    && ln -s "$(dirname "$(dirname "$(readlink -f /usr/bin/java)")")" /usr/lib/jvm/java-17-openjdk \
    && python -m venv /opt/dbt-venv \
    && mkdir -p /opt/millrace/bin \
    && ln -s /opt/dbt-venv/bin/dbt /opt/millrace/bin/dbt \
    && chown -R airflow:root /opt/dbt-venv /opt/millrace

USER airflow

ARG AIRFLOW_CONSTRAINTS_URL=https://raw.githubusercontent.com/apache/airflow/constraints-3.1.6/constraints-3.11.txt

RUN pip install --no-cache-dir --constraint "${AIRFLOW_CONSTRAINTS_URL}" \
    "apache-airflow==3.1.6" \
    "apache-airflow-providers-fab>=2.4,<4" \
    "boto3>=1.35,<2" \
    "duckdb>=1.4,<1.5" \
    "prometheus-client>=0.21,<1" \
    "psycopg[binary]>=3.2,<4" \
    "pydantic>=2.10,<3" \
    "pydantic-settings>=2.7,<3" \
    "pyyaml>=6.0,<7" \
    "snowflake-connector-python>=3.12,<5"

RUN pip install --no-cache-dir "pyspark==4.0.1"

RUN /opt/dbt-venv/bin/pip install --no-cache-dir \
    "dbt-core>=1.10,<1.11" \
    "dbt-duckdb>=1.10,<1.11" \
    "dbt-snowflake>=1.10,<1.11" \
    "duckdb>=1.4,<1.5"

COPY --chown=airflow:root src /opt/millrace/src
COPY --chown=airflow:root dbt /opt/millrace/dbt
COPY --chown=airflow:root config /opt/millrace/config

ENV PYTHONPATH=/opt/millrace/src
# Points at the arch-neutral symlink created above (java-17-openjdk-amd64 vs
# -arm64 depends on the build host; this project is built natively on both
# amd64 CI runners and Apple Silicon developer machines).
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk
ENV PATH=/opt/millrace/bin:${PATH}
WORKDIR /opt/millrace
