FROM python:3.11.13-slim-bookworm

RUN apt-get update \
    && apt-get install --yes --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir \
    "boto3>=1.35,<2" \
    "duckdb>=1.4,<1.5" \
    "pandas>=2.2,<3" \
    "plotly>=6.0,<7" \
    "pydantic>=2.10,<3" \
    "pydantic-settings>=2.7,<3" \
    "streamlit>=1.42,<2"

RUN useradd --create-home --uid 10001 millrace
COPY --chown=millrace:millrace src /opt/millrace/src
COPY --chown=millrace:millrace dashboard /opt/millrace/dashboard

USER millrace
ENV PYTHONPATH=/opt/millrace:/opt/millrace/src
WORKDIR /opt/millrace
