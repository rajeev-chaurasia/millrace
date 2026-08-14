FROM apache/spark:4.0.1-python3

USER root

RUN python3 -m pip install --no-cache-dir \
    "boto3>=1.35,<2" \
    "psycopg[binary]>=3.2,<4" \
    "pydantic>=2.10,<3" \
    "pydantic-settings>=2.7,<3" \
    "pyyaml>=6.0,<7"

COPY src /opt/millrace/src

ENV PYTHONPATH=/opt/millrace/src
WORKDIR /opt/millrace

USER spark
