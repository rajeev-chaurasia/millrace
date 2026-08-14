FROM alpine:3.22.1

RUN apk add --no-cache curl jq

COPY infra/debezium/postgres-connector.json.template /config/postgres-connector.json.template
COPY docker/connect-init/register.sh /usr/local/bin/register.sh

RUN sed -i 's/\r$//' /usr/local/bin/register.sh \
    && chmod 0555 /usr/local/bin/register.sh

USER 65532:65532
ENTRYPOINT ["/usr/local/bin/register.sh"]
