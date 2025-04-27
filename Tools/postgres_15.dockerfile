# check=skip=SecretsUsedInArgOrEnv

FROM postgres:15.12@sha256:fe45ed1a824b81c0c9c605890963b67501758ca8c946db89089c85ce0f88e974

ADD init.sh /docker-entrypoint-initdb.d/init.sh

ENV POSTGRES_PASSWORD=postgres

HEALTHCHECK CMD ["pg_isready", "-U", "postgres"]

CMD [ "postgres", "-c", "log_statement=all", "-c", "log_connections=on", "-c", "log_disconnections=on", "-c", "log_duration=on", "-c", "ssl=on", "-c", "ssl_cert_file=/certs/server.crt", "-c", "ssl_key_file=/certs/server.key" ]
