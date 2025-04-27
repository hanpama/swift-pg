# check=skip=SecretsUsedInArgOrEnv

FROM postgres:16.8@sha256:cef2d22004db69e3d601442ca4cac142adda0987ad7ca4f28c4e998bef690951

ADD init.sh /docker-entrypoint-initdb.d/init.sh

ENV POSTGRES_PASSWORD=postgres

HEALTHCHECK CMD ["pg_isready", "-U", "postgres"]

CMD [ "postgres", "-c", "log_statement=all", "-c", "log_connections=on", "-c", "log_disconnections=on", "-c", "log_duration=on", "-c", "ssl=on", "-c", "ssl_cert_file=/certs/server.crt", "-c", "ssl_key_file=/certs/server.key" ]
