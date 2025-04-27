# check=skip=SecretsUsedInArgOrEnv

FROM postgres:17.4@sha256:fe3f571d128e8efadcd8b2fde0e2b73ebab6dbec33f6bfe69d98c682c7d8f7bd

ADD init.sh /docker-entrypoint-initdb.d/init.sh

ENV POSTGRES_PASSWORD=postgres

HEALTHCHECK CMD ["pg_isready", "-U", "postgres"]

CMD [ "postgres", "-c", "log_statement=all", "-c", "log_connections=on", "-c", "log_disconnections=on", "-c", "log_duration=on", "-c", "ssl=on", "-c", "ssl_cert_file=/certs/root_ca_1_postgres_17.crt.pem", "-c", "ssl_key_file=/certs/root_ca_1_postgres_17.key.pem", "-c", "ssl_ca_file=/certs/root_ca_1.crt.pem" ]
