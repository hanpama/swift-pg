# check=skip=SecretsUsedInArgOrEnv

FROM postgres:17.4@sha256:fe3f571d128e8efadcd8b2fde0e2b73ebab6dbec33f6bfe69d98c682c7d8f7bd

ADD init.sh /docker-entrypoint-initdb.d/init.sh

ENV POSTGRES_PASSWORD=postgres
