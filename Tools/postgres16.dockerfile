# check=skip=SecretsUsedInArgOrEnv

FROM postgres:16.8@sha256:cef2d22004db69e3d601442ca4cac142adda0987ad7ca4f28c4e998bef690951

ADD init.sh /docker-entrypoint-initdb.d/init.sh

ENV POSTGRES_PASSWORD=postgres
