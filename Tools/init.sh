#!/bin/bash
set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE USER local_trust WITH PASSWORD 'a1~!@#$%^&*()_+';
    CREATE USER local_reject WITH PASSWORD 'a1~!@#$%^&*()_+';
    CREATE USER local_scram_sha_256 WITH PASSWORD 'a1~!@#$%^&*()_+';
    CREATE USER host_trust WITH PASSWORD 'a1~!@#$%^&*()_+';
    CREATE USER host_reject WITH PASSWORD 'a1~!@#$%^&*()_+';
    CREATE USER host_scram_sha_256 WITH PASSWORD 'a1~!@#$%^&*()_+';
    CREATE USER hostssl_trust WITH PASSWORD 'a1~!@#$%^&*()_+';
    CREATE USER hostssl_reject WITH PASSWORD 'a1~!@#$%^&*()_+';
    CREATE USER hostssl_scram_sha_256 WITH PASSWORD 'a1~!@#$%^&*()_+';
    CREATE USER hostssl_clientcert_verify_ca WITH PASSWORD 'a1~!@#$%^&*()_+';
    CREATE USER hostssl_clientcert_verify_full WITH PASSWORD 'a1~!@#$%^&*()_+';
    CREATE USER hostnossl_trust WITH PASSWORD 'a1~!@#$%^&*()_+';
    CREATE USER hostnossl_reject WITH PASSWORD 'a1~!@#$%^&*()_+';
    CREATE USER hostnossl_scram_sha_256 WITH PASSWORD 'a1~!@#$%^&*()_+';
    GRANT ALL PRIVILEGES ON DATABASE postgres TO local_trust;
    GRANT ALL PRIVILEGES ON DATABASE postgres TO local_reject;
    GRANT ALL PRIVILEGES ON DATABASE postgres TO local_scram_sha_256;
    GRANT ALL PRIVILEGES ON DATABASE postgres TO host_trust;
    GRANT ALL PRIVILEGES ON DATABASE postgres TO host_reject;
    GRANT ALL PRIVILEGES ON DATABASE postgres TO host_scram_sha_256;
    GRANT ALL PRIVILEGES ON DATABASE postgres TO hostssl_trust;
    GRANT ALL PRIVILEGES ON DATABASE postgres TO hostssl_reject;
    GRANT ALL PRIVILEGES ON DATABASE postgres TO hostssl_scram_sha_256;
    GRANT ALL PRIVILEGES ON DATABASE postgres TO hostssl_clientcert_verify_ca;
    GRANT ALL PRIVILEGES ON DATABASE postgres TO hostssl_clientcert_verify_full;
    GRANT ALL PRIVILEGES ON DATABASE postgres TO hostnossl_trust;
    GRANT ALL PRIVILEGES ON DATABASE postgres TO hostnossl_reject;
    GRANT ALL PRIVILEGES ON DATABASE postgres TO hostnossl_scram_sha_256;
EOSQL

rm /var/lib/postgresql/data/pg_hba.conf

cat <<EOF >> /var/lib/postgresql/data/pg_hba.conf
local       all        postgres                                   trust
local       all        local_trust                                trust
local       all        local_reject                               trust
local       all        local_scram_sha_256                        trust
host        all        host_trust                     0.0.0.0/0   trust
host        all        host_reject                    0.0.0.0/0   reject
host        all        host_scram_sha_256             0.0.0.0/0   scram-sha-256
hostssl     all        hostssl_trust                  0.0.0.0/0   trust
hostssl     all        hostssl_reject                 0.0.0.0/0   reject
hostssl     all        hostssl_scram_sha_256          0.0.0.0/0   scram-sha-256
hostssl     all        hostssl_clientcert_verify_full 0.0.0.0/0   cert clientcert=verify-full
hostnossl   all        hostnossl_trust                0.0.0.0/0   trust
hostnossl   all        hostnossl_reject               0.0.0.0/0   reject
hostnossl   all        hostnossl_scram_sha_256        0.0.0.0/0   scram-sha-256
EOF
