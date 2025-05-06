#!/bin/bash
set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE USER user_trust WITH PASSWORD 'a1~!@#$%^&*()_+';
    CREATE USER user_reject WITH PASSWORD 'a1~!@#$%^&*()_+';
    CREATE USER user_scram_sha_256 WITH PASSWORD 'a1~!@#$%^&*()_+';
    CREATE USER user_cert WITH PASSWORD 'a1~!@#$%^&*()_+';
    CREATE USER user_trust_hostnossl WITH PASSWORD 'a1~!@#$%^&*()_+';

    GRANT ALL PRIVILEGES ON DATABASE postgres TO user_trust;
    GRANT ALL PRIVILEGES ON DATABASE postgres TO user_reject;
    GRANT ALL PRIVILEGES ON DATABASE postgres TO user_scram_sha_256;
    GRANT ALL PRIVILEGES ON DATABASE postgres TO user_cert;
    GRANT ALL PRIVILEGES ON DATABASE postgres TO user_trust_hostnossl;
EOSQL

rm /var/lib/postgresql/data/pg_hba.conf

cat <<EOF >> /var/lib/postgresql/data/pg_hba.conf
local       all        postgres                                   trust
local       all        user_trust                                 trust
local       all        user_reject                                reject
local       all        user_scram_sha_256                         scram-sha-256
host        all        user_trust                     0.0.0.0/0   trust
host        all        user_reject                    0.0.0.0/0   reject
host        all        user_scram_sha_256             0.0.0.0/0   scram-sha-256
hostssl     all        user_trust                     0.0.0.0/0   trust
hostssl     all        user_reject                    0.0.0.0/0   reject
hostssl     all        user_scram_sha_256             0.0.0.0/0   scram-sha-256
hostssl     all        user_cert                      0.0.0.0/0   cert clientcert=verify-full
hostnossl   all        user_trust_hostnossl           0.0.0.0/0   trust
EOF
