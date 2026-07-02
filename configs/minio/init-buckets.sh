
set -e

mc alias set local http://minio:9000 "${MINIO_ROOT_USER}" "${MINIO_ROOT_PASSWORD}"

mc mb --ignore-existing local/bronze
mc mb --ignore-existing local/silver
mc mb --ignore-existing local/warehouse
mc mb --ignore-existing local/checkpoints

mc ls local

