# Deployment Guide — VPS

Deploy the lakehouse stack on a Linux VPS (Ubuntu 22.04+ recommended).

## Prerequisites

| Requirement | Minimum |
|-------------|---------|
| RAM | 4 GB (full stack, 14 containers) |
| CPU | 2 cores |
| Docker | 24+ with Compose v2 |

```bash
curl -fsSL https://get.docker.com | sh
sudo usermod -aG docker $USER
# re-login so docker runs without sudo
```

## Deploy

```bash
git clone <your-repo-url> ~/lakehouse
cd ~/lakehouse
cp .env.example .env
```

Edit `.env` — only credentials (defaults in `.env.example` work for dev):

| Variable | Role |
|----------|------|
| `AIRFLOW_FERNET_KEY` / `AIRFLOW_JWT_SECRET` | Airflow secrets |
| `AIRFLOW_ADMIN_USER` / `AIRFLOW_ADMIN_PASSWORD` | UI login |
| `POSTGRES_USER` / `POSTGRES_PASSWORD` | Airflow metadata DB |
| `MINIO_ROOT_USER` / `MINIO_ROOT_PASSWORD` | Object storage |
| `CLICKHOUSE_USER` / `CLICKHOUSE_PASSWORD` | Gold layer DB |
| `METABASE_DB_USER` / `METABASE_DB_PASSWORD` | Metabase metadata DB |

Ports, Kafka address, versions → edit `docker-compose.yml`.

```bash
docker compose up -d
docker compose ps
```

## Verify Phase 0 (infrastructure)

```bash
# All services running; airflow-init exited successfully
docker compose ps

curl -s http://localhost:8080/api/v2/monitor/health   # Airflow
curl -s http://localhost:9000/minio/health/live       # MinIO
nc -zv 185.255.90.14 9092                             # Kafka (before Bronze)
```

## Service URLs

Replace `localhost` with your VPS IP when accessing remotely.

| Service | URL | Default credentials |
|---------|-----|---------------------|
| Airflow | http://localhost:8080 | admin / admin |
| Spark Master UI | http://localhost:8081 | — |
| Spark Worker UI | http://localhost:8082 | — |
| MinIO Console | http://localhost:9001 | minioadmin / minioadmin123 |
| MinIO API | http://localhost:9000 | same as above |
| Iceberg REST | http://localhost:8181 | — |
| ClickHouse HTTP | http://localhost:8123 | default / clickhouse123 |
| Metabase | http://localhost:3000 | create account on first visit |

Inside Docker network, use service names: `http://minio:9000`, `http://clickhouse:8123`, etc.

## MinIO buckets

Buckets are **not** created at startup. Create them per development phase — see [DEVELOPMENT.md](DEVELOPMENT.md).

## Firewall (public VPS)

Restrict UI ports to your IP:

```bash
sudo ufw allow OpenSSH
sudo ufw allow from <YOUR_IP> to any port 8080,9001,3000
sudo ufw enable
```

## Updates and reset

```bash
# Pull code changes and rebuild
git pull && docker compose up -d

# Stop containers, keep data
docker compose down

# Full reset (destroys all volumes and data)
docker compose down -v
docker compose up -d
```

## Troubleshooting

**Airflow first start is slow**

First startup runs `pip install` via `_PIP_ADDITIONAL_REQUIREMENTS`. Wait and check logs:

```bash
docker compose logs -f airflow-scheduler
```

**Airflow keeps restarting**

```bash
docker compose logs airflow-init
```

Check `AIRFLOW_FERNET_KEY` and `AIRFLOW_JWT_SECRET` are set in `.env`. Wait for `postgres` to be healthy.

**Port already in use**

Change the port mapping in `docker-compose.yml` (e.g. `"8090:8080"` for Airflow).

**Spark worker not connecting**

```bash
docker compose ps spark-master
docker compose logs spark-worker
```

**Iceberg REST not healthy**

Iceberg starts after MinIO is healthy. The `warehouse` bucket is only needed when you write Iceberg tables (Phase 2).
