# Development Guide — Phased Workflow

Build the project in PDF phase order. Do not skip ahead — each phase depends on the previous one.

```
Kafka (external) → Bronze → Silver → Gold → Metabase dashboards
```

## Phase 0 — Infrastructure

**Goal:** Stack running locally or on VPS.

```bash
cp .env.example .env
docker compose up -d
docker compose ps
```

**Checklist:**
- [ ] All services healthy (or `airflow-init` exited successfully)
- [ ] Airflow UI opens at :8080
- [ ] MinIO Console opens at :9001
- [ ] Kafka reachable: `nc -zv <KAFKA_HOST> 9092`

No MinIO buckets needed yet.

---

## Phase 1 — Bronze layer

**PDF goal:** Spark Structured Streaming reads Kafka, enforces schema, writes Parquet to MinIO.

### Create buckets (before coding)

MinIO Console → http://localhost:9001 → **Buckets → Create**:

| Bucket | Purpose |
|--------|---------|
| `bronze` | Raw Parquet files |
| `checkpoints` | Spark streaming offsets |

Or from the host (replace credentials from `.env`):

```bash
docker run --rm --network modern_data_analytics_platform_datalake \
  -e MINIO_ROOT_USER=minioadmin \
  -e MINIO_ROOT_PASSWORD=minioadmin123 \
  minio/mc:RELEASE.2024-12-18T13-15-42Z sh -c '
    mc alias set local http://minio:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" &&
    mc mb --ignore-existing local/bronze local/checkpoints &&
    mc ls local
  '
```

### Build

| What | Where |
|------|-------|
| Spark Structured Streaming job | `src/bronze/` |
| Shared schemas / helpers | `src/common/` |

**Requirements (from PDF):**
- Read from Kafka topics (transactional + clickstream)
- Cast string timestamps to `TimestampType`
- Micro-batch ingestion
- Write compressed Parquet partitioned by ingestion date
- Checkpoint location: `s3://checkpoints/bronze/...`

### Verify

- [ ] Parquet files visible in MinIO bucket `bronze`
- [ ] Spark UI shows running application (:8081)

---

## Phase 2 — Silver layer

**PDF goal:** Airflow-orchestrated batch ETL, Iceberg tables, Kimball star schema.

### Create bucket

| Bucket | Purpose |
|--------|---------|
| `warehouse` | Iceberg catalog path (`ICEBERG_WAREHOUSE=s3://warehouse`) |

Create in MinIO Console before writing Iceberg tables.

### Build

| What | Where |
|------|-------|
| Cleansing + Iceberg transforms | `src/silver/` |
| Airflow DAGs | `workflow/dags/` |
| Task functions | `workflow/tasks/` |
| Shared utilities | `workflow/utils/` |

**Requirements (from PDF):**
- Iceberg tables on MinIO via REST catalog (:8181)
- Data cleansing (nulls, dedup, type fixes)
- Kimball star schema:
  - Dimensions: `dim_user`, `dim_product`, `dim_date` (SCD Type 2 for price history)
  - Facts: `fact_order`, `fact_behavioral_events`

### Verify

- [ ] Iceberg tables queryable via Spark
- [ ] Airflow DAG runs successfully
- [ ] Data in `s3://warehouse/`

---

## Phase 3 — Gold layer

**PDF goal:** Denormalized One Big Table (OBT) in ClickHouse for fast OLAP.

No new MinIO buckets.

### Build

| What | Where |
|------|-------|
| ClickHouse DDL | `sql/clickhouse/` (auto-runs on first ClickHouse start) |
| Silver → Gold ETL | `src/gold/` |
| Airflow DAG | `workflow/dags/` |

**Requirements (from PDF):**
- Flatten star schema into OBT
- Optimize with sorting keys and partition keys
- Load into ClickHouse database `lakehouse`

### Verify

- [ ] Tables exist in ClickHouse
- [ ] Aggregation queries return results quickly

---

## Phase 4 — Metabase dashboards

**PDF goal:** Business dashboards connected to ClickHouse.

- Connect Metabase to ClickHouse at http://localhost:3000
- Build dashboards from PDF section 4:
  - Conversion funnel by device
  - User behavior funnel
  - Revenue and discount metrics
  - Cohort analysis
  - Revenue by category

Optional: save queries in `sql/metabase/`.

---

## Phase 5 — Bonus monitoring

**PDF goal:** Near-real-time pipeline monitoring via Kafka.

- Monitoring DAG in `workflow/dags/`
- Health checks: order volume, funnel ratios, anomaly detection
- Publish alerts to a Kafka topic

---

## Git workflow

- Never commit `.env`
- Branch per phase: `feature/bronze-streaming`, `feature/silver-iceberg`, etc.
- `main` = stable infrastructure

## Folder map

```
src/bronze/          Spark streaming (Phase 1)
src/silver/          Iceberg ETL (Phase 2)
src/gold/            ClickHouse load (Phase 3)
src/common/          Shared code
workflow/dags/       Airflow DAGs (Phase 2+)
workflow/tasks/      Task callables
workflow/utils/      MinIO, ClickHouse, Iceberg helpers
sql/clickhouse/      Gold DDL (Phase 3)
```

## Future compose changes

The full 14-container stack runs by default. Later you can add Compose profiles to start only the services needed for the current phase (e.g. MinIO + Spark for Phase 1).
