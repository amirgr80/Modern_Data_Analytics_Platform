# مستندات طراحی زیرساخت — چرا این‌طور پیاده‌سازی شده؟

این سند توضیح می‌دهد **چرا** هر بخش از پروژه Lakehouse بوت‌کمپ مهندسی داده این‌طور طراحی شده است.
هدف: یک فایل مرجع فارسی برای خودت، منتور، و deploy روی VPS.

---

## ۱. نمای کلی معماری

پروژه یک **Lakehouse** با معماری Medallion (برنز / نقره / طلا) است:

```
Kafka (خارجی) → Spark Streaming → MinIO (Bronze)
                                      ↓
                              Airflow (Batch ETL)
                                      ↓
                              Iceberg (Silver)
                                      ↓
                              ClickHouse (Gold)
                                      ↓
                                 Metabase
```

| لایه | تکنولوژی | نقش |
|------|----------|-----|
| Bronze | Spark + MinIO | ingest خام از Kafka به Parquet |
| Silver | Airflow + Iceberg | پاکسازی، مدل Kimball، جداول ACID |
| Gold | ClickHouse | OBT برای OLAP سریع |
| Viz | Metabase | داشبورد BI |

---

## ۲. چرا همه‌چیز در `docker-compose.yml` متمرکز است؟

### ۲.۱ فلسفه ساده‌سازی

قبلاً برای Airflow سه فایل لازم بود:
- `workflow/Dockerfile`
- `workflow/requirements.txt`
- `docker-compose.yml`

الان **همه تنظیمات Airflow** (ایمیج + پکیج‌ها) داخل [`docker-compose.yml`](../docker-compose.yml) است:

```yaml
image: apache/airflow:3.2.1-python3.12
_PIP_ADDITIONAL_REQUIREMENTS: >-
  apache-airflow-providers-celery
  apache-airflow-providers-postgres
  ...
```

**چرا این کار را کردیم؟**

| مزیت | توضیح |
|------|--------|
| یک فایل مرکزی | فقط `docker-compose.yml` + `.env` — کمتر گشتن بین فایل‌ها |
| بدون build | `docker compose up -d` بدون `--build` |
| ایمیج رسمی | مستقیم از Apache Airflow، بدون لایه Dockerfile اضافه |
| تغییر آسان | پکیج جدید → یک خط به `_PIP_ADDITIONAL_REQUIREMENTS` اضافه کن |

**معامله (trade-off):**

- **اولین استارت Airflow کندتر است** — entrypoint رسمی Airflow موقع بالا آمدن کانتینر `pip install` می‌زند.
- **نیاز به اینترنت** در اولین استارت (یا بعد از `docker compose down -v`).
- استارت‌های بعدی معمولاً سریع‌ترند چون pip کش می‌کند.

> اگر بعداً پروژه بزرگ شد و استارت خیلی کند شد، می‌توانی دوباره Dockerfile برگردانی. برای بوت‌کمپ و VPS، سادگی مهم‌تر است.

### ۲.۲ YAML Anchorها (`x-airflow-common` و `x-pipeline-env`)

به‌جای کپی‌کردن ۵۰ خط تکراری برای هر سرویس:

- **`x-airflow-common`** — تنظیمات مشترک ۵ سرویس Airflow (env، volume، شبکه)
- **`x-pipeline-env`** — متغیرهای مشترک Spark (MinIO، Kafka، Iceberg)

یک env جدید اضافه کنی → فقط **یک جا** تغییر می‌دهی.

### ۲.۳ Spark جدا از Airflow

قبلاً `spark-master` اشتباهاً از `*airflow-common` ارث می‌برد و image/build Airflow را می‌کشید.

الان Spark **مستقل** است:
- ایمیج: `apache/spark:3.5.3` (طبق صورت‌مسئله)
- فقط env پایپلاین (`*pipeline-env`) را می‌گیرد
- به postgres/redis وابسته نیست

---

## ۳. سرویس‌ها — یکی‌یکی

### ۳.۱ Kafka (خارج از Compose)

**چرا داخل compose نیست؟**
- بوت‌کمپ خوشه Kafka را فراهم کرده (آدرس در `docker-compose.yml`: `185.255.90.14:9092`)
- Kafka اضافه = RAM بیشتر + تداخل پورت
- Spark و Airflow فقط به bootstrap server وصل می‌شوند

### ۳.۲ Apache Spark (master + worker)

| تصمیم | چرایی |
|-------|--------|
| ایمیج رسمی `apache/spark` | صریحاً در PDF خواسته شده |
| master/worker جدا | cluster واقعی؛ worker قابل scale |
| `./src` mount شده | jobهای Bronze/Silver/Gold بدون rebuild |
| Structured Streaming | فاز Bronze — micro-batch از Kafka |

### ۳.۳ MinIO

| تصمیم | چرایی |
|-------|--------|
| S3-compatible | Parquet Bronze + warehouse Iceberg بدون AWS |
| بدون init خودکار باکت | **باکت در هر فاز جدا ساخته می‌شود** (هم‌راستا با PDF) |
| Named volume `minio_data` | داده بین restart حفظ می‌شود |

**باکت‌ها per phase:**

| فاز | باکت | کاربرد |
|-----|------|--------|
| Bronze | `bronze` | Parquet خام |
| Bronze | `checkpoints` | offsetهای Spark Streaming |
| Silver | `warehouse` | مسیر `ICEBERG_WAREHOUSE=s3://warehouse` |

### ۳.۴ Iceberg REST Catalog

Iceberg برای metadata به catalog نیاز دارد.

**چرا REST Catalog؟**
- سبک‌ترین گزینه برای محیط آموزشی
- بدون Hadoop، بدون Hive Metastore
- HTTP ساده — Spark و Airflow هر دو می‌توانند وصل شوند

**چرا به MinIO healthy وابسته است؟**
- Iceberg فایل‌ها را روی MinIO (S3) ذخیره می‌کند
- باکت `warehouse` موقع **نوشتن جدول** لازم است، نه موقع استارت سرویس

### ۳.۵ Apache Airflow

| تصمیم | چرایی |
|-------|--------|
| **CeleryExecutor** | PDF صریحاً خواسته؛ worker جدا = مقیاس‌پذیر |
| **Redis** | broker صف Celery — سبک و استاندارد |
| **Postgres (Airflow)** | metadata DAGها، run history، connections |
| **`airflow-init`** | migrate DB + ساخت admin — یک‌بار قبل از scheduler |
| **Simple Auth Manager** | login ساده برای dev (`admin/admin`) |
| **DAG paused at creation** | ایمن — DAG جدید خودکار run نمی‌شود |

**پکیج‌های `_PIP_ADDITIONAL_REQUIREMENTS`:**

| پکیج | کاربرد |
|------|--------|
| `providers-celery` | CeleryExecutor |
| `providers-postgres` | اتصال DB Airflow |
| `providers-amazon` | MinIO (S3 API) |
| `providers-apache-spark` | SparkSubmitOperator |
| `providers-common-sql` | SQL operators |
| `confluent-kafka` | consume/produce Kafka |
| `clickhouse-connect` | لود Gold |
| `pandas` / `pyarrow` | transform داده |

### ۳.۶ ClickHouse

| تصمیم | چرایی |
|-------|--------|
| OLAP columnar | aggregation روی میلیون‌ها ردیف — سریع |
| لایه Gold | OBT برای Metabase بدون JOIN سنگین |
| `sql/clickhouse/` mount | DDL اولین بار خودکار اجرا می‌شود |

### ۳.۷ Metabase + Postgres جدا

| تصمیم | چرایی |
|-------|--------|
| Metabase | BI بدون کدنویسی dashboard |
| Postgres جدا (`postgres-metabase`) | metadata Metabase ≠ metadata Airflow |
| وابستگی به ClickHouse healthy | قبل از UI، منبع داده آماده باشد |

---

## ۴. شبکه، volume، و startup

### ۴.۱ شبکه `datalake`

- DNS داخلی Docker: `minio`، `clickhouse`، `spark-master` ...
- جدا از کانتینرهای دیگر روی سیستم
- داخل DAG/Spark از **نام سرویس** استفاده کن، نه `localhost`

### ۴.۲ Named Volume vs Bind Mount

| نوع | برای چه | مثال |
|-----|---------|------|
| Named volume | داده پایدار DB/storage | `minio_data`, `postgres_data` |
| Bind mount | کدی که edit می‌کنی | `workflow/dags`, `src/` |

### ۴.۳ `depends_on` با condition

| condition | مثال | چرایی |
|-----------|------|--------|
| `service_healthy` | postgres → airflow | DB واقعاً ready باشد |
| `service_completed_successfully` | airflow-init → scheduler | migrate تمام شده باشد |

`depends_on` ساده فقط **ترتیب start** را تضمین می‌کند، نه readiness.

---

## ۵. تقسیم تنظیمات: compose vs `.env`

| کجا | چه چیزهایی |
|-----|------------|
| **`docker-compose.yml`** | پورت‌ها، نسخه image، Kafka، timezone، نام DB، Spark cores، Iceberg warehouse |
| **`.env`** | فقط user / password / secret |

### متغیرهای `.env`

| متغیر | نقش |
|-------|-----|
| `AIRFLOW_FERNET_KEY` | رمزنگاری connection در DB |
| `AIRFLOW_JWT_SECRET` | توکن API Airflow 3.x |
| `AIRFLOW_ADMIN_*` | login UI |
| `POSTGRES_*` | DB متادیتای Airflow |
| `MINIO_ROOT_*` | دسترسی S3/MinIO |
| `CLICKHOUSE_*` | DB لایه Gold |
| `METABASE_DB_*` | DB متادیتای Metabase |

**`.env` در Git commit نشود** — `.gitignore` این را enforce می‌کند.

برای تغییر پورت یا Kafka → `docker-compose.yml` را ویرایش کن.

---

## ۶. فازبندی توسعه (هم‌راستا با PDF)

| فاز | کار | زیرساخت |
|-----|-----|---------|
| 0 | stack up | `docker compose up -d` |
| 1 Bronze | Spark Streaming → Parquet | MinIO buckets: `bronze`, `checkpoints` |
| 2 Silver | Iceberg + Kimball | bucket: `warehouse` + Airflow DAGs |
| 3 Gold | OBT → ClickHouse | `sql/clickhouse/` |
| 4 Viz | Metabase dashboards | اتصال به ClickHouse |
| 5 Bonus | monitoring | DAG نظارت real-time |

جزئیات: [DEVELOPMENT.md](DEVELOPMENT.md)

---

## ۷. Deploy روی VPS

```bash
git clone <repo> && cd lakehouse
cp .env.example .env
docker compose up -d
```

حداقل 4GB RAM. جزئیات: [DEPLOYMENT.md](DEPLOYMENT.md)

---

## ۸. pin کردن نسخه image

| سرویس | tag |
|-------|-----|
| Airflow | `3.2.1-python3.12` |
| Spark | `3.5.3` |
| MinIO | `RELEASE.2024-12-18T13-15-42Z` |
| Iceberg REST | `1.6.0` |
| ClickHouse | `24.8` |
| Metabase | `v0.51.4` |

**چرا `:latest` نمی‌زنیم؟** rebuild ممکن است نسخه عوض شود و stack بشکند.

---

## ۹. چیزهایی که عمداً ساده نگه داشتیم

| موضوع | وضعیت فعلی | بعداً اگر لازم شد |
|-------|------------|-------------------|
| Dockerfile Airflow | حذف شد — pip در compose | برگرداندن برای prod |
| init باکت MinIO | per-phase دستی | اسکریپت یا profile |
| Compose profiles | نداریم — full stack | `core` / `airflow` / `viz` |
| Kubernetes | نداریم | bonus PDF |
| CI/CD | نداریم | GitHub Actions |

---

## ۱۰. ساختار پوشه‌ها

```
docker-compose.yml    ← همه سرویس‌ها + پورت‌ها + Kafka + pip Airflow
.env.example          ← فقط user / password / secret
workflow/dags/        ← DAGهای Airflow (فاز Silver+)
workflow/tasks/       ← توابع task
workflow/utils/       ← helper (MinIO, ClickHouse, ...)
src/bronze/           ← Spark Streaming
src/silver/           ← Iceberg ETL
src/gold/             ← ClickHouse load
sql/clickhouse/       ← DDL Gold
docs/                 ← مستندات
```

---

## ۱۱. عیب‌یابی رایج

**Airflow اولین بار خیلی طول می‌کشد**
→ `_PIP_ADDITIONAL_REQUIREMENTS` دارد pip install می‌زند. صبر کن. لاگ: `docker compose logs airflow-scheduler`

**Airflow restart می‌شود**
→ `AIRFLOW_FERNET_KEY` / `JWT_SECRET` خالی؟ postgres healthy?

**Spark به Master وصل نمی‌شود**
→ `docker compose ps spark-master` و `logs spark-worker`

**Iceberg خطا می‌دهد**
→ باکت `warehouse` در Phase 2 ساخته شده؟

---

## ۱۲. جمع‌بندی

این زیرساخت برای **یادگیری و deploy سریع** طراحی شده:
- **یک فایل compose** = کل stack
- **بدون Dockerfile** = کمتر پیچیدگی
- **باکت per phase** = هم‌راستا با PDF
- **مستندات فارسی** = فهم «چرا»، نه فقط «چطور»

کد pipeline (Bronze/Silver/Gold) را خودت در `src/` و `workflow/dags/` می‌نویسی — این repo فقط زیرساخت آماده را فراهم می‌کند.
