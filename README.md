# 🏗️ Lakehouse Docker Infrastructure — پروژه دوم بوت‌کمپ مهندسی داده

این ریپازیتوری، زیرساخت Docker Compose کامل برای پروژه Lakehouse بوت‌کمپه:

```
Kafka (خارجی، توسط بوت‌کمپ) → Spark → MinIO (Bronze) → Iceberg (Silver) → ClickHouse (Gold) → Metabase
```

همه‌چیز با **Apache Airflow** ارکستریشن میشه. کافی‌ه یک دستور بزنید تا کل پلتفرم بالا بیاد.
Airflow و دیتابیس‌هایش خودکار init میشن؛ **باکت‌های MinIO** رو در هر فاز پروژه خودت می‌سازی (جزئیات: [docs/DEVELOPMENT.md](docs/DEVELOPMENT.md)).

برای deploy روی VPS: [docs/DEPLOYMENT.md](docs/DEPLOYMENT.md)

**Traefik + دامنه‌های group1:** [docs/TRAEFIK.fa.md](docs/TRAEFIK.fa.md)

**مستندات طراحی (فارسی — چرا این‌طور پیاده‌سازی شده):** [docs/DESIGN.fa.md](docs/DESIGN.fa.md)

---

## 📋 پیش‌نیازها

قبل از هر چیز این‌ها رو روی سیستمت نصب کن:

| ابزار                                                      | حداقل نسخه                   | برای چی لازمه             |
| ---------------------------------------------------------- | ---------------------------- | ------------------------- |
| [Docker](https://docs.docker.com/get-docker/)              | 24+                          | اجرای همه کانتینرها       |
| [Docker Compose](https://docs.docker.com/compose/install/) | v2 (پلاگین `docker compose`) | مدیریت همه سرویس‌ها با هم |

**منابع پیشنهادی سیستم:** حداقل ۴ گیگ رم و ۲ هسته CPU خالی برای Docker (این پروژه ۱۴ کانتینر بالا میاره، سبک نیست!).

روی لینوکس، UID خودت رو با این دستور پیدا کن و توی `.env` بذار:

```bash
id -u
```

---

## 📁 ساختار پروژه

```
project/
├── docker-compose.yml      # قلب پروژه - تعریف همه سرویس‌ها
├── .env.example             # نمونه متغیرهای محیطی (کپی کن به .env)
├── README.md                 # همین فایلی که داری می‌خونی
│
├── workflow/                 # همه‌چیز مربوط به Airflow
│   ├── dags/                 # فایل‌های DAG (پایپلاین‌های شما)
│   ├── tasks/                 # توابع/کلاس‌های Task که DAG ها صداشون می‌زنن
│   ├── utils/                 # کد کمکی مشترک (اتصال به MinIO، ClickHouse و ...)
│   └── plugins/                # پلاگین‌های اختصاصی Airflow (اختیاری)
│
├── configs/                   # کانفیگ سرویس‌ها (spark، clickhouse، ...)
│
├── docs/
│   ├── DESIGN.fa.md             # مستندات طراحی فارسی — چرا این‌طور پیاده‌سازی شده
│   ├── DEPLOYMENT.md              # راهنمای deploy روی VPS
│   └── DEVELOPMENT.md             # فازبندی توسعه (Bronze → Gold)
│
├── sql/
│   ├── clickhouse/                # فایل‌های SQL که موقع اولین استارت اجرا میشن
│   ├── iceberg/                     # اسکریپت‌های ساخت جدول Iceberg (اختیاری)
│   └── metabase/                     # کوئری‌های ذخیره‌شده متابیس (اختیاری)
│
├── src/                          # کد پایتون/اسپارک پایپلاین
│   ├── bronze/                     # کانسیومر Spark Structured Streaming
│   ├── silver/                      # پاکسازی + مدل‌سازی Kimball روی Iceberg
│   ├── gold/                         # لود کردن OBT توی ClickHouse
│   └── common/                        # کد مشترک بین لایه‌ها
│
└── data/                          # اگه لازم شد فایل local نمونه اینجا بذار
```

---

## ⚙️ متغیرهای محیطی (.env)

**همه تنظیمات** (پورت‌ها، نسخه Airflow، آدرس Kafka، نام دیتابیس‌ها و ...) داخل [`docker-compose.yml`](docker-compose.yml) نوشته شده.

فایل `.env` **فقط user / password / secret** دارد:

```bash
cp .env.example .env
```

| متغیر | نقش |
|-------|-----|
| `AIRFLOW_FERNET_KEY` / `AIRFLOW_JWT_SECRET` | secretهای Airflow |
| `AIRFLOW_ADMIN_USER` / `AIRFLOW_ADMIN_PASSWORD` | login UI |
| `POSTGRES_USER` / `POSTGRES_PASSWORD` | DB متادیتای Airflow |
| `MINIO_ROOT_USER` / `MINIO_ROOT_PASSWORD` | object storage |
| `CLICKHOUSE_USER` / `CLICKHOUSE_PASSWORD` | DB لایه Gold |
| `METABASE_DB_USER` / `METABASE_DB_PASSWORD` | DB متادیتای Metabase |

مقادیر پیش‌فرض `.env.example` برای تمرین کافی است. برای تغییر پورت یا Kafka، [`docker-compose.yml`](docker-compose.yml) را ویرایش کن.

---

## 🚀 نحوه اجرا (Start)

```bash
docker compose up -d
```

این دستور:

1. ایمیج رسمی Airflow را pull می‌کند و پکیج‌های اضافی را نصب می‌کند (`_PIP_ADDITIONAL_REQUIREMENTS` در compose)
2. Postgres، Redis، MinIO، ClickHouse را بالا می‌آورد و صبر می‌کند تا healthy شوند
3. `airflow-init` دیتابیس Airflow را migrate می‌کند و کاربر admin می‌سازد
4. `iceberg-rest` بعد از آماده شدن MinIO بالا می‌آید
5. `clickhouse` فایل‌های SQL داخل `sql/clickhouse/` را در اولین استارت اجرا می‌کند
6. بقیه سرویس‌ها (Airflow، Spark، Metabase) بالا می‌آیند

> اولین استارت Airflow کندتر است (pip install). استارت‌های بعدی سریع‌ترند.

> باکت‌های MinIO در هر فاز جدا ساخته میشن — [docs/DEVELOPMENT.md](docs/DEVELOPMENT.md)

برای دیدن اینکه همه‌چیز سالمه:

```bash
docker compose ps
```

اگه دنبال چیزی توی لاگ‌ها می‌گردی:

```bash
docker compose logs -f airflow-scheduler
```

## 🛑 نحوه توقف (Stop)

```bash
# فقط کانتینرها رو خاموش می‌کنه، داده‌ها (volume ها) می‌مونن
docker compose down

# همه‌چیز رو پاک می‌کنه، حتی volume ها (یعنی از صفر شروع میشه!)
docker compose down -v
```

---

## 🌐 آدرس سرویس‌ها (Service URLs)

| سرویس                | آدرس                  | یوزرنیم/پسورد پیش‌فرض                                                                      |
| -------------------- | --------------------- | ------------------------------------------------------------------------------------------ |
| Airflow UI           | http://localhost:8080 | مقدار `AIRFLOW_ADMIN_USER` / `AIRFLOW_ADMIN_PASSWORD` در `.env` (پیش‌فرض: `admin`/`admin`) |
| Spark Master UI      | http://localhost:8081 | -                                                                                          |
| Spark Worker UI      | http://localhost:8082 | -                                                                                          |
| MinIO Console        | http://localhost:9001 | مقدار `MINIO_ROOT_USER` / `MINIO_ROOT_PASSWORD` (پیش‌فرض: `minioadmin`/`minioadmin123`)    |
| MinIO API (S3)       | http://localhost:9000 | همون بالا                                                                                  |
| Iceberg REST Catalog | http://localhost:8181 | -                                                                                          |
| ClickHouse HTTP      | http://localhost:8123 | مقدار `CLICKHOUSE_USER` / `CLICKHOUSE_PASSWORD` (پیش‌فرض: `default`/`clickhouse123`)       |
| Metabase             | http://localhost:3000 | بار اول باید از توی UI خودت یک اکانت بسازی                                                 |

> 💡 نکته: داخل شبکه داکر (یعنی از داخل DAG های Airflow یا job های Spark)، سرویس‌ها با اسم کانتینرشون صدا زده میشن، نه `localhost`. مثلا `http://minio:9000` نه `http://localhost:9000`.

---

## 🧠 تصمیم‌های طراحی

توضیحات کامل فارسی برای **هر سرویس، هر تصمیم compose، trade-offها، و فازبندی** در این فایل است:

**[docs/DESIGN.fa.md](docs/DESIGN.fa.md)** — مستندات طراحی با جزئیات

خلاصه:
- Kafka خارجی (بوت‌کمپ) — داخل compose نیست
- Airflow با `_PIP_ADDITIONAL_REQUIREMENTS` در compose — بدون Dockerfile جدا
- CeleryExecutor + Redis — طبق PDF
- MinIO + Iceberg REST + ClickHouse — Medallion architecture
- باکت‌های MinIO در هر فاز جدا ساخته می‌شوند

---

## 🩺 عیب‌یابی (Troubleshooting)

**Airflow اولین بار کند است**

اولین استارت pip install می‌زند (`_PIP_ADDITIONAL_REQUIREMENTS`). صبر کن:

```bash
docker compose logs -f airflow-scheduler
```

**Airflow بالا نمیاد / restart می‌شود**

```bash
docker compose logs airflow-init
```

معمولا یا `AIRFLOW_FERNET_KEY`/`AIRFLOW_JWT_SECRET` خالیه یا هنوز Postgres آماده نشده. صبر کن `postgres` هلث‌چکش سبز بشه.

**پورتی روی سیستم اشغال است**

پورت‌ها در [`docker-compose.yml`](docker-compose.yml) تعریف شده (مثلاً `8080:8080`). مقدار host-side را عوض کن (مثلاً `"8090:8080"`).

**باکت MinIO ندارم**

باکت‌ها upfront ساخته نمیشن. طبق فاز پروژه در MinIO Console (:9001) بساز — [docs/DEVELOPMENT.md](docs/DEVELOPMENT.md)

**Iceberg REST نمیاد بالا**

مطمئن شو MinIO healthy شده:

```bash
docker compose ps minio
docker compose logs iceberg-rest
```

باکت `warehouse` فقط موقع Phase 2 (Silver) لازمه، نه برای استارت سرویس.

**Spark Worker به Master وصل نمیشه**
مطمئن شو `spark-master` قبلش healthy شده (`docker compose ps spark-master`). لاگ worker رو ببین:

```bash
docker compose logs spark-worker
```

**می‌خوام از صفر شروع کنم (پاک کردن کامل)**

```bash
docker compose down -v
docker compose up -d
```

---

## 📝 نکته پایانی

این پروژه یک نقطه‌ی شروع مرتب و best-practice‌ـه، نه محصول نهایی. DAG ها، job های Spark و اسکریپت‌های SQL واقعیِ لایه‌های Bronze/Silver/Gold رو باید خودت داخل `workflow/dags`، `src/` و `sql/` بنویسی.

راهنمای فازبندی: [docs/DEVELOPMENT.md](docs/DEVELOPMENT.md) | deploy: [docs/DEPLOYMENT.md](docs/DEPLOYMENT.md) | طراحی: [docs/DESIGN.fa.md](docs/DESIGN.fa.md)

موفق باشی! 🚀
