# 🏗️ Lakehouse Docker Infrastructure — پروژه دوم بوت‌کمپ مهندسی داده

این ریپازیتوری، زیرساخت Docker Compose کامل برای پروژه Lakehouse بوت‌کمپه:

```
Kafka (خارجی، توسط بوت‌کمپ) → Spark → MinIO (Bronze) → Iceberg (Silver) → ClickHouse (Gold) → Metabase
```

همه‌چیز با **Apache Airflow** ارکستریشن میشه. کافی‌ه یک دستور بزنید تا کل پلتفرم بالا بیاد؛
لازم نیست هیچ چیزی رو دستی init کنید (نه دیتابیس Airflow، نه باکت‌های MinIO، نه اسکیمای ClickHouse).

---

## 📋 پیش‌نیازها

قبل از هر چیز این‌ها رو روی سیستمت نصب کن:

| ابزار                                                      | حداقل نسخه                   | برای چی لازمه             |
| ---------------------------------------------------------- | ---------------------------- | ------------------------- |
| [Docker](https://docs.docker.com/get-docker/)              | 24+                          | اجرای همه کانتینرها       |
| [Docker Compose](https://docs.docker.com/compose/install/) | v2 (پلاگین `docker compose`) | مدیریت همه سرویس‌ها با هم |

**منابع پیشنهادی سیستم:** حداقل ۴ گیگ رم و ۲ هسته CPU خالی برای Docker (این پروژه ۱۵ کانتینر بالا میاره، سبک نیست!).

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
│   ├── plugins/                # پلاگین‌های اختصاصی Airflow (اختیاری)
│   ├── logs/                    # لاگ‌های اجرای Airflow (volume، خودش پر میشه)
│   ├── requirements.txt        # پکیج‌های پایتونی موردنیاز DAG ها
│   └── Dockerfile               # ایمیج اختصاصی Airflow با پکیج‌های نصب‌شده
│
├── configs/                   # فایل‌های کانفیگ هر سرویس
│   ├── airflow/                 # کانفیگ اضافه ایرفلو (اختیاری)
│   ├── spark/                    # spark-defaults.conf و مشابه
│   ├── clickhouse/               # کانفیگ سرور کلیک‌هاوس (اختیاری)
│   ├── minio/                     # اسکریپت ساخت باکت‌ها
│   └── metabase/                  # کانفیگ متابیس (اختیاری)
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

فایل `.env.example` همه چیزهای قابل‌تنظیم پروژه رو داره. اول این کارو بکن:

```bash
cp .env.example .env
```

بعد فایل `.env` رو باز کن و این‌ها رو حتما عوض کن:

- `AIRFLOW_FERNET_KEY` → با این دستور یکی بساز:
  ```bash
  python3 -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
  ```
- `AIRFLOW_JWT_SECRET` → یک رشته تصادفی، مثلا:
  ```bash
  openssl rand -hex 32
  ```
- `AIRFLOW_UID` → روی لینوکس مقدار `id -u` خودت
- `KAFKA_BOOTSTRAP_SERVERS` → آدرسی که منتورهای بوت‌کمپ بهت میدن
- پسوردهای MinIO / ClickHouse / Postgres / Metabase → برای تمرین همون پیش‌فرض هم کار می‌کنه، ولی بهتره عوضشون کنی

بقیه متغیرها (پورت‌ها، اسم دیتابیس‌ها و ...) پیش‌فرض‌های منطقی دارن و لازم نیست دستکاریشون کنی مگه اینکه پورتی روی سیستمت اشغال باشه.

---

## 🚀 نحوه اجرا (Start)

```bash
docker compose up -d --build
```

این دستور:

1. ایمیج اختصاصی Airflow رو build می‌کنه (`workflow/Dockerfile`)
2. Postgres، Redis، MinIO، ClickHouse رو بالا میاره و صبر می‌کنه Healthy بشن
3. `airflow-init` دیتابیس Airflow رو می‌سازه و یوزر ادمین می‌سازه
4. `minio-init` باکت‌های `bronze` / `silver` / `warehouse` / `checkpoints` رو می‌سازه
5. `iceberg-rest` رو بعد از آماده شدن MinIO بالا میاره
6. `clickhouse` هر فایل SQL داخل `sql/clickhouse/` رو خودکار اجرا می‌کنه
7. بقیه سرویس‌های Airflow (apiserver/scheduler/worker/triggerer)، Spark و Metabase بالا میان

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

## 🧠 تصمیم‌های طراحی مهم و چرایی‌شون

### چرا Kafka رو خودمون نساختیم؟

طبق صورت‌مسئله، خوشه Kafka توسط بوت‌کمپ فراهم شده. ساختن یک Kafka اضافه هم غیرضروریه هم می‌تونه باعث تداخل بشه. به‌جاش فقط از طریق متغیر `KAFKA_BOOTSTRAP_SERVERS` به Kafka خارجی وصل میشیم؛ کد Spark Streaming شما این آدرس رو از env می‌خونه.

### چرا برای Airflow ایمیج اختصاصی (Dockerfile) ساختیم، نه `_PIP_ADDITIONAL_REQUIREMENTS`؟

متغیر `_PIP_ADDITIONAL_REQUIREMENTS` هر بار که کانتینر بالا میاد پکیج‌ها رو از اینترنت نصب می‌کنه. این هم استارتاپ رو خیلی کند می‌کنه هم اگه یک بار پکیج نصب نشه (مثلا قطعی اینترنت)، کل سرویس Airflow خراب میشه. با ساختن ایمیج اختصاصی (`workflow/Dockerfile` + `workflow/requirements.txt`) پکیج‌ها فقط یک بار موقع build نصب میشن و نتیجه یک ایمیج پایدار و سریع میشه — دقیقا شبیه چیزی که در محیط واقعی هم استفاده می‌کنن.

### چرا CeleryExecutor به‌جای LocalExecutor؟

صورت‌مسئله صراحتا CeleryExecutor خواسته. تفاوت اصلی: با LocalExecutor همه Task ها روی همون یک کانتینر Scheduler اجرا میشن (مقیاس‌پذیر نیست). با CeleryExecutor یک یا چند کانتینر Worker جدا داریم که Task ها رو از صف Redis برمی‌دارن و اجرا می‌کنن — یعنی اگه لازم شد، می‌تونی چند تا Worker موازی اجرا کنی (`docker compose up -d --scale airflow-worker=3`).

### چرا Redis؟

Redis نقش "بروکر" پیام رو برای CeleryExecutor بازی می‌کنه: Scheduler یک Task رو توی صف Redis می‌ذاره، یکی از Worker های آزاد اونو برمی‌داره و اجرا می‌کنه. سبک و سریعه، برای همین برای این کار انتخاب پیش‌فرض Celery/Airflow‌ه.

### چرا Postgres جدا برای Airflow و جدا برای Metabase؟

هرکدوم اسکیمای دیتابیسی کاملا متفاوت و مستقل دارن (متادیتای DAG ها vs. متادیتای داشبوردهای Metabase). جدا نگه‌داشتنشون یعنی اگه یکی خراب شد یا نیاز به ریست داشت، اون یکی دست‌نخورده می‌مونه. این جدا از دیتابیس‌های خود پایپلاین (که قراره تو ClickHouse باشن) هست.

### چرا MinIO؟

MinIO یک storage سازگار با S3 API هست که می‌تونی روی سیستم خودت (بدون نیاز به AWS واقعی) اجرا کنی. لایه Bronze (فایل‌های Parquet خام) و همچنین warehouse جدول‌های Iceberg (لایه Silver) هر دو روی MinIO ذخیره میشن.

### چرا سرویس `minio-init` جدا از `minio`؟

کانتینر `minio` فقط سرور Object Storage رو اجرا می‌کنه؛ خودش باکت نمی‌سازه. `minio-init` یک کانتینر یک‌بارمصرف (اجرا میشه، کارشو می‌کنه، exit می‌کنه) هست که با ابزار `mc` (MinIO Client) باکت‌های `bronze`/`silver`/`warehouse`/`checkpoints` رو خودکار می‌سازه. این یعنی هیچ‌وقت لازم نیست دستی وارد کنسول MinIO بشی و باکت بسازی.

### چرا Iceberg REST Catalog؟

Apache Iceberg برای اینکه بدونه هر جدول کجاست و اسکیماش چیه، به یک "کاتالوگ" نیاز داره. از بین گزینه‌های مختلف (Hive Metastore، JDBC Catalog، REST Catalog)، REST Catalog ساده‌ترین و سبک‌ترین گزینه برای یک محیط آموزشیه — نه نیاز به Hadoop داره نه به یک دیتابیس اضافه، فقط یک سرویس HTTP کوچیکه که هم Spark و هم (در آینده) موتورهای دیگه می‌تونن باهاش صحبت کنن.

### چرا ایمیج رسمی Apache Spark به‌جای Bitnami؟

صورت‌مسئله صراحتا "Apache Spark Official Image" خواسته. ایمیج رسمی `apache/spark` سبک‌تره و مستقیما از پروژه Apache میاد (نه یک شرکت ثالث). برای اجرا به‌عنوان master/worker مستقیم کلاس‌های Java اسپارک (`org.apache.spark.deploy.master.Master` / `...worker.Worker`) رو صدا می‌زنیم.

### چرا ClickHouse برای لایه Gold؟

ClickHouse یک دیتابیس OLAP ستون‌گراست که برای کوئری‌های تحلیلی سنگین (aggregation روی میلیون‌ها ردیف) فوق‌العاده سریعه — خیلی سریع‌تر از Postgres معمولی برای این نوع کار. الگوی One Big Table (OBT) که قراره اینجا بسازی دقیقا برای این طراحی شده که Metabase بدون JOIN سنگین بتونه سریع جواب بگیره.

### چرا فایل‌های SQL توی `sql/clickhouse/` خودکار اجرا میشن؟

ایمیج رسمی ClickHouse یک قابلیت built-in داره: هر فایل `.sql` (یا `.sh`) داخل `/docker-entrypoint-initdb.d` رو در اولین بار بالا اومدن کانتینر (وقتی دیتای دیتابیس خالیه) خودکار اجرا می‌کنه. ما همون پوشه `sql/clickhouse` رو به اونجا mount کردیم؛ یعنی هر جدول یا دیتابیسی که آنجا تعریف کنی، خودکار ساخته میشه.

### چرا از YAML Anchor (`x-airflow-common`) استفاده کردیم؟

پنج سرویس Airflow (init/apiserver/scheduler/worker/triggerer) تقریبا همه‌ی env، volume و تنظیمات شبکه‌شون یکیه. به‌جای کپی‌پیست همون چند ده خط تنظیمات توی هر سرویس (که هم فایل رو طولانی می‌کنه هم خطر عدم‌همخوانی رو بالا می‌بره)، یک بار توی `x-airflow-common` تعریفشون کردیم و با `<<: *airflow-common` هر سرویس اونا رو "کپی" می‌کنه. اگه بخوای یک env variable جدید اضافه کنی، فقط یک جا اضافه می‌کنی.

### چرا `depends_on` با `condition: service_healthy` / `service_completed_successfully`؟

`depends_on` ساده فقط ترتیب _استارت_ کانتینرها رو تضمین می‌کنه، نه اینکه سرویس داخلش واقعا آماده‌ست یا نه (مثلا Postgres ممکنه کانتینرش بالا اومده باشه ولی هنوز داره فایل‌های دیتابیس رو آماده می‌کنه). با `condition: service_healthy` صبر می‌کنیم تا healthcheck سرویس سبز بشه، و با `service_completed_successfully` (برای `airflow-init` و `minio-init`) صبر می‌کنیم اون کار یک‌بارمصرف واقعا با موفقیت _تموم_ بشه. این دقیقا همون چیزیه که باعث میشه `docker compose up -d` بدون هیچ init دستی کار کنه.

### چرا Named Volume به‌جای bind mount برای دیتابیس‌ها؟

Named volume (مثل `postgres_data`, `minio_data`, `clickhouse_data`) توسط خود Docker مدیریت میشه، مستقل از مسیر دقیق فایل‌سیستم هاست کار می‌کنه و بین ری‌استارت‌ها (`docker compose down` بدون `-v`) داده‌ها حفظ میشن. برای کدی که خودت می‌خوای ادیت کنی (مثل `workflow/dags`) به‌جاش از bind mount استفاده کردیم چون می‌خوایم تغییرات فایل رو فورا توی کانتینر ببینیم.

### چرا یک شبکه (`datalake`) به‌جای شبکه پیش‌فرض داکر؟

یک شبکه bridge اختصاصی یعنی DNS داخلی داکر بین سرویس‌ها خودکار کار می‌کنه (هر سرویس فقط با اسمش، مثلا `clickhouse`، در دسترس بقیه‌ست) و از تداخل با کانتینرهای دیگه‌ای که شاید روی سیستمت داری، جلوگیری میشه.

---

## 🩺 عیب‌یابی (Troubleshooting)

**سرویس Airflow بالا نمیاد / همش restart میشه**

```bash
docker compose logs airflow-init
```

معمولا یا `AIRFLOW_FERNET_KEY`/`AIRFLOW_JWT_SECRET` خالیه یا هنوز Postgres آماده نشده. صبر کن `postgres` هلث‌چکش سبز بشه.

**پورتی already in use میگه**
یکی از پورت‌های پیش‌فرض (مثلا 8080 یا 5432) روی سیستمت اشغاله. مقدار پورت مربوطه رو توی `.env` عوض کن (مثلا `AIRFLOW_APISERVER_PORT=8090`).

**باکت‌های MinIO ساخته نشدن**

```bash
docker compose logs minio-init
```

اگه `minio` هنوز healthy نشده باشه، `minio-init` صبر می‌کنه. اگه بازم مشکل بود، دستی اجراش کن:

```bash
docker compose up minio-init
```

**Iceberg REST نمیاد بالا**
بستگی به این داره که `minio-init` واقعا با موفقیت تموم شده باشه (باکت `warehouse` ساخته شده باشه). چک کن:

```bash
docker compose ps minio-init
```

**Spark Worker به Master وصل نمیشه**
مطمئن شو `spark-master` قبلش healthy شده (`docker compose ps spark-master`). لاگ worker رو ببین:

```bash
docker compose logs spark-worker
```

**می‌خوام از صفر شروع کنم (پاک کردن کامل)**

```bash
docker compose down -v
docker compose up -d --build
```

---

## 📝 نکته پایانی

این پروژه یک نقطه‌ی شروع مرتب و best-practice‌ـه، نه محصول نهایی. DAG ها، job های Spark و اسکریپت‌های SQL واقعیِ لایه‌های Bronze/Silver/Gold رو باید خودت داخل `workflow/dags`، `src/` و `sql/` بنویسی — طبق چیزی که در فازهای پروژه (Bronze → Silver → Gold → Dashboard) خواسته شده.

موفق باشی! 🚀
