# راهنمای Traefik — دامنه‌های group1

Traefik روی سرور بوت‌کمپ **از قبل نصب است**. کار تو: stack خودت را بالا بیاور و با **label** به Traefik معرفی کن.

## دامنه‌های group1

| سرویس | دامنه | پورت داخل container |
|-------|-------|----------------------|
| Airflow | https://airflow.group1.querabootcamp-de.ir | 8080 |
| MinIO Console | https://storage.group1.querabootcamp-de.ir | 9001 |
| Metabase | https://mb.group1.querabootcamp-de.ir | 3000 |
| Traefik Dashboard | https://traefik.group1.querabootcamp-de.ir | (مدیریت بوت‌کمپ) |

Spark، ClickHouse، Iceberg، Redis و Postgres **پشت Traefik نمی‌روند** — فقط داخل شبکه Docker در دسترس‌اند (امنیت بهتر).

---

## مراحل deploy روی سرور

### ۱. SSH به سرور

```bash
ssh -p 3031 deuser@45.159.113.176
```

> پسورد را در Git یا compose قرار نده. فقط موقع login استفاده کن.

### ۲. نصب Docker (اگر نصب نیست)

```bash
curl -fsSL https://get.docker.com | sh
sudo usermod -aG docker $USER
# logout/login
```

### ۳. پیدا کردن شبکه Traefik

```bash
docker network ls
```

معمولاً یکی از این نام‌هاست: `traefik_traefik_network` (group1)

در [`docker-compose.yml`](../docker-compose.yml) همین نام تنظیم شده:

```yaml
networks:
  traefik:
    external: true
    name: traefik_traefik_network
```

### ۴. Clone و env

```bash
git clone <repo-url> ~/lakehouse
cd ~/lakehouse
cp .env.example .env
# در صورت نیاز پسوردها را عوض کن
```

### ۵. بالا آوردن stack

```bash
docker compose up -d
docker compose ps
```

### ۶. تست دامنه‌ها

چند دقیقه صبر کن (Airflow اولین بار pip install می‌زند)، بعد:

- https://airflow.group1.querabootcamp-de.ir
- https://storage.group1.querabootcamp-de.ir
- https://mb.group1.querabootcamp-de.ir

---

## Traefik چطور کار می‌کند؟

```
Internet → Traefik (روی سرور، SSL دارد)
              ↓ Host header
    airflow.group1...  →  container airflow-apiserver:8080
    storage.group1...  →  container minio:9001 (Console)
    mb.group1...       →  container metabase:3000
```

Traefik containerهایی که **label** دارند و به **شبکه مشترک** وصل‌اند را پیدا می‌کند.

Labelهای مهم (مثال Airflow):

```yaml
traefik.enable=true
traefik.http.routers.airflow.rule=Host(`airflow.group1.querabootcamp-de.ir`)
traefik.http.routers.airflow.entrypoints=websecure
traefik.http.services.airflow.loadbalancer.server.port=8080
```

---

## MinIO — یک دامنه، دو پورت

MinIO دو پورت دارد:
- **9000** — S3 API (Spark/Airflow داخل Docker: `http://minio:9000`)
- **9001** — Console (مرورگر)

روی Traefik فقط **Console (9001)** را public می‌کنیم. API داخل شبکه Docker می‌ماند — برای پایپلاین کافی است.

متغیرهای MinIO برای redirect درست پشت HTTPS:

```yaml
MINIO_BROWSER_REDIRECT_URL: https://storage.group1.querabootcamp-de.ir
MINIO_SERVER_URL: https://storage.group1.querabootcamp-de.ir
```

---

## Airflow — base URL

Airflow 3 باید URL عمومی را بشناسد:

```yaml
AIRFLOW__API__BASE_URL: https://airflow.group1.querabootcamp-de.ir
```

بدون این، UI ممکن است linkها را `localhost` نشان دهد.

---

## Metabase — site URL

```yaml
MB_SITE_URL: https://mb.group1.querabootcamp-de.ir
```

---

## Traefik Dashboard

- URL: https://traefik.group1.querabootcamp-de.ir
- user: `admin`
- password: همان پسورد سرور (طبق پیام بوت‌کمپ)

از dashboard می‌توانی ببینی routerهای تو register شده‌اند یا نه.

---

## عیب‌یابی

**دامنه 404 / Bad Gateway**

```bash
# container healthy است؟
docker compose ps

# به شبکه traefik وصل است؟
docker inspect minio --format '{{json .NetworkSettings.Networks}}'

# Traefik لاگ
docker logs traefik 2>&1 | tail -50
```

**Airflow بالا نمی‌آید**

```bash
docker compose logs -f airflow-scheduler
# اولین بار pip install — ۵–۱۰ دقیقه صبر کن
```

**MinIO Console باز می‌شود ولی login redirect error**

`MINIO_BROWSER_REDIRECT_URL` باید دقیقاً `https://storage.group1.querabootcamp-de.ir` باشد.

**Router در Traefik نیست**

- نام شبکه external در compose با `docker network ls` یکی باشد
- label `traefik.enable=true` روی سرویس باشد
- container restart: `docker compose up -d --force-recreate minio metabase airflow-apiserver`

---

## `2048.group1.querabootcamp-de.ir`

این دامنه احتمالاً برای سرویس جدا یا bonus است. اگر منتور گفت لازم است، router جدا اضافه کن؛ فعلاً در stack پیش‌فرض استفاده نمی‌شود.

---

## امنیت

- پسورد SSH را commit نکن
- `.env` در Git نیست
- فقط Airflow / MinIO Console / Metabase public هستند
- ClickHouse و Spark از اینترنت expose نشوند
