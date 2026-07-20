# لایهٔ Gold — طراحی، پیاده‌سازی، و تصمیم OBT

این سند سه چیز را پوشش می‌دهد:

1. **چرا** این معماری (OBT روی ClickHouse) برای لایهٔ Gold انتخاب شده.
2. **تحلیل تصمیم اصلی**: آیا باید دادهٔ transactional و behavioral را در یک OBT ادغام کرد؟
3. **توضیح هر فایل** پیاده‌سازی‌شدهٔ Gold و نقشش.

---

## بخش ۱ — چرا این معماری برای لایهٔ Gold؟

### قرارداد لایهٔ Gold

Gold یک کار دارد: **سرعت کوئری برای BI**. برخلاف Silver که باید درست و نرمال باشد،
Gold باید طوری شکل بگیرد که Metabase بدون join سنگین، aggregate سریع بزند.

### چرا One Big Table (OBT) و نه star schema در Gold؟

Silver از قبل یک star schema تمیز دارد (`fact_behavioral_events` + سه dimension). چرا در Gold دوباره denormalize می‌کنیم؟

| دلیل | توضیح |
|------|-------|
| **حذف join در زمان کوئری** | dimensionها یک‌بار در زمان بارگذاری (Spark) join می‌شوند. هر کوئری داشبورد روی یک جدول تخت اجرا می‌شود، نه join چندجدولی. |
| **ClickHouse برای اسکن ستونی ساخته شده** | join در ClickHouse (به‌ویژه توزیع‌شده) گران است؛ اسکن ستونی روی یک جدول پهن ارزان است. OBT دقیقاً با نقاط قوت موتور هم‌راستاست. |
| **سادگی برای تحلیل‌گر** | کاربر Metabase یک جدول می‌بیند با همهٔ صفتها؛ لازم نیست مدل ابعادی را بفهمد. |
| **قابل بازتولید از Silver** | OBT هیچ‌وقت مبدأ حقیقت نیست؛ هر زمان از Silver قابل ساخت دوباره است. |

معاملهٔ OBT: تکرار داده (device_name، event_category و... در هر ردیف تکرار می‌شوند) و
پهن‌شدن جدول. برای یک لایهٔ سرو تحلیلی که از منبع دیگری بازتولید می‌شود، این تکرار **پذیرفتنی و عمدی** است.

### چرا ClickHouse؟

- موتور ستونی خالص، بهینه برای aggregate روی میلیون‌ها ردیف رویداد.
- `ReplacingMergeTree` امکان idempotency می‌دهد (پایین‌تر).
- پارتیشن‌بندی زمانی بومی، سازگار با ماهیت رویدادهای رفتاری.

### چرا `ReplacingMergeTree` و partition-replace؟

جدول Gold با موتور `ReplacingMergeTree(gold_loaded_at)` ساخته شده و `ORDER BY` آن با
`event_key` تمام می‌شود. الگوی بارگذاری (در `behavioral_gold_clickhouse.py`):

1. اول ردیف‌های همان `processing_date` را `ALTER TABLE ... DELETE` می‌کند (`mutations_sync=2`).
2. بعد ردیف‌های تازه را insert می‌کند.

این یعنی **اجرای دوبارهٔ یک تاریخ، ردیف تکراری نمی‌سازد** — یک partition-replace idempotent.
`ReplacingMergeTree` هم به‌عنوان تور ایمنی، اگر ردیفی با کلید یکسان بماند، نسخهٔ با
`gold_loaded_at` جدیدتر را نگه می‌دارد.

### چرا این grain (یک ردیف به‌ازای هر event)؟

OBT رفتاری دقیقاً grain جدول fact را حفظ می‌کند: **یک ردیف به‌ازای هر رویداد** (`event_key`).
هیچ pre-aggregation در خود OBT انجام نمی‌شود. دلیل: داشبوردهای مختلف به بُرش‌های مختلف
نیاز دارند (بر اساس session، device، utm_source، قیف تبدیل). نگه‌داشتن grain ریز یعنی
هر aggregate در زمان کوئری ممکن است، بدون اینکه در بارگذاری به یک تجمیع خاص قفل شویم.

---

## بخش ۲ — تصمیم اصلی: آیا transactional و behavioral را در یک OBT ادغام کنیم؟

### پاسخ کوتاه

**نه — نباید در یک OBT فیزیکی ادغام شوند. باید دو OBT جدا بمانند و در صورت نیاز در لایهٔ
کوئری/داشبورد به هم join شوند (یا حداکثر یک مدل مشترک صریح ساخته شود).**

این دقیقاً همان تصمیمی است که پروژه در عمل گرفته و در `plans/behavioral_gold_design.md` هم آمده:
> «Behavioral Gold should remain independent from Transactional Gold until a shared model is explicitly introduced.»

### چرا نه؟ — دلایل فنی

#### ۱. Grain ناسازگار است (دلیل اصلی)

این تعیین‌کننده‌ترین دلیل است. دو دامنه grainهای متفاوتی دارند:

| دامنه | grain | یعنی |
|-------|-------|------|
| Behavioral OBT | یک **رویداد** (`event_key`) | میلیون‌ها ردیف ریز |
| Transactional `fact_order` | یک **سفارش** (`order_id`) | |
| Transactional `fact_order_item` | یک **قلم سفارش** | |

ادغام دو fact با grain متفاوت در یک جدول، **fan-out** می‌سازد: هر رویداد رفتاری با هر سفارش
مرتبط تکرار می‌شود و **معیارها دوبار شمرده می‌شوند** (double counting). مثلاً `sum(cart_value)`
یا `sum(order_total)` بی‌معنی می‌شود چون یک مقدار در چند ردیف تکرار شده. این کلاسیک‌ترین
خطای مدل‌سازی ابعادی است: **هرگز دو fact با grain متفاوت را در یک جدول join نکن.**

#### ۲. رابطه یک‌به‌چند و پراکندگی (sparsity)

اکثر رویدادهای رفتاری اصلاً `order_id` ندارند (بازدید صفحه، جستجو، افزودن به سبد).
اگر transactional را داخل behavioral OBT بریزیم، ستون‌های سفارش برای اکثر ردیف‌ها NULL می‌شوند —
یک جدول پهنِ عمدتاً خالی. برعکس هم صادق است: یک سفارش ممکن است به ده‌ها رویداد رفتاری وصل باشد.

#### ۳. چرخهٔ به‌روزرسانی و مالکیت متفاوت

- Behavioral: جریان رویداد پرحجم، بارگذاری روزانه بر اساس `processing_date`.
- Transactional: وضعیت سفارش/کاربر/محصول، به‌روزرسانی‌های کندتر، SCD برای قیمت (`dim_product_price_scd`).

قفل‌کردن این دو در یک جدول یعنی چرخهٔ بارگذاری، پارتیشن‌بندی و idempotency هر دو به هم گره می‌خورند.

#### ۴. ایزولاسیون تیم و لایه‌ها

Silver عمداً دو دامنه را جدا نگه داشته (تیم‌های موازی، بدون merge conflict). ادغام اجباری
در Gold این مرز را می‌شکند و یکی از مزیت‌های اصلی معماری Medallion (محصورسازی خطا) را از بین می‌برد.

### پس اتصال دو دامنه چطور انجام شود؟

نقاط اتصال از قبل موجودند و **کافی‌اند**:

- `fact_behavioral_events` ستون‌های `user_id`, `product_id`, `order_id` را دارد (از payload رویداد).
- Transactional کلیدهای `user_id`, `product_id`, `order_id` را دارد.

راه درست، بسته به نیاز:

1. **Join در زمان کوئری / سطح Metabase** (توصیه‌شده برای الان): دو OBT جدا، و در داشبورد
   روی `user_id` یا `order_id` به هم join شوند. انعطاف بالا، بدون double counting.
2. **مدل مشترک صریح و purpose-built** (اگر یک نیاز تحلیلی مشخص شد): مثلاً یک جدول
   `user_journey` با grain «یک session» یا «یک کاربر در روز» که هم معیارهای رفتاری تجمیع‌شده
   و هم معیارهای سفارش تجمیع‌شده را در **همان grain** کنار هم بگذارد. اینجا چون grain یکی است،
   double counting رخ نمی‌دهد.

### جمع‌بندی تصمیم

| گزینه | grain | double counting؟ | توصیه |
|-------|-------|------------------|-------|
| ادغام خام در یک OBT | ناسازگار | ✅ بله (خطرناک) | ⛔ نه |
| دو OBT جدا + join در کوئری | حفظ می‌شود | ❌ نه | ✅ الان |
| مدل مشترک با grain یکسان | یکسان و عمدی | ❌ نه | ✅ وقتی نیاز مشخص شد |

**خلاصه:** ادغام فیزیکی transactional و behavioral در یک OBT، به‌خاطر ناسازگاری grain و
fan-out، از نظر مدل‌سازی ابعادی نادرست است. آن‌ها را جدا نگه دار و در سطح کوئری یا با یک
مدل مشترکِ هم‌grain کنار هم بیاور.

---

## بخش ۳ — توضیح هر فایل پیاده‌سازی‌شدهٔ Gold

### `src/gold/behavioral_gold_config.py`
پیکربندی. کلاس `GoldBehavioralConfig` (frozen dataclass) که از env خوانده می‌شود:
اتصال ClickHouse (host, port, db, user, password, table)، اندازهٔ batch درج، و مسیر
جداول Silver (`fact_table`, `device_table`, `event_type_table`, `session_table`).
همچنین `GOLD_BEHAVIORAL_COLUMNS` — قرارداد ستون‌های OBT به‌ترتیب دقیق (منبع حقیقتِ schema).

### `src/gold/behavioral_gold_transform.py`
منطق تبدیل. `build_behavioral_gold_obt` جدول `fact_behavioral_events` را برای یک
`processing_date` فیلتر می‌کند، سپس سه dimension (device, event_type, session) را با
`left join` می‌چسباند و یک ردیف تخت به‌ازای هر رویداد می‌سازد. اینجاست که star schema
به OBT تبدیل می‌شود. `_require_columns` وجود ستون‌های حیاتی fact را قبل از کار چک می‌کند.
ستون‌های پیچیده (`cart_items`, `dq_flags`) با `to_json` به رشته تبدیل می‌شوند.

### `src/gold/behavioral_gold_clickhouse.py`
نویسندهٔ ClickHouse. شامل:
- `CREATE_BEHAVIORAL_GOLD_TABLE_SQL` — DDL جدول (`ReplacingMergeTree`, پارتیشن ماهانه، `ORDER BY`).
- `ensure_behavioral_gold_table` — ساخت database و جدول اگر نبود.
- `replace_behavioral_gold_partition` — قلب idempotency: اول `DELETE` پارتیشن همان تاریخ،
  بعد insert بچی. در پایان `inserted == source_count` را چک می‌کند و اگر نخواند خطا می‌دهد
  (تضمین کامل بودن بارگذاری).

### `src/jobs/gold_behavioral_job.py`
نقطهٔ ورود batch (CLI). `run_gold_behavioral_job` یک SparkSession می‌سازد،
`build_behavioral_gold_obt` را صدا می‌زند و cache می‌کند، سپس `_assert_gold_contract` را
اجرا می‌کند: تطابق دقیق ستون‌ها، null نبودن `event_key` و `event_timestamp`، و نبود
`event_key` تکراری. بعد `replace_behavioral_gold_partition` را صدا می‌زند. آرگومان `--execution-date` می‌گیرد.

### `sql/clickhouse/001_behavioral_gold_obt.sql`
DDL مستقل و نسخه‌دار جدول `lakehouse.behavioral_obt` — همان تعریفی که کد Python هم می‌سازد.
داشتنش جدا یعنی می‌توان جدول را دستی/در migration ساخت بدون اجرای job.

### `sql/clickhouse/behavioral_gold_verification.sql`
کوئری‌های تأیید بعد از بارگذاری: شمارش کل و یکتایی `event_key` (کشف duplicate)،
تازگی داده (`max` تایم‌استمپ‌ها)، توزیع رویداد بر اساس category/type، تفکیک device، و قیف
تبدیل بر اساس utm_source. این‌ها هم verification‌اند هم نمونهٔ آماده برای داشبورد Metabase.

### `workflow/dags/gold_behavioral_dag.py`
DAG ارکستراسیون (`gold_behavioral_clickhouse_etl`, زمان‌بندی `0 3 * * *`). سه تسک زنجیره‌ای:
1. `check_silver_behavioral_succeeded` — چک می‌کند Silver برای آن تاریخ وضعیت `SUCCEEDED` دارد.
2. `check_clickhouse_ready` — یک `SELECT 1` به ClickHouse می‌زند.
3. `run_gold_behavioral_job` — با `spark-submit` مرحلهٔ Gold را اجرا می‌کند.

الگوی «اول upstream را چک کن، بعد اجرا کن» — تضمین می‌کند Gold روی Silver ناقص اجرا نشود.

---

## بخش ۴ — چند نکتهٔ عملیاتی

- Gold هرگز Bronze یا Silver را تغییر نمی‌دهد؛ فقط می‌خواند.
- Gold باید هر زمان از Silver بازتولید شدنی باشد.
- بارگذاری دوبارهٔ یک تاریخ نباید ردیف تحلیلی تکراری بسازد (partition-replace + ReplacingMergeTree).
- تنها دامنهٔ پیاده‌سازی‌شدهٔ Gold فعلاً Behavioral است؛ Transactional Gold عمداً به تعویق افتاده (بخش ۲).
