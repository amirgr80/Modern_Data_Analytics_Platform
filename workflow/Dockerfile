# ==============================================================================
# workflow/Dockerfile
#
# چرا ایمیج اختصاصی می‌سازیم به‌جای استفاده از _PIP_ADDITIONAL_REQUIREMENTS؟
#   1) _PIP_ADDITIONAL_REQUIREMENTS هر بار که کانتینر بالا میاد، پکیج‌ها رو
#      از اینترنت نصب می‌کنه؛ یعنی هم استارتاپ کند میشه هم اگه اینترنت نباشه
#      کل سرویس بالا نمیاد.
#   2) با build کردن یک ایمیج، پکیج‌ها فقط یک‌بار (موقع build) نصب میشن و
#      نتیجه‌اش یک ایمیج قابل‌تکرار (reproducible) و سریع است - دقیقا همون
#      چیزی که در محیط واقعی (production-like) هم استفاده میشه.
# ==============================================================================

ARG AIRFLOW_VERSION=3.2.1
ARG AIRFLOW_PYTHON_VERSION=3.12

FROM apache/airflow:${AIRFLOW_VERSION}-python${AIRFLOW_PYTHON_VERSION}

# آرگومان‌ها بعد از FROM دوباره باید تعریف بشن تا داخل این استیج در دسترس باشن
ARG AIRFLOW_VERSION
ARG AIRFLOW_PYTHON_VERSION

# فایل requirements رو داخل ایمیج کپی می‌کنیم
COPY requirements.txt /requirements.txt

# نصب پکیج‌ها هنگام build با استفاده از فایل constraints رسمی ایرفلو
# (فایل constraints نسخه دقیق کتابخانه‌هایی که با این ورژن از ایرفلو تست شدن رو
#  قفل می‌کنه تا تداخل نسخه‌ای پیش نیاد)
RUN pip install --no-cache-dir \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${AIRFLOW_PYTHON_VERSION}.txt" \
    -r /requirements.txt