FROM python:3.13-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PLAYWRIGHT_BROWSERS_PATH=/ms-playwright \
    SCRAPER_DEFAULT_HEADLESS=1 \
    SCRAPER_MAX_CONCURRENT_JOBS=1

WORKDIR /app

COPY requirements.txt /app/requirements.txt

RUN pip install --upgrade pip \
    && pip install --no-cache-dir -r /app/requirements.txt \
    && python -m playwright install --with-deps chromium

COPY . /app

RUN mkdir -p /app/runs

EXPOSE 10000

CMD ["python", "main.py"]
