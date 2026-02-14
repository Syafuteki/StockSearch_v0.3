FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /workspace

COPY pyproject.toml README.md ./
COPY app ./app
COPY config ./config

RUN pip install --no-cache-dir --upgrade pip setuptools wheel \
    && pip install --no-cache-dir -e . \
    && addgroup --system app \
    && adduser --system --ingroup app app \
    && chown -R app:app /workspace

USER app

CMD ["python", "-m", "jpswing.main"]
