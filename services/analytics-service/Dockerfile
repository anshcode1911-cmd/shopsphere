# Build context = repository root:  docker build -f services/analytics-service/Dockerfile .
FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 SERVICE_NAME=analytics-service
WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY services/analytics-service/requirements.txt /tmp/requirements.txt
COPY libs/common /tmp/common
RUN pip install --no-cache-dir -r /tmp/requirements.txt /tmp/common

# (no gRPC stubs needed)

COPY services/analytics-service/ /app/

RUN useradd --uid 10001 --no-create-home appuser
USER 10001
EXPOSE 8080
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8080", "--workers", "1", "--no-access-log"]
