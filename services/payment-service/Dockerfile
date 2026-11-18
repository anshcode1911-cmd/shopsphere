# Build context = repository root:  docker build -f services/payment-service/Dockerfile .
FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 SERVICE_NAME=payment-service
WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY services/payment-service/requirements.txt /tmp/requirements.txt
COPY libs/common /tmp/common
RUN pip install --no-cache-dir -r /tmp/requirements.txt /tmp/common

COPY proto/ /tmp/proto/
RUN python -m grpc_tools.protoc -I /tmp/proto --python_out=/app --grpc_python_out=/app /tmp/proto/ecommerce.proto

COPY services/payment-service/ /app/

RUN useradd --uid 10001 --no-create-home appuser
USER 10001
EXPOSE 50052 9090
CMD ["python", "server.py"]
