"""order-service — checkout API and Saga orchestrator.

Storage : Cloud SQL for PostgreSQL (database `orders`): orders, order_items, saga_log.
Exposes : REST :8080  POST /orders, GET /orders/{id}, GET /orders/{id}/saga, GET /orders?user_id=
Talks to: user-service    (REST)  - validate buyer
          catalog-service (gRPC)  - saga step 1 ReserveStock / ReleaseStock
          payment-service (gRPC)  - saga step 2 Charge / Refund
          Kafka  topic `shop-events`      - order_completed / order_cancelled events (-> Flink)
          PubSub topic `order-completed`  - triggers the invoice Cloud Function (serverless)
"""
import json
import os
import time
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone

import grpc
import httpx
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

import ecommerce_pb2 as pb
import ecommerce_pb2_grpc as pb_grpc
from ecom_common.db import make_pool
from ecom_common.kafka_config import EVENTS_TOPIC, kafka_config
from ecom_common.logging_setup import log_event, setup_logging
from ecom_common.metrics import EVENTS_PUBLISHED, SAGA_EVENTS, install_fastapi_metrics
from saga import SagaOrchestrator, SagaStep, StepFailed

log = setup_logging()

USER_SERVICE_URL = os.getenv("USER_SERVICE_URL", "http://user-service:8080")
CATALOG_ADDR = os.getenv("CATALOG_GRPC_ADDR", "dns:///catalog-service:50051")
PAYMENT_ADDR = os.getenv("PAYMENT_GRPC_ADDR", "dns:///payment-service:50052")
GRPC_TIMEOUT = float(os.getenv("GRPC_TIMEOUT_SECONDS", "5"))
KAFKA_ENABLED = os.getenv("KAFKA_ENABLED", "true") == "true"
PUBSUB_ENABLED = os.getenv("PUBSUB_ENABLED", "true") == "true"
PUBSUB_TOPIC = os.getenv("PUBSUB_ORDER_TOPIC", "order-completed")
GCP_PROJECT = os.getenv("GCP_PROJECT", "")

SCHEMA = """
CREATE TABLE IF NOT EXISTS orders (
    id             TEXT PRIMARY KEY,
    user_id        TEXT NOT NULL,
    status         TEXT NOT NULL,      -- PENDING | COMPLETED | CANCELLED | REJECTED
    total          NUMERIC(12,2) NOT NULL DEFAULT 0,
    failure_reason TEXT,
    payment_id     TEXT,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS orders_user_idx ON orders(user_id);
CREATE TABLE IF NOT EXISTS order_items (
    order_id   TEXT NOT NULL REFERENCES orders(id),
    product_id TEXT NOT NULL,
    quantity   INTEGER NOT NULL,
    PRIMARY KEY (order_id, product_id)
);
CREATE TABLE IF NOT EXISTS saga_log (
    id         BIGSERIAL PRIMARY KEY,
    order_id   TEXT NOT NULL,
    step       TEXT NOT NULL,
    action     TEXT NOT NULL,          -- EXECUTE | COMPENSATE
    status     TEXT NOT NULL,          -- STARTED | SUCCEEDED | FAILED
    detail     TEXT,
    ts         TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS saga_log_order_idx ON saga_log(order_id);
"""

# Client-side round-robin across all pods behind the headless Service.
GRPC_CLIENT_OPTS = [("grpc.lb_policy_name", "round_robin"), ("grpc.enable_retries", 1)]

state: dict = {}


@asynccontextmanager
async def lifespan(_: FastAPI):
    pool = make_pool(max_size=int(os.getenv("DB_POOL_MAX", "10")))
    with pool.connection() as conn:
        conn.execute("SELECT pg_advisory_xact_lock(44)")
        conn.execute(SCHEMA)
    state["pool"] = pool
    state["http"] = httpx.Client(timeout=3.0)
    catalog_ch = grpc.insecure_channel(CATALOG_ADDR, options=GRPC_CLIENT_OPTS)
    payment_ch = grpc.insecure_channel(PAYMENT_ADDR, options=GRPC_CLIENT_OPTS)
    state["catalog"] = pb_grpc.CatalogServiceStub(catalog_ch)
    state["payment"] = pb_grpc.PaymentServiceStub(payment_ch)
    if KAFKA_ENABLED:
        from confluent_kafka import Producer
        state["kafka"] = Producer(kafka_config("order-service", {"linger.ms": 20, "acks": "all"}))
    if PUBSUB_ENABLED:
        from google.cloud import pubsub_v1
        publisher = pubsub_v1.PublisherClient()
        state["pubsub"] = publisher
        state["pubsub_topic"] = publisher.topic_path(GCP_PROJECT, PUBSUB_TOPIC)
    log_event(log, "order-service started", kafka=KAFKA_ENABLED, pubsub=PUBSUB_ENABLED)
    yield
    if "kafka" in state:
        state["kafka"].flush(5)
    catalog_ch.close()
    payment_ch.close()
    state["http"].close()
    pool.close()


app = FastAPI(title="order-service", lifespan=lifespan)
install_fastapi_metrics(app)


class Item(BaseModel):
    product_id: str
    quantity: int = Field(gt=0, le=100)


class OrderIn(BaseModel):
    user_id: str
    items: list[Item] = Field(min_length=1, max_length=20)
    simulate_payment_failure: bool = False  # demo hook for the Saga compensation path


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------
def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def publish_kafka(event: dict) -> None:
    producer = state.get("kafka")
    if not producer:
        return
    try:
        producer.produce(EVENTS_TOPIC, key=event.get("user_id", ""), value=json.dumps(event).encode())
        producer.poll(0)
        EVENTS_PUBLISHED.labels("kafka", EVENTS_TOPIC).inc()
    except Exception as exc:  # never fail a checkout because analytics is down
        log_event(log, "kafka publish failed", level=40, error=str(exc))


def publish_pubsub(message: dict) -> None:
    publisher = state.get("pubsub")
    if not publisher:
        return
    future = publisher.publish(state["pubsub_topic"], json.dumps(message).encode(), order_id=message["order_id"])

    def _done(f):
        try:
            f.result()
            EVENTS_PUBLISHED.labels("pubsub", PUBSUB_TOPIC).inc()
        except Exception as exc:
            log_event(log, "pubsub publish failed", level=40, error=str(exc), order_id=message["order_id"])

    future.add_done_callback(_done)


def validate_user(user_id: str) -> None:
    try:
        r = state["http"].get(f"{USER_SERVICE_URL}/users/{user_id}")
    except httpx.HTTPError as exc:
        raise HTTPException(503, f"user-service unavailable: {exc}") from exc
    if r.status_code == 404:
        raise HTTPException(422, f"unknown user {user_id}")
    if r.status_code >= 400:
        raise HTTPException(502, f"user-service error {r.status_code}")


def fetch_order(order_id: str) -> dict | None:
    with state["pool"].connection() as conn:
        row = conn.execute(
            "SELECT id, user_id, status, total, failure_reason, payment_id, created_at, updated_at "
            "FROM orders WHERE id = %s",
            (order_id,),
        ).fetchone()
        if not row:
            return None
        items = conn.execute(
            "SELECT product_id, quantity FROM order_items WHERE order_id = %s", (order_id,)
        ).fetchall()
    return {
        "id": row[0], "user_id": row[1], "status": row[2], "total": float(row[3]),
        "failure_reason": row[4], "payment_id": row[5],
        "created_at": row[6].isoformat(), "updated_at": row[7].isoformat(),
        "items": [{"product_id": i[0], "quantity": i[1]} for i in items],
    }


def fetch_saga_log(order_id: str) -> list[dict]:
    with state["pool"].connection() as conn:
        rows = conn.execute(
            "SELECT step, action, status, detail, ts FROM saga_log WHERE order_id = %s ORDER BY id",
            (order_id,),
        ).fetchall()
    return [{"step": r[0], "action": r[1], "status": r[2], "detail": r[3], "ts": r[4].isoformat()} for r in rows]


# ---------------------------------------------------------------------------
# Saga definition
# ---------------------------------------------------------------------------
def build_saga(order_id: str) -> SagaOrchestrator:
    pool = state["pool"]
    catalog = state["catalog"]
    payment = state["payment"]

    def record(step, action, status, detail):
        with pool.connection() as conn:
            conn.execute(
                "INSERT INTO saga_log (order_id, step, action, status, detail) VALUES (%s,%s,%s,%s,%s)",
                (order_id, step, action, status, detail),
            )
        log_event(log, f"saga {action.lower()} {step}: {status}", order_id=order_id, saga_step=step,
                  saga_action=action, saga_status=status, detail=detail)

    # Step 1 -------------------------------------------------------------
    def reserve_stock(ctx):
        resp = catalog.ReserveStock(
            pb.ReserveStockRequest(
                order_id=ctx["order_id"],
                items=[pb.OrderItem(product_id=i["product_id"], quantity=i["quantity"]) for i in ctx["items"]],
            ),
            timeout=GRPC_TIMEOUT,
        )
        if not resp.success:
            raise StepFailed(f"stock reservation rejected: {resp.reason}")
        ctx["total"] = resp.total_amount
        with pool.connection() as conn:
            conn.execute("UPDATE orders SET total = %s, updated_at = now() WHERE id = %s",
                         (resp.total_amount, ctx["order_id"]))

    def release_stock(ctx):
        catalog.ReleaseStock(pb.ReleaseStockRequest(order_id=ctx["order_id"]), timeout=GRPC_TIMEOUT)

    # Step 2 -------------------------------------------------------------
    def charge_payment(ctx):
        resp = payment.Charge(
            pb.ChargeRequest(order_id=ctx["order_id"], user_id=ctx["user_id"], amount=ctx["total"],
                             force_failure=ctx["force_payment_failure"]),
            timeout=GRPC_TIMEOUT,
        )
        if not resp.success:
            raise StepFailed(f"payment declined: {resp.reason}")
        ctx["payment_id"] = resp.payment_id

    def refund_payment(ctx):
        payment.Refund(pb.RefundRequest(order_id=ctx["order_id"]), timeout=GRPC_TIMEOUT)

    # Step 3 -------------------------------------------------------------
    def confirm_order(ctx):
        with pool.connection() as conn:
            conn.execute(
                "UPDATE orders SET status = 'COMPLETED', payment_id = %s, updated_at = now() "
                "WHERE id = %s AND status = 'PENDING'",
                (ctx.get("payment_id"), ctx["order_id"]),
            )

    steps = [
        SagaStep("reserve_stock", reserve_stock, release_stock),
        SagaStep("charge_payment", charge_payment, refund_payment),
        SagaStep("confirm_order", confirm_order, None),
    ]
    return SagaOrchestrator(steps, record, lambda s, o: SAGA_EVENTS.labels(s, o).inc())


# ---------------------------------------------------------------------------
# REST API
# ---------------------------------------------------------------------------
@app.get("/healthz")
def healthz():
    return {"status": "ok"}


@app.get("/readyz")
def readyz():
    with state["pool"].connection() as conn:
        conn.execute("SELECT 1")
    return {"status": "ready"}


@app.post("/orders", status_code=201)
def create_order(body: OrderIn):
    started = time.perf_counter()
    validate_user(body.user_id)

    # merge duplicate lines
    merged: dict[str, int] = {}
    for it in body.items:
        merged[it.product_id] = merged.get(it.product_id, 0) + it.quantity
    items = [{"product_id": k, "quantity": v} for k, v in merged.items()]

    order_id = f"o-{uuid.uuid4().hex[:12]}"
    with state["pool"].connection() as conn:
        conn.execute("INSERT INTO orders (id, user_id, status) VALUES (%s, %s, 'PENDING')", (order_id, body.user_id))
        with conn.cursor() as cur:
            cur.executemany("INSERT INTO order_items (order_id, product_id, quantity) VALUES (%s,%s,%s)",
                            [(order_id, i["product_id"], i["quantity"]) for i in items])

    ctx = {"order_id": order_id, "user_id": body.user_id, "items": items, "total": 0.0,
           "force_payment_failure": body.simulate_payment_failure}
    result = build_saga(order_id).execute(ctx)

    if result.succeeded:
        event = {"type": "order_completed", "order_id": order_id, "user_id": body.user_id,
                 "amount": ctx["total"], "ts": int(time.time() * 1000)}
        publish_kafka(event)
        publish_pubsub({"order_id": order_id, "user_id": body.user_id, "total": ctx["total"],
                        "items": items, "payment_id": ctx.get("payment_id"), "completed_at": _now_iso()})
    else:
        status = "REJECTED" if result.failed_step == "reserve_stock" else "CANCELLED"
        with state["pool"].connection() as conn:
            conn.execute("UPDATE orders SET status = %s, failure_reason = %s, updated_at = now() WHERE id = %s",
                         (status, result.reason, order_id))
        publish_kafka({"type": "order_cancelled", "order_id": order_id, "user_id": body.user_id,
                       "amount": 0.0, "ts": int(time.time() * 1000)})

    order = fetch_order(order_id)
    order["saga"] = {
        "succeeded": result.succeeded,
        "failed_step": result.failed_step,
        "compensated_steps": result.compensated_steps,
        "log": fetch_saga_log(order_id),
    }
    log_event(log, "checkout finished", order_id=order_id, status=order["status"],
              duration_ms=round((time.perf_counter() - started) * 1000, 1))
    return order


@app.get("/orders/{order_id}")
def get_order(order_id: str):
    order = fetch_order(order_id)
    if not order:
        raise HTTPException(404, "order not found")
    return order


@app.get("/orders/{order_id}/saga")
def get_saga(order_id: str):
    entries = fetch_saga_log(order_id)
    if not entries:
        raise HTTPException(404, "no saga log for order")
    return {"order_id": order_id, "log": entries}


@app.get("/orders")
def list_orders(user_id: str, limit: int = 20):
    limit = max(1, min(limit, 100))
    with state["pool"].connection() as conn:
        rows = conn.execute(
            "SELECT id FROM orders WHERE user_id = %s ORDER BY created_at DESC LIMIT %s", (user_id, limit)
        ).fetchall()
    return [fetch_order(r[0]) for r in rows]
