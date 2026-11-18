"""gateway — public web front-end + API gateway (Backend-for-Frontend).

Public  : exposed through a GCP external L4 load balancer (Service type LoadBalancer).
Serves  : the single-page web UI (/), and /api/* which fans out to the internal services:
          catalog-service  over gRPC  (product listing / details)
          user-service     over REST
          order-service    over REST  (checkout -> Saga)
          analytics-service over REST (live Flink results)
Emits   : click-stream events (product_view, add_to_cart, checkout_started ...) to the
          Kafka topic `shop-events`, which the Flink job aggregates per 1-minute window.
"""
import json
import os
import time
from contextlib import asynccontextmanager
from pathlib import Path

import grpc
import httpx
from fastapi import FastAPI, Header, HTTPException, Request
from fastapi.responses import FileResponse, JSONResponse
from google.protobuf.json_format import MessageToDict
from pydantic import BaseModel

import ecommerce_pb2 as pb
import ecommerce_pb2_grpc as pb_grpc
from ecom_common.kafka_config import EVENTS_TOPIC, kafka_config
from ecom_common.logging_setup import log_event, setup_logging
from ecom_common.metrics import EVENTS_PUBLISHED, install_fastapi_metrics

log = setup_logging()

USER_URL = os.getenv("USER_SERVICE_URL", "http://user-service:8080")
ORDER_URL = os.getenv("ORDER_SERVICE_URL", "http://order-service:8080")
ANALYTICS_URL = os.getenv("ANALYTICS_SERVICE_URL", "http://analytics-service:8080")
CATALOG_ADDR = os.getenv("CATALOG_GRPC_ADDR", "dns:///catalog-service:50051")
KAFKA_ENABLED = os.getenv("KAFKA_ENABLED", "true") == "true"
STATIC = Path(__file__).parent / "static"

# Client-side round-robin across all pods behind the headless Service.
GRPC_CLIENT_OPTS = [("grpc.lb_policy_name", "round_robin"), ("grpc.enable_retries", 1)]

state: dict = {}


@asynccontextmanager
async def lifespan(_: FastAPI):
    state["http"] = httpx.Client(timeout=15.0)
    channel = grpc.insecure_channel(CATALOG_ADDR, options=GRPC_CLIENT_OPTS)
    state["catalog"] = pb_grpc.CatalogServiceStub(channel)
    if KAFKA_ENABLED:
        from confluent_kafka import Producer
        state["kafka"] = Producer(kafka_config("gateway", {"linger.ms": 50, "acks": "1"}))
    log_event(log, "gateway started", kafka=KAFKA_ENABLED)
    yield
    if "kafka" in state:
        state["kafka"].flush(5)
    channel.close()
    state["http"].close()


app = FastAPI(title="ShopSphere gateway", lifespan=lifespan)
install_fastapi_metrics(app)


def emit(event_type: str, user_id: str | None, **fields) -> None:
    producer = state.get("kafka")
    if not producer:
        return
    event = {"type": event_type, "user_id": user_id or "anonymous", "ts": int(time.time() * 1000), **fields}
    try:
        producer.produce(EVENTS_TOPIC, key=event["user_id"], value=json.dumps(event).encode())
        producer.poll(0)
        EVENTS_PUBLISHED.labels("kafka", EVENTS_TOPIC).inc()
    except Exception as exc:
        log_event(log, "kafka publish failed", level=40, error=str(exc))


def forward(method: str, url: str, **kwargs):
    try:
        r = state["http"].request(method, url, **kwargs)
    except httpx.HTTPError as exc:
        raise HTTPException(503, f"upstream unavailable: {exc.__class__.__name__}") from exc
    try:
        body = r.json()
    except ValueError:
        body = {"detail": r.text}
    return JSONResponse(status_code=r.status_code, content=body)


def grpc_call(fn, request):
    try:
        return fn(request, timeout=5)
    except grpc.RpcError as exc:
        code = exc.code()
        status = 404 if code == grpc.StatusCode.NOT_FOUND else 503
        raise HTTPException(status, f"catalog-service: {code.name}: {exc.details()}") from exc


# ---------------------------------------------------------------------------
@app.get("/", include_in_schema=False)
def index():
    return FileResponse(STATIC / "index.html")


@app.get("/healthz")
def healthz():
    return {"status": "ok"}


@app.get("/api/products")
def list_products(category: str = "", x_user_id: str | None = Header(default=None)):
    resp = grpc_call(state["catalog"].ListProducts, pb.ListProductsRequest(category=category))
    emit("catalog_view", x_user_id, category=category or "all")
    return [MessageToDict(p, preserving_proto_field_name=True, always_print_fields_with_no_presence=True)
            for p in resp.products]


@app.get("/api/products/{product_id}")
def get_product(product_id: str, x_user_id: str | None = Header(default=None)):
    p = grpc_call(state["catalog"].GetProduct, pb.GetProductRequest(id=product_id))
    emit("product_view", x_user_id, product_id=product_id, amount=p.price)
    return MessageToDict(p, preserving_proto_field_name=True, always_print_fields_with_no_presence=True)


class CartAdd(BaseModel):
    product_id: str
    quantity: int = 1


@app.post("/api/cart")
def add_to_cart(body: CartAdd, x_user_id: str | None = Header(default=None)):
    # The cart lives client-side; the gateway only records the intent for analytics.
    emit("add_to_cart", x_user_id, product_id=body.product_id, quantity=body.quantity)
    return {"status": "recorded"}


@app.post("/api/users")
async def create_user(request: Request):
    return forward("POST", f"{USER_URL}/users", json=await request.json())


@app.get("/api/users/{user_id}")
def get_user(user_id: str):
    return forward("GET", f"{USER_URL}/users/{user_id}")


@app.post("/api/orders")
async def create_order(request: Request):
    body = await request.json()
    emit("checkout_started", body.get("user_id"), items=len(body.get("items", [])))
    return forward("POST", f"{ORDER_URL}/orders", json=body)


@app.get("/api/orders")
def list_orders(user_id: str):
    return forward("GET", f"{ORDER_URL}/orders", params={"user_id": user_id})


@app.get("/api/orders/{order_id}")
def get_order(order_id: str):
    return forward("GET", f"{ORDER_URL}/orders/{order_id}")


@app.get("/api/orders/{order_id}/saga")
def get_saga(order_id: str):
    return forward("GET", f"{ORDER_URL}/orders/{order_id}/saga")


@app.get("/api/analytics/latest")
def analytics_latest(limit: int = 30):
    return forward("GET", f"{ANALYTICS_URL}/analytics/latest", params={"limit": limit})


@app.get("/api/analytics/summary")
def analytics_summary(windows: int = 5):
    return forward("GET", f"{ANALYTICS_URL}/analytics/summary", params={"windows": windows})
