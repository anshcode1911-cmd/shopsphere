"""analytics-service — serves the real-time analytics produced by the Flink job.

Consumes: Kafka topic `analytics-results` (written by the Flink job on Dataproc).
Storage : Firestore collection `analytics_windows` (one doc per window x event type)
          — semi-structured, write-heavy time-series data.
Exposes : REST :8080  GET /analytics/latest, GET /analytics/summary
"""
import json
import os
import threading
import time
from contextlib import asynccontextmanager

from fastapi import FastAPI
from google.cloud import firestore
from prometheus_client import Counter, Gauge

from ecom_common.kafka_config import RESULTS_TOPIC, kafka_config
from ecom_common.logging_setup import log_event, setup_logging
from ecom_common.metrics import install_fastapi_metrics

log = setup_logging()
KAFKA_ENABLED = os.getenv("KAFKA_ENABLED", "true") == "true"

CONSUMED = Counter("analytics_results_consumed_total", "Window results consumed from Kafka")
LAST_WINDOW_LAG = Gauge("analytics_last_window_lag_seconds", "Now minus end of the newest window seen")

state: dict = {"stop": threading.Event()}


def consume_loop() -> None:
    from confluent_kafka import Consumer

    consumer = Consumer(kafka_config("analytics-service", {
        "group.id": os.getenv("KAFKA_GROUP_ID", "analytics-service"),
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
    }))
    consumer.subscribe([RESULTS_TOPIC])
    col = state["db"].collection("analytics_windows")
    log_event(log, "consumer started", topic=RESULTS_TOPIC)
    while not state["stop"].is_set():
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            log_event(log, "kafka error", level=40, error=str(msg.error()))
            continue
        try:
            rec = json.loads(msg.value())
            doc_id = f"{rec['windowStart']}_{rec['eventType']}"
            rec["ingestedAt"] = firestore.SERVER_TIMESTAMP
            col.document(doc_id).set(rec)
            CONSUMED.inc()
            LAST_WINDOW_LAG.set(max(0.0, time.time() - rec["windowEnd"] / 1000.0))
            log_event(log, "window result stored", window_start=rec["windowStart"],
                      event_type=rec["eventType"], events=rec.get("eventCount"),
                      unique_users=rec.get("uniqueUsers"))
        except Exception as exc:
            log_event(log, "bad analytics record", level=40, error=str(exc))
    consumer.close()


@asynccontextmanager
async def lifespan(_: FastAPI):
    state["db"] = firestore.Client(project=os.getenv("GCP_PROJECT"),
                                   database=os.getenv("FIRESTORE_DATABASE", "shopsphere"))
    if KAFKA_ENABLED:
        t = threading.Thread(target=consume_loop, name="kafka-consumer", daemon=True)
        t.start()
        state["thread"] = t
    yield
    state["stop"].set()
    if "thread" in state:
        state["thread"].join(timeout=5)


app = FastAPI(title="analytics-service", lifespan=lifespan)
install_fastapi_metrics(app)


@app.get("/healthz")
def healthz():
    alive = "thread" not in state or state["thread"].is_alive()
    return {"status": "ok" if alive else "consumer-dead"}


@app.get("/analytics/latest")
def latest(limit: int = 30):
    limit = max(1, min(limit, 200))
    docs = (state["db"].collection("analytics_windows")
            .order_by("windowEnd", direction=firestore.Query.DESCENDING)
            .limit(limit).stream())
    out = []
    for d in docs:
        rec = d.to_dict()
        rec.pop("ingestedAt", None)
        out.append(rec)
    return {"windows": out}


@app.get("/analytics/summary")
def summary(windows: int = 5):
    """Aggregate of the last N one-minute windows, grouped by event type."""
    data = latest(limit=windows * 6)["windows"]
    if not data:
        return {"windows": 0, "byType": {}}
    newest = sorted({w["windowEnd"] for w in data}, reverse=True)[:windows]
    agg: dict = {}
    for w in data:
        if w["windowEnd"] not in newest:
            continue
        a = agg.setdefault(w["eventType"], {"events": 0, "maxUniqueUsers": 0, "revenue": 0.0})
        a["events"] += w.get("eventCount", 0)
        a["maxUniqueUsers"] = max(a["maxUniqueUsers"], w.get("uniqueUsers", 0))
        a["revenue"] += w.get("revenue", 0.0)
    return {"windows": len(newest), "byType": agg}
