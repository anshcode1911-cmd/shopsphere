"""invoice-function — serverless, asynchronous, event-driven (Cloud Functions 2nd gen).

Trigger : Pub/Sub topic `order-completed` (published by order-service when the
          checkout Saga succeeds) delivered via Eventarc as a CloudEvent.
Work    : renders an HTML invoice, stores it in the Cloud Storage bucket
          `<project>-shopsphere-invoices`, and records invoice + customer
          notification documents in Firestore.
Why serverless: invoicing is bursty, off the critical checkout path, and needs
          no always-on capacity -> scale-to-zero, pay-per-invocation.
"""
import base64
import html
import json
import logging
import os
from datetime import datetime, timezone

import functions_framework
from google.cloud import firestore, storage

BUCKET = os.environ.get("INVOICE_BUCKET", "")
FIRESTORE_DB = os.environ.get("FIRESTORE_DATABASE", "shopsphere")

_storage = None
_db = None


def _clients():
    global _storage, _db
    if _storage is None:
        _storage = storage.Client()
        _db = firestore.Client(database=FIRESTORE_DB)
    return _storage, _db


def _log(message: str, severity: str = "INFO", **fields) -> None:
    # Structured log line -> parsed by Cloud Logging (centralised with the GKE + Dataproc logs).
    print(json.dumps({"severity": severity, "service": "invoice-function", "message": message, **fields}))


def render_invoice(order: dict) -> str:
    rows = "".join(
        f"<tr><td>{html.escape(i['product_id'])}</td><td>{int(i['quantity'])}</td></tr>"
        for i in order.get("items", [])
    )
    return f"""<!DOCTYPE html><html><head><meta charset="utf-8"><title>Invoice {html.escape(order['order_id'])}</title>
<style>body{{font-family:sans-serif;max-width:640px;margin:40px auto}}td,th{{padding:6px 12px;border-bottom:1px solid #ddd}}</style></head>
<body><h2>ShopSphere — Tax Invoice</h2>
<p><b>Order:</b> {html.escape(order['order_id'])}<br><b>Customer:</b> {html.escape(order['user_id'])}<br>
<b>Payment:</b> {html.escape(str(order.get('payment_id')))}<br><b>Date:</b> {html.escape(order.get('completed_at', ''))}</p>
<table><tr><th>Product</th><th>Qty</th></tr>{rows}</table>
<h3>Total: ₹{float(order.get('total', 0)):,.2f}</h3></body></html>"""


@functions_framework.cloud_event
def handle_order_completed(cloud_event):
    data = cloud_event.data or {}
    message = data.get("message", {})
    try:
        order = json.loads(base64.b64decode(message.get("data", "")).decode())
    except Exception as exc:  # malformed message: ack it (don't retry forever) but log loudly
        _log("could not decode order message", "ERROR", error=str(exc))
        return

    order_id = order["order_id"]
    gcs, db = _clients()

    # Idempotency: Pub/Sub is at-least-once; skip if this invoice already exists.
    inv_ref = db.collection("invoices").document(order_id)
    if inv_ref.get().exists:
        _log("invoice already generated, skipping duplicate delivery", order_id=order_id)
        return

    blob_name = f"invoices/{datetime.now(timezone.utc):%Y/%m/%d}/{order_id}.html"
    blob = gcs.bucket(BUCKET).blob(blob_name)
    blob.upload_from_string(render_invoice(order), content_type="text/html")

    inv_ref.set({
        "order_id": order_id,
        "user_id": order["user_id"],
        "total": order.get("total", 0),
        "gcs_uri": f"gs://{BUCKET}/{blob_name}",
        "created_at": firestore.SERVER_TIMESTAMP,
    })
    db.collection("notifications").add({
        "user_id": order["user_id"],
        "channel": "email",
        "subject": f"Your ShopSphere order {order_id} is confirmed",
        "invoice": f"gs://{BUCKET}/{blob_name}",
        "created_at": firestore.SERVER_TIMESTAMP,
    })
    _log("invoice generated", order_id=order_id, gcs_uri=f"gs://{BUCKET}/{blob_name}")
