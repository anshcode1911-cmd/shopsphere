"""payment-service — simulated payment gateway, exposed over gRPC.

Storage : Firestore (Native mode, database `shopsphere`) collection `payments`
          — semi-structured, high-write ledger keyed by order_id.
Exposes : gRPC :50052 (PaymentService), Prometheus /metrics on :9090.
Saga    : Charge (step 2) / Refund (compensation). Both idempotent per order_id,
          so the orchestrator can safely retry.
Failure : requests can be declined (amount above limit, random decline rate,
          or `force_failure` demo flag) which makes the order saga compensate.
"""
import os
import random
import signal
import uuid
from concurrent import futures

import grpc
from google.cloud import firestore
from grpc_health.v1 import health, health_pb2, health_pb2_grpc

import ecommerce_pb2 as pb
import ecommerce_pb2_grpc as pb_grpc
from ecom_common.logging_setup import log_event, setup_logging
from ecom_common.metrics import GrpcMetricsInterceptor, start_metrics_server

log = setup_logging()

MAX_AMOUNT = float(os.getenv("PAYMENT_MAX_AMOUNT", "100000"))
DECLINE_RATE = float(os.getenv("PAYMENT_DECLINE_RATE", "0.05"))


class PaymentServicer(pb_grpc.PaymentServiceServicer):
    def __init__(self, db: firestore.Client):
        self.col = db.collection("payments")

    def Charge(self, request, context):
        ref = self.col.document(request.order_id)
        snap = ref.get()
        if snap.exists:  # idempotent retry
            data = snap.to_dict()
            ok = data.get("status") == "CHARGED"
            return pb.ChargeResponse(success=ok, payment_id=data.get("payment_id", ""),
                                     reason="" if ok else data.get("reason", "declined"))

        reason = ""
        if request.force_failure:
            reason = "card declined (forced by demo flag)"
        elif request.amount > MAX_AMOUNT:
            reason = f"amount {request.amount:.2f} exceeds limit {MAX_AMOUNT:.2f}"
        elif random.random() < DECLINE_RATE:
            reason = "card declined by issuer (simulated)"

        payment_id = f"pay-{uuid.uuid4().hex[:12]}"
        doc = {
            "payment_id": payment_id,
            "order_id": request.order_id,
            "user_id": request.user_id,
            "amount": request.amount,
            "status": "DECLINED" if reason else "CHARGED",
            "reason": reason,
            "created_at": firestore.SERVER_TIMESTAMP,
        }
        ref.set(doc)
        if reason:
            log_event(log, "payment declined", order_id=request.order_id, reason=reason)
            return pb.ChargeResponse(success=False, payment_id=payment_id, reason=reason)
        log_event(log, "payment charged", order_id=request.order_id, amount=request.amount)
        return pb.ChargeResponse(success=True, payment_id=payment_id)

    def Refund(self, request, context):
        ref = self.col.document(request.order_id)
        snap = ref.get()
        if snap.exists and snap.to_dict().get("status") == "CHARGED":
            ref.update({"status": "REFUNDED", "refunded_at": firestore.SERVER_TIMESTAMP})
            log_event(log, "payment refunded (saga compensation)", order_id=request.order_id)
        return pb.RefundResponse(success=True)


def serve() -> None:
    db = firestore.Client(project=os.getenv("GCP_PROJECT"), database=os.getenv("FIRESTORE_DATABASE", "shopsphere"))
    start_metrics_server(int(os.getenv("METRICS_PORT", "9090")))
    server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=int(os.getenv("GRPC_WORKERS", "16"))),
        interceptors=[GrpcMetricsInterceptor()],
        # Recycle connections every 30 s so clients re-resolve DNS and discover pods
        # added by the HPA (gRPC keeps HTTP/2 connections open indefinitely otherwise).
        options=[("grpc.max_connection_age_ms", 30000), ("grpc.max_connection_age_grace_ms", 10000)],
    )
    pb_grpc.add_PaymentServiceServicer_to_server(PaymentServicer(db), server)
    health_servicer = health.HealthServicer()
    health_pb2_grpc.add_HealthServicer_to_server(health_servicer, server)
    health_servicer.set("", health_pb2.HealthCheckResponse.SERVING)
    port = os.getenv("GRPC_PORT", "50052")
    server.add_insecure_port(f"[::]:{port}")
    server.start()
    log_event(log, "payment-service gRPC server started", port=port)

    def _stop(*_):
        health_servicer.enter_graceful_shutdown()
        server.stop(grace=10)

    signal.signal(signal.SIGTERM, _stop)
    server.wait_for_termination()


if __name__ == "__main__":
    serve()
