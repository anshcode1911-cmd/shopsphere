"""catalog-service — product catalogue + inventory, exposed over gRPC.

Storage : Cloud SQL for PostgreSQL (database `catalog`).
Exposes : gRPC :50051 (CatalogService), Prometheus /metrics on :9090,
          standard gRPC health service (used by k8s gRPC probes).
Saga    : ReserveStock (step 1) / ReleaseStock (compensation) are local ACID
          transactions, idempotent per order_id.
"""
import os
import signal
from concurrent import futures

import grpc
from grpc_health.v1 import health, health_pb2, health_pb2_grpc

import ecommerce_pb2 as pb
import ecommerce_pb2_grpc as pb_grpc
from ecom_common.db import make_pool
from ecom_common.logging_setup import log_event, setup_logging
from ecom_common.metrics import GrpcMetricsInterceptor, start_metrics_server

log = setup_logging()

SCHEMA = """
CREATE TABLE IF NOT EXISTS products (
    id          TEXT PRIMARY KEY,
    name        TEXT NOT NULL,
    description TEXT NOT NULL,
    price       NUMERIC(10,2) NOT NULL CHECK (price >= 0),
    stock       INTEGER NOT NULL CHECK (stock >= 0),
    category    TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS reservations (
    order_id    TEXT NOT NULL,
    product_id  TEXT NOT NULL REFERENCES products(id),
    quantity    INTEGER NOT NULL,
    unit_price  NUMERIC(10,2) NOT NULL,
    status      TEXT NOT NULL,             -- RESERVED | RELEASED
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (order_id, product_id)
);
"""

SEED = [
    ("p-001", "Wireless Earbuds", "Bluetooth 5.3 earbuds with ANC", 2999.00, 100000, "electronics"),
    ("p-002", "Mechanical Keyboard", "Hot-swappable 75% keyboard", 5499.00, 100000, "electronics"),
    ("p-003", "USB-C Hub", "7-in-1 hub with HDMI and PD", 1899.00, 100000, "electronics"),
    ("p-004", "Running Shoes", "Lightweight daily trainers", 3999.00, 100000, "fashion"),
    ("p-005", "Denim Jacket", "Classic fit, washed blue", 2499.00, 100000, "fashion"),
    ("p-006", "Steel Bottle", "1L insulated water bottle", 799.00, 100000, "home"),
    ("p-007", "Desk Lamp", "LED lamp with wireless charger", 1599.00, 100000, "home"),
    ("p-008", "Yoga Mat", "6mm anti-slip mat", 999.00, 100000, "sports"),
    ("p-009", "Cloud Computing Book", "Concepts, technology & architecture", 649.00, 100000, "books"),
    ("p-010", "Coffee Grinder", "Burr grinder, 40 settings", 3299.00, 100000, "home"),
    # Deliberately scarce item: lets the demo show a saga failing at step 1.
    ("p-099", "Limited Edition Sneakers", "Only 3 pairs ever made", 19999.00, 3, "fashion"),
]


class _Rejected(Exception):
    """Business rejection raised inside a DB transaction (forces rollback)."""


class CatalogServicer(pb_grpc.CatalogServiceServicer):
    def __init__(self, pool):
        self.pool = pool

    @staticmethod
    def _to_pb(row) -> pb.Product:
        return pb.Product(id=row[0], name=row[1], description=row[2], price=float(row[3]),
                          stock=row[4], category=row[5])

    def ListProducts(self, request, context):
        sql = "SELECT id, name, description, price, stock, category FROM products"
        args = ()
        if request.category:
            sql += " WHERE category = %s"
            args = (request.category,)
        with self.pool.connection() as conn:
            rows = conn.execute(sql + " ORDER BY id", args).fetchall()
        return pb.ListProductsResponse(products=[self._to_pb(r) for r in rows])

    def GetProduct(self, request, context):
        with self.pool.connection() as conn:
            row = conn.execute(
                "SELECT id, name, description, price, stock, category FROM products WHERE id = %s",
                (request.id,),
            ).fetchone()
        if not row:
            context.abort(grpc.StatusCode.NOT_FOUND, f"product {request.id} not found")
        return self._to_pb(row)

    def ReserveStock(self, request, context):
        try:
            return self._reserve(request)
        except _Rejected as exc:
            # The local transaction was rolled back, so nothing is reserved.
            log_event(log, "stock reservation rejected", order_id=request.order_id, reason=str(exc))
            return pb.ReserveStockResponse(success=False, reason=str(exc))

    def _reserve(self, request):
        if not request.items:
            return pb.ReserveStockResponse(success=False, reason="no items")
        with self.pool.connection() as conn:
            with conn.transaction():
                # Idempotency: a retried request for the same order returns the original result.
                existing = conn.execute(
                    "SELECT quantity, unit_price, status FROM reservations WHERE order_id = %s",
                    (request.order_id,),
                ).fetchall()
                if existing:
                    if all(r[2] == "RESERVED" for r in existing):
                        total = sum(float(r[1]) * r[0] for r in existing)
                        return pb.ReserveStockResponse(success=True, total_amount=total)
                    return pb.ReserveStockResponse(success=False, reason="reservation already released")

                total = 0.0
                # Lock rows in a stable order to avoid deadlocks between concurrent orders.
                for item in sorted(request.items, key=lambda i: i.product_id):
                    if item.quantity <= 0:
                        raise _Rejected(f"invalid quantity for {item.product_id}")
                    row = conn.execute(
                        "SELECT stock, price FROM products WHERE id = %s FOR UPDATE",
                        (item.product_id,),
                    ).fetchone()
                    if row is None or row[0] < item.quantity:
                        reason = ("unknown product " if row is None else "insufficient stock for ") + item.product_id
                        raise _Rejected(reason)  # rolls back the whole local transaction
                    conn.execute("UPDATE products SET stock = stock - %s WHERE id = %s",
                                 (item.quantity, item.product_id))
                    conn.execute(
                        "INSERT INTO reservations (order_id, product_id, quantity, unit_price, status) "
                        "VALUES (%s, %s, %s, %s, 'RESERVED')",
                        (request.order_id, item.product_id, item.quantity, row[1]),
                    )
                    total += float(row[1]) * item.quantity
        log_event(log, "stock reserved", order_id=request.order_id, total=total)
        return pb.ReserveStockResponse(success=True, total_amount=round(total, 2))

    def ReleaseStock(self, request, context):
        with self.pool.connection() as conn:
            with conn.transaction():
                rows = conn.execute(
                    "SELECT product_id, quantity FROM reservations "
                    "WHERE order_id = %s AND status = 'RESERVED' FOR UPDATE",
                    (request.order_id,),
                ).fetchall()
                for product_id, qty in rows:
                    conn.execute("UPDATE products SET stock = stock + %s WHERE id = %s", (qty, product_id))
                conn.execute("UPDATE reservations SET status = 'RELEASED' WHERE order_id = %s",
                             (request.order_id,))
        log_event(log, "stock released (saga compensation)", order_id=request.order_id, lines=len(rows))
        return pb.ReleaseStockResponse(success=True)


def init_db(pool) -> None:
    with pool.connection() as conn:
        conn.execute("SELECT pg_advisory_xact_lock(43)")
        conn.execute(SCHEMA)
        with conn.cursor() as cur:
            cur.executemany(
                "INSERT INTO products (id, name, description, price, stock, category) "
                "VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT (id) DO NOTHING",
                SEED,
            )
    log_event(log, "catalog database initialised", products=len(SEED))


def serve() -> None:
    pool = make_pool(max_size=int(os.getenv("DB_POOL_MAX", "10")))
    init_db(pool)
    start_metrics_server(int(os.getenv("METRICS_PORT", "9090")))

    server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=int(os.getenv("GRPC_WORKERS", "16"))),
        interceptors=[GrpcMetricsInterceptor()],
        # Recycle connections every 30 s so clients re-resolve DNS and discover pods
        # added by the HPA (gRPC keeps HTTP/2 connections open indefinitely otherwise).
        options=[("grpc.max_connection_age_ms", 30000), ("grpc.max_connection_age_grace_ms", 10000)],
    )
    pb_grpc.add_CatalogServiceServicer_to_server(CatalogServicer(pool), server)
    health_servicer = health.HealthServicer()
    health_pb2_grpc.add_HealthServicer_to_server(health_servicer, server)
    health_servicer.set("", health_pb2.HealthCheckResponse.SERVING)

    port = os.getenv("GRPC_PORT", "50051")
    server.add_insecure_port(f"[::]:{port}")
    server.start()
    log_event(log, "catalog-service gRPC server started", port=port)

    def _stop(*_):
        health_servicer.enter_graceful_shutdown()
        server.stop(grace=10)

    signal.signal(signal.SIGTERM, _stop)
    server.wait_for_termination()
    pool.close()


if __name__ == "__main__":
    serve()
