"""Prometheus RED metrics (Rate, Errors, Duration) shared by HTTP and gRPC services.

Metric names are identical for REST and gRPC services so that one Grafana
dashboard can show RPS / error-rate / latency for every service. The `service`
label is attached by Prometheus from the Kubernetes Service name.
"""
import time

import grpc
from prometheus_client import Counter, Histogram, start_http_server

REQUESTS = Counter(
    "app_requests_total",
    "Total requests handled",
    ["protocol", "endpoint", "status", "error"],
)
LATENCY = Histogram(
    "app_request_duration_seconds",
    "Request latency in seconds",
    ["protocol", "endpoint"],
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10),
)
SAGA_EVENTS = Counter(
    "saga_transitions_total",
    "Saga step transitions (order-service only)",
    ["step", "outcome"],
)
EVENTS_PUBLISHED = Counter(
    "events_published_total",
    "Messages published to Kafka / PubSub",
    ["transport", "topic"],
)


def observe(protocol: str, endpoint: str, status: str, is_error: bool, seconds: float) -> None:
    REQUESTS.labels(protocol, endpoint, status, "true" if is_error else "false").inc()
    LATENCY.labels(protocol, endpoint).observe(seconds)


# ----------------------------------------------------------------------------
# FastAPI / Starlette middleware
# ----------------------------------------------------------------------------
def install_fastapi_metrics(app) -> None:
    from fastapi import Request
    from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
    from starlette.responses import Response

    @app.middleware("http")
    async def _metrics_mw(request: Request, call_next):
        start = time.perf_counter()
        status = 500
        try:
            response = await call_next(request)
            status = response.status_code
            return response
        finally:
            route = request.scope.get("route")
            endpoint = getattr(route, "path", "unmatched")
            if endpoint not in ("/metrics", "/healthz", "/readyz"):
                observe("http", f"{request.method} {endpoint}", str(status), status >= 500,
                        time.perf_counter() - start)

    @app.get("/metrics", include_in_schema=False)
    def _metrics():
        return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


# ----------------------------------------------------------------------------
# gRPC server interceptor
# ----------------------------------------------------------------------------
_SERVER_ERRORS = {
    grpc.StatusCode.INTERNAL,
    grpc.StatusCode.UNKNOWN,
    grpc.StatusCode.UNAVAILABLE,
    grpc.StatusCode.DEADLINE_EXCEEDED,
    grpc.StatusCode.DATA_LOSS,
}


class GrpcMetricsInterceptor(grpc.ServerInterceptor):
    def intercept_service(self, continuation, handler_call_details):
        handler = continuation(handler_call_details)
        if handler is None or handler.unary_unary is None:
            return handler
        method = handler_call_details.method.split("/")[-1]
        inner = handler.unary_unary

        def wrapper(request, context):
            start = time.perf_counter()
            try:
                resp = inner(request, context)
                code = context.code() if hasattr(context, "code") and context.code() else grpc.StatusCode.OK
                observe("grpc", method, code.name, code in _SERVER_ERRORS, time.perf_counter() - start)
                return resp
            except Exception:
                code = context.code() if hasattr(context, "code") and context.code() else grpc.StatusCode.INTERNAL
                observe("grpc", method, code.name, code in _SERVER_ERRORS, time.perf_counter() - start)
                raise

        return grpc.unary_unary_rpc_method_handler(
            wrapper,
            request_deserializer=handler.request_deserializer,
            response_serializer=handler.response_serializer,
        )


def start_metrics_server(port: int = 9090) -> None:
    """Expose /metrics on a side port (used by gRPC services)."""
    start_http_server(port)
