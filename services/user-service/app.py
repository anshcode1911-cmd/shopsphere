"""user-service — REST API for customer accounts.

Storage : Cloud SQL for PostgreSQL (database `users`) — relational data.
Exposes : REST on :8080 (POST/GET /users), /metrics, /healthz.
Callers : gateway (REST), order-service (REST, to validate the buyer).
"""
import os
import uuid
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, EmailStr, Field

from ecom_common.db import make_pool
from ecom_common.logging_setup import log_event, setup_logging
from ecom_common.metrics import install_fastapi_metrics

log = setup_logging()
pool = None

SCHEMA = """
CREATE TABLE IF NOT EXISTS users (
    id          TEXT PRIMARY KEY,
    name        TEXT NOT NULL,
    email       TEXT NOT NULL UNIQUE,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
"""


def init_db() -> None:
    with pool.connection() as conn:
        # transaction-scoped lock: only one replica migrates/seeds at a time
        conn.execute("SELECT pg_advisory_xact_lock(42)")
        conn.execute(SCHEMA)
        # Deterministic demo users (u-0001 .. u-0200) so the load test has valid buyers.
        seed = int(os.getenv("SEED_USERS", "200"))
        with conn.cursor() as cur:
            cur.executemany(
                "INSERT INTO users (id, name, email) VALUES (%s, %s, %s) ON CONFLICT DO NOTHING",
                [(f"u-{i:04d}", f"Demo User {i}", f"user{i}@shopsphere.dev") for i in range(1, seed + 1)],
            )
    log_event(log, "database initialised", seeded_users=os.getenv("SEED_USERS", "200"))


@asynccontextmanager
async def lifespan(_: FastAPI):
    global pool
    pool = make_pool()
    init_db()
    yield
    pool.close()


app = FastAPI(title="user-service", lifespan=lifespan)
install_fastapi_metrics(app)


class UserIn(BaseModel):
    name: str = Field(min_length=1, max_length=100)
    email: EmailStr


class UserOut(BaseModel):
    id: str
    name: str
    email: str


@app.get("/healthz")
def healthz():
    return {"status": "ok"}


@app.get("/readyz")
def readyz():
    with pool.connection() as conn:
        conn.execute("SELECT 1")
    return {"status": "ready"}


@app.post("/users", status_code=201, response_model=UserOut)
def create_user(body: UserIn):
    user_id = f"u-{uuid.uuid4().hex[:10]}"
    with pool.connection() as conn:
        try:
            conn.execute(
                "INSERT INTO users (id, name, email) VALUES (%s, %s, %s)",
                (user_id, body.name, body.email),
            )
        except Exception as exc:  # unique violation
            if "unique" in str(exc).lower():
                raise HTTPException(409, "email already registered") from exc
            raise
    log_event(log, "user created", user_id=user_id)
    return UserOut(id=user_id, name=body.name, email=body.email)


@app.get("/users/{user_id}", response_model=UserOut)
def get_user(user_id: str):
    with pool.connection() as conn:
        row = conn.execute("SELECT id, name, email FROM users WHERE id = %s", (user_id,)).fetchone()
    if not row:
        raise HTTPException(404, "user not found")
    return UserOut(id=row[0], name=row[1], email=row[2])


@app.get("/users", response_model=list[UserOut])
def list_users(limit: int = 20, offset: int = 0):
    limit = max(1, min(limit, 100))
    with pool.connection() as conn:
        rows = conn.execute(
            "SELECT id, name, email FROM users ORDER BY id LIMIT %s OFFSET %s", (limit, offset)
        ).fetchall()
    return [UserOut(id=r[0], name=r[1], email=r[2]) for r in rows]
