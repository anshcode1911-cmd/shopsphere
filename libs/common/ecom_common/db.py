"""PostgreSQL (Cloud SQL, private IP) connection pool helper."""
import os

from psycopg_pool import ConnectionPool


def make_pool(db_name_env: str = "DB_NAME", min_size: int = 1, max_size: int | None = None) -> ConnectionPool:
    conninfo = (
        f"host={os.environ.get('DB_HOST', 'localhost')} "
        f"port={os.environ.get('DB_PORT', '5432')} "
        f"dbname={os.environ.get(db_name_env, 'postgres')} "
        f"user={os.environ.get('DB_USER', 'app')} "
        f"password={os.environ.get('DB_PASSWORD', 'app')} "
        "connect_timeout=5"
    )
    return ConnectionPool(
        conninfo,
        min_size=min_size,
        max_size=max_size or int(os.getenv("DB_POOL_MAX", "5")),
        open=True,
        timeout=10,
    )
