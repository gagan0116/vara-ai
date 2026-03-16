# db_verification/db.py
from __future__ import annotations

import os
from contextlib import contextmanager
from typing import Any, Dict, Iterator, List

from google.cloud.sql.connector import Connector, IPTypes

def _required_env(name: str) -> str:
    val = os.getenv(name)
    if not val or not val.strip():
        raise RuntimeError(f"Missing required environment variable: {name}")
    return val.strip()

CLOUD_INSTANCE = _required_env("CLOUD_INSTANCE")  
CLOUD_DB_NAME = _required_env("CLOUD_DB_NAME")
CLOUD_DB_USER = _required_env("CLOUD_DB_USER")
CLOUD_DB_PASS = _required_env("CLOUD_DB_PASS")

_connector = Connector()


@contextmanager
def db_connection() -> Iterator[Any]:
    """
    Opens a DB connection using Cloud SQL Python Connector + pg8000 and closes it reliably.
    Tools should use:
        with db_connection() as conn:
            ...
    """
    conn = None
    try:
        conn = _connector.connect(
            CLOUD_INSTANCE,
            "pg8000",
            user=CLOUD_DB_USER,
            password=CLOUD_DB_PASS,
            db=CLOUD_DB_NAME,
            ip_type=IPTypes.PUBLIC,
        )
        yield conn
    finally:
        try:
            if conn is not None:
                conn.close()
        except Exception:
            pass


def rows_as_dicts(cur) -> List[Dict[str, Any]]:
    """Convert cursor results to a list of dicts using column names."""
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]


def close_connector() -> None:
    """Call on shutdown if you want explicit cleanup."""
    try:
        _connector.close()
    except Exception:
        pass
