import logging
import os
from contextlib import contextmanager
import psycopg2
from psycopg2.extras import RealDictCursor

logger = logging.getLogger(__name__)

DATABASE_URL = os.getenv("DATABASE_URL")

def check_database_url() -> None:
    """Raise a clear error at startup if DATABASE_URL is missing."""
    if not DATABASE_URL:
        raise RuntimeError(
            "DATABASE_URL environment variable is not set. "
            "Please configure it before starting the application."
        )
    logger.info("DATABASE_URL is configured.")

@contextmanager
def get_db():
    """Context manager for database connections."""
    if not DATABASE_URL:
        raise RuntimeError(
            "DATABASE_URL environment variable is not set. "
            "Cannot open a database connection."
        )
    conn = None
    try:
        logger.debug("Opening database connection.")
        conn = psycopg2.connect(DATABASE_URL)
        yield conn
    except psycopg2.OperationalError as exc:
        logger.error("Failed to connect to the database: %s", exc)
        raise
    finally:
        if conn is not None:
            conn.close()
            logger.debug("Database connection closed.")
