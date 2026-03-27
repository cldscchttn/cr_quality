import logging
import os
from contextlib import contextmanager

import psycopg2
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from psycopg2.extras import RealDictCursor

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# App
# ---------------------------------------------------------------------------
app = FastAPI(title="REST API", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------
DATABASE_URL = os.getenv("DATABASE_URL")


def _check_database_url() -> None:
    """Raise a clear error at startup if DATABASE_URL is missing."""
    if not DATABASE_URL:
        raise RuntimeError(
            "DATABASE_URL environment variable is not set. "
            "Please configure it before starting the application."
        )
    logger.info("DATABASE_URL is configured.")


@contextmanager
def get_db():
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


# ---------------------------------------------------------------------------
# Startup event
# ---------------------------------------------------------------------------
@app.on_event("startup")
async def startup_event() -> None:
    logger.info("Application starting up.")
    _check_database_url()
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
        logger.info("Database connectivity check passed.")
    except Exception as exc:
        # Log the error but do not prevent startup — the /health endpoint
        # will still respond so Railway can surface the issue via logs.
        logger.error(
            "Database connectivity check FAILED at startup: %s", exc
        )


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------
@app.get("/health")
def health_check():
    logger.info("GET /health")
    return {"status": "ok"}


@app.get("/")
def root():
    logger.info("GET /")
    return {"message": "FastAPI REST API is running"}


@app.get("/api/test")
def test_connection():
    logger.info("GET /api/test — attempting database query.")
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                result = cur.fetchone()
        logger.info("GET /api/test — query succeeded, result: %s", result)
        return {"status": "connected", "result": result}
    except psycopg2.OperationalError as exc:
        logger.error("GET /api/test — database connection error: %s", exc)
        raise HTTPException(
            status_code=503,
            detail=f"Database connection error: {exc}",
        )
    except Exception as exc:
        logger.error("GET /api/test — unexpected error: %s", exc)
        raise HTTPException(status_code=500, detail=str(exc))

