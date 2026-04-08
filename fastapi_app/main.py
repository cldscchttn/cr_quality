import logging
import os
from pathlib import Path
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware

# Carica variabili da .env PRIMA di importare database
env_path = Path(__file__).parent.parent / ".env"
print(f"Loading .env from: {env_path}")
print(f".env exists: {env_path.exists()}")
load_dotenv(dotenv_path=env_path, override=True)

# Verifica che DATABASE_URL sia caricato
print(f"DATABASE_URL after load_dotenv: {os.getenv('DATABASE_URL')}")

from fastapi_app.database import check_database_url
from fastapi_app.routes import popolazione, pazienti, casi, tassi

# Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

# App
app = FastAPI(title="REST API", version="1.0.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers
app.include_router(popolazione.router)
app.include_router(pazienti.router)
app.include_router(casi.router)
app.include_router(tassi.router)

# Startup event
@app.on_event("startup")
async def startup_event() -> None:
    logger.info("Application starting up.")
    check_database_url()
    try:
        from fastapi_app.database import get_db
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                logger.info("Database connectivity check passed.")
    except Exception as exc:
        logger.error("Database connectivity check FAILED at startup: %s", exc)

# Endpoints
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
        from fastapi_app.database import get_db
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                result = cur.fetchone()
                logger.info("GET /api/test — query succeeded, result: %s", result)
                return {"status": "connected", "result": result}
    except Exception as exc:
        logger.error("GET /api/test — unexpected error: %s", exc)
        raise HTTPException(status_code=500, detail=str(exc))
    