import logging
from fastapi import APIRouter, HTTPException
from psycopg2.extras import RealDictCursor
from fastapi_app.database import get_db

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/popolazione", tags=["popolazione"])

@router.get("/")
def get_popolazione():
    """Recupera tutti i dati di popolazione."""
    logger.info("GET /api/popolazione")
    try:
        with get_db() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("SELECT * FROM popolazione LIMIT 100")
                results = cur.fetchall()
                logger.info(f"Retrieved {len(results)} records from popolazione")
                return {"count": len(results), "data": results}
    except Exception as exc:
        logger.error(f"Error fetching popolazione: {exc}")
        raise HTTPException(status_code=500, detail=str(exc))

@router.get("/by-anno/{anno}")
def get_popolazione_by_anno(anno: int):
    """Recupera dati di popolazione per anno specifico."""
    logger.info(f"GET /api/popolazione/by-anno/{anno}")
    try:
        with get_db() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    "SELECT * FROM popolazione WHERE AnnoDiCalendario = %s",
                    (anno,)
                )
                results = cur.fetchall()
                logger.info(f"Retrieved {len(results)} records for anno {anno}")
                return {"anno": anno, "count": len(results), "data": results}
    except Exception as exc:
        logger.error(f"Error fetching popolazione by anno: {exc}")
        raise HTTPException(status_code=500, detail=str(exc))
