import logging
from fastapi import APIRouter, HTTPException
from psycopg2.extras import RealDictCursor
from fastapi_app.database import get_db

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/casi", tags=["casi"])

@router.get("/")
def get_casi():
    """Recupera tutti i casi."""
    logger.info("GET /api/casi")
    try:
        with get_db() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("SELECT * FROM scheda_caso LIMIT 100")
                results = cur.fetchall()
                logger.info(f"Retrieved {len(results)} casi")
                return {"count": len(results), "data": results}
    except Exception as exc:
        logger.error(f"Error fetching casi: {exc}")
        raise HTTPException(status_code=500, detail=str(exc))

@router.get("/{codice_caso}")
def get_caso(codice_caso: str):
    """Recupera un caso specifico."""
    logger.info(f"GET /api/casi/{codice_caso}")
    try:
        with get_db() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    "SELECT * FROM scheda_caso WHERE CodiceAnonimoDelCasoSpecifico = %s",
                    (codice_caso,)
                )
                result = cur.fetchone()
                if not result:
                    raise HTTPException(status_code=404, detail="Caso non trovato")
                return result
    except HTTPException:
        raise
    except Exception as exc:
        logger.error(f"Error fetching caso: {exc}")
        raise HTTPException(status_code=500, detail=str(exc))
