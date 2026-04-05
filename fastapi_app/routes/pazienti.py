import logging
from fastapi import APIRouter, HTTPException
from psycopg2.extras import RealDictCursor
from fastapi_app.database import get_db

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/pazienti", tags=["pazienti"])

@router.get("/")
def get_pazienti():
    """Recupera tutti i pazienti."""
    logger.info("GET /api/pazienti")
    try:
        with get_db() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("SELECT * FROM scheda_paziente LIMIT 100")
                results = cur.fetchall()
                logger.info(f"Retrieved {len(results)} pazienti")
                return {"count": len(results), "data": results}
    except Exception as exc:
        logger.error(f"Error fetching pazienti: {exc}")
        raise HTTPException(status_code=500, detail=str(exc))

@router.get("/{codice_paziente}")
def get_paziente(codice_paziente: str):
    """Recupera un paziente specifico."""
    logger.info(f"GET /api/pazienti/{codice_paziente}")
    try:
        with get_db() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    "SELECT * FROM scheda_paziente WHERE CodiceAnonimoDelPazienteSpecifico = %s",
                    (codice_paziente,)
                )
                result = cur.fetchone()
                if not result:
                    raise HTTPException(status_code=404, detail="Paziente non trovato")
                return result
    except HTTPException:
        raise
    except Exception as exc:
        logger.error(f"Error fetching paziente: {exc}")
        raise HTTPException(status_code=500, detail=str(exc))
