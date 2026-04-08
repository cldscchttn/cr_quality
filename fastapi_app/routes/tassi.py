import logging
from fastapi import APIRouter, HTTPException, Query
from psycopg2.extras import RealDictCursor
from fastapi_app.database import get_db
from typing import Optional, List, Any
import re

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/tassi", tags=["tassi"])

@router.get("/grezzi_eta")
def get_tassi_eta(
    rows:        List[str] = Query(default=[]),
    cols:        List[str] = Query(default=[]),
    anno:        List[str] = Query(default=[]),
    sesso:       List[str] = Query(default=[]),
    ):
    """Recupera i tassi per fascia di età."""
    logger.info("GET /api/tassi/grezzi_eta with rows=%s cols=%s", rows, cols)
    VALID_CASI = {"sede", "followup", "base", "comportamento", "grado", "lateralita"}
    VALID_POP = {"fascia_eta", "sesso", "anno", "comune"}
    row_dims_casi = [d for d in rows if d in VALID_CASI]
    row_dims_pop = [d for d in rows if d in VALID_POP]
    row_dims = list(dict.fromkeys(row_dims_casi + row_dims_pop))
    col_dims_casi = [d for d in cols if d in VALID_CASI]
    col_dims_pop = [d for d in cols if d in VALID_POP]
    col_dims = list(dict.fromkeys(col_dims_casi + col_dims_pop))
    all_dims_casi = list(dict.fromkeys(row_dims_casi + col_dims_casi))
    all_dims_pop = list(dict.fromkeys(row_dims_pop + col_dims_pop))
    all_dims = list(dict.fromkeys(all_dims_casi + all_dims_pop))


    SELECT_RENAME_MAP = {
        "fascia_eta": "eta.eta19 as fascia_eta",
        "anno": "annodicalendario as anno",
        "comune": "codicecomune as comune",
        "followup": "statoinvita as followup",
        "base": "basedelladiagnosi as base",
        "comportamento": "behaviour as comportamento",
        "grado": "gradodellalesione as grado",
        "sede": "substring(sededellalesione,1,3) as sede",
    }

    all_dims_pop_select = [SELECT_RENAME_MAP.get(d, d) for d in all_dims_pop]
    all_dims_select = [SELECT_RENAME_MAP.get(d, d) for d in all_dims]
    all_dims_pop_select = [SELECT_RENAME_MAP.get(d, d) for d in all_dims_pop]
    all_dims_select = [SELECT_RENAME_MAP.get(d, d) for d in all_dims]

    all_dims_sql = ", ".join(all_dims) if all_dims else ""
    all_dims_sql_comma = all_dims_sql + ", " if all_dims else ""
    all_dims_select_sql = ", ".join(all_dims_select) + ", " if all_dims_select else ""
    all_dims_sql_groupby = "group by " + all_dims_sql if all_dims else ""
    all_dims_pop_sql = ", ".join(all_dims_pop) if all_dims_pop else ""
    all_dims_pop_select_sql = ", ".join(all_dims_pop_select) + ", "  if all_dims_pop_select else ""
    all_dims_pop_sql_groupby = "group by " + all_dims_pop_sql if all_dims_pop else ""


    # ── Costruzione clausola WHERE con parametri psycopg2 (sicura da SQL injection) ──
    conditions_casi = []
    conditions_pop = []

    WHERE_RENAME_MAP = {
        "fascia_eta": "eta.eta19",
        "anno": "annodicalendario",
        "comune": "codicecomune",
        "followup": "statoinvita",
        "base": "basedelladiagnosi",
        "comportamento": "behaviour",
        "grado": "gradodellalesione",
        "sede": "substring(sededellalesione,1,3)",
    }

    if anno:
        anno_ints = []
        for a in anno:
            try:
                anno_ints.append(int(a))
            except ValueError:
                pass
        if anno_ints:
            placeholders = ",".join(map(str, anno_ints))
            conditions_pop.append(f"annodicalendario IN ({placeholders})")
            conditions_casi.append(f"annodicalendario IN ({placeholders})")

    if sesso:
        sesso_safe = [s for s in sesso if s in ("1", "2", "9")]
        if sesso_safe:
            placeholders = ",".join(sesso_safe)
            conditions_pop.append(f"sesso IN ({placeholders})")
            conditions_casi.append(f"sesso IN ({placeholders})")

    where_sql_casi = ("WHERE " + " AND ".join(conditions_casi)) if conditions_casi else ""
    where_sql_pop = ("WHERE " + " AND ".join(conditions_pop)) if conditions_pop else ""

    estrazione = """
        with
        casi as (
        select
        {all_dims_select_sql}
        count(*) as conteggio
        FROM scheda_paziente
        natural inner join scheda_caso
        natural inner join eta
        {where_sql_casi}
        {all_dims_sql_groupby}
        ), 
        popolazione as (
        select
        {all_dims_pop_select_sql}
        sum(popolazione.popolazioneresidente) as popolazione
        from popolazione
        natural inner join eta
        {where_sql_pop}
        {all_dims_pop_sql_groupby}
        )
        select
        {all_dims_sql_comma}
        c.conteggio,
        p.popolazione,
        (c.conteggio::float / p.popolazione)*100000 as tasso
        from casi c
        natural left outer join popolazione p
        order by {all_dims_sql}
        ;
    """.format(
                all_dims_sql=all_dims_sql,
                all_dims_sql_comma=all_dims_sql_comma,
                all_dims_select_sql=all_dims_select_sql,
                all_dims_sql_groupby=all_dims_sql_groupby,
                all_dims_pop_sql=all_dims_pop_sql,
                all_dims_pop_select_sql=all_dims_pop_select_sql,
                all_dims_pop_sql_groupby=all_dims_pop_sql_groupby,
                where_sql_casi=where_sql_casi,
                where_sql_pop=where_sql_pop,
            )
    
    print(estrazione)

    try:
        with get_db() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(estrazione)
                results = cur.fetchall()
                logger.info(f"Retrieved {len(results)} tassi records")
                return {"count": len(results), "data": results}
    except Exception as exc:
        logger.error(f"Error fetching tassi: {exc}")
        raise HTTPException(status_code=500, detail=str(exc))
   



