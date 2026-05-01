import logging
import re
from fastapi import APIRouter, HTTPException, Query
from psycopg2.extras import RealDictCursor
from fastapi_app.database import get_db
from typing import List

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/sopravvivenza", tags=["sopravvivenza"])


# ---------------------------------------------------------------------------
# Valori distinti per campo (riuso stesso pattern di tassi.py)
# ---------------------------------------------------------------------------
@router.get("/valori-campo")
def valori_campo(campo: str = Query(...)):
    """Valori distinti per un campo della CTE combinata."""
    VALID = {"sede", "followup", "base", "comportamento", "grado", "lateralita",
             "fascia_eta", "sesso", "anno", "comune"}
    if campo not in VALID:
        raise HTTPException(status_code=400, detail="Campo non valido")

    queries = {
        "anno":          'SELECT DISTINCT "annodicalendario"::text AS v '
                         'FROM scheda_caso ORDER BY v',
        "fascia_eta":    'SELECT DISTINCT fascia_eta AS v FROM eta ORDER BY v',
        "comune":        'SELECT DISTINCT codicecomune AS v FROM scheda_paziente ORDER BY v',
        "sesso":         'SELECT DISTINCT sesso AS v FROM scheda_paziente ORDER BY v',
        "sede":          'SELECT DISTINCT substring(sededellalesione,1,3) AS v '
                         'FROM scheda_caso ORDER BY v',
        "followup":      'SELECT DISTINCT statoinvita AS v FROM scheda_caso ORDER BY v',
        "base":          'SELECT DISTINCT basedelladiagnosi AS v FROM scheda_caso ORDER BY v',
        "comportamento": 'SELECT DISTINCT behaviour AS v FROM scheda_caso ORDER BY v',
        "grado":         'SELECT DISTINCT gradodellalesione AS v FROM scheda_caso ORDER BY v',
        "lateralita":    'SELECT DISTINCT lateralita AS v FROM scheda_caso ORDER BY v',
    }

    try:
        with get_db() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(queries[campo])
                results = [row["v"] for row in cur.fetchall()]
                return results
    except Exception as exc:
        logger.error(f"Error fetching valori campo: {exc}")
        raise HTTPException(status_code=500, detail=str(exc))


# ---------------------------------------------------------------------------
# Sopravvivenza a 5 anni (Kaplan-Meier osservata)
# ---------------------------------------------------------------------------
@router.get("/sopravvivenza")
def get_sopravvivenza(
    rows:          List[str] = Query(default=[]),
    cols:          List[str] = Query(default=[]),
    anno:          List[str] = Query(default=[]),
    sesso:         List[str] = Query(default=[]),
    comune:        List[str] = Query(default=[]),
    fascia_eta:    List[str] = Query(default=[]),
    sede:          List[str] = Query(default=[]),
    followup:      List[str] = Query(default=[]),
    base:          List[str] = Query(default=[]),
    comportamento: List[str] = Query(default=[]),
    grado:         List[str] = Query(default=[]),
    lateralita:    List[str] = Query(default=[]),
):
    """Calcola la sopravvivenza osservata a 5 anni (Kaplan-Meier)."""
    logger.info("GET /api/sopravvivenza/sopravvivenza rows=%s cols=%s", rows, cols)

    VALID_CASI = {"sede", "followup", "base", "comportamento", "grado", "lateralita"}
    VALID_PAZ  = {"fascia_eta", "sesso", "anno", "comune"}

    row_dims_casi = [d for d in rows if d in VALID_CASI]
    row_dims_paz  = [d for d in rows if d in VALID_PAZ]
    col_dims_casi = [d for d in cols if d in VALID_CASI]
    col_dims_paz  = [d for d in cols if d in VALID_PAZ]

    all_dims_casi = list(dict.fromkeys(row_dims_casi + col_dims_casi))
    all_dims_paz  = list(dict.fromkeys(row_dims_paz  + col_dims_paz))
    all_dims      = list(dict.fromkeys(all_dims_casi + all_dims_paz))

    SELECT_RENAME_MAP = {
        "fascia_eta":    "eta.eta19 as fascia_eta",
        "anno":          "annodicalendario as anno",
        "comune":        "codicecomune as comune",
        "sesso":         "sesso",
        "followup":      "statoinvita as followup",
        "base":          "basedelladiagnosi as base",
        "comportamento": "behaviour as comportamento",
        "grado":         "gradodellalesione as grado",
        "sede":          "substring(sededellalesione,1,3) as sede",
        "lateralita":    "lateralita",
    }

    all_dims_select = [SELECT_RENAME_MAP.get(d, d) for d in all_dims]

    all_dims_sql            = ", ".join(all_dims)           if all_dims        else ""
    all_dims_sql_comma      = all_dims_sql + ", "           if all_dims        else ""
    all_dims_sql_1comma     = ", " + all_dims_sql           if all_dims        else ""
    all_dims_select_sql     = ", ".join(all_dims_select) + ", " if all_dims_select else ""
    all_dims_sql_groupby    = "group by " + all_dims_sql    if all_dims        else ""
    partition_by_clause     = "partition by " + all_dims_sql if all_dims       else ""

    # ── WHERE conditions ─────────────────────────────────────────────────────
    conditions = []

    if anno:
        anno_ints = [int(a) for a in anno if a.isdigit()]
        if anno_ints:
            conditions.append(f"annodicalendario IN ({','.join(map(str, anno_ints))})")

    if sesso:
        sesso_safe = [s for s in sesso if s in ("1", "2", "9")]
        if sesso_safe:
            conditions.append(f"sesso::text IN ({','.join(repr(s) for s in sesso_safe)})")

    if comune:
        comune_safe = [c for c in comune if re.match(r"^\d{6}$", c)]
        if comune_safe:
            conditions.append(f"codicecomune::text IN ({','.join(repr(c) for c in comune_safe)})")

    if fascia_eta:
        fascia_safe = [f for f in fascia_eta if re.match(r"^\d{2}$", f)]
        if fascia_safe:
            conditions.append(f"eta.eta19::text IN ({','.join(repr(f) for f in fascia_safe)})")

    if sede:
        sede_safe = [s.upper() for s in sede if re.match(r"^[A-Za-z0-9]{3}$", s)]
        if sede_safe:
            conditions.append(f"substring(sededellalesione,1,3) IN ({','.join(repr(s) for s in sede_safe)})")

    if followup:
        followup_safe = [f for f in followup if f in ("2", "3")]
        if followup_safe:
            conditions.append(f"statoinvita::text IN ({','.join(repr(f) for f in followup_safe)})")

    if base:
        base_safe = [b for b in base if b in tuple("0123456789")]
        if base_safe:
            conditions.append(f"basedelladiagnosi::text IN ({','.join(repr(b) for b in base_safe)})")

    if comportamento:
        comp_safe = [c for c in comportamento if c in ("0", "1", "2", "3")]
        if comp_safe:
            conditions.append(f"behaviour::text IN ({','.join(repr(c) for c in comp_safe)})")

    if grado:
        grado_safe = [g for g in grado if g in tuple("123456789")]
        if grado_safe:
            conditions.append(f"gradodellalesione::text IN ({','.join(repr(g) for g in grado_safe)})")

    if lateralita:
        lat_safe = [l for l in lateralita if l in ("1", "2", "3", "9")]
        if lat_safe:
            conditions.append(f"lateralita::text IN ({','.join(repr(l) for l in lat_safe)})")

    extra_where = (" AND " + " AND ".join(conditions)) if conditions else ""

    estrazione = """
        WITH
        tempi AS (
            SELECT DISTINCT intervallostatoinvita AS intervallo
            FROM scheda_caso
            WHERE intervallostatoinvita BETWEEN '0 year' AND '5 year'
        ),
        dati AS (
            SELECT
                {all_dims_select_sql}
                tempi.intervallo,
                count(CASE WHEN scheda_caso.intervallostatoinvita =  tempi.intervallo
                                AND scheda_paziente.statoinvita IN ('2') THEN true END) AS morti,
                count(CASE WHEN scheda_caso.intervallostatoinvita >  tempi.intervallo
                                AND scheda_paziente.statoinvita IN ('2') THEN true
                           WHEN scheda_caso.intervallostatoinvita >= tempi.intervallo
                                AND scheda_paziente.statoinvita IN ('1','3') THEN true
                      END) AS vivi,
                count(CASE WHEN scheda_caso.intervallostatoinvita =  tempi.intervallo
                                AND scheda_paziente.statoinvita IN ('1','3') THEN true END) AS persi
            FROM scheda_paziente
            NATURAL INNER JOIN scheda_caso
            NATURAL INNER JOIN eta
            CROSS JOIN tempi
            WHERE scheda_caso.intervallostatoinvita >= '0 years'
                {extra_where}
            GROUP BY tempi.intervallo
                {all_dims_sql_1comma}
        ),
        dati_sopravvivenza AS (
            SELECT
                {all_dims_sql_comma}
                intervallo,
                morti,
                vivi,
                persi,
                CASE WHEN vivi > 0 AND morti > 0
                     THEN 1.0 * vivi / (vivi + morti) END AS sopravvivenza_intervallo,
                exp(
                    sum(CASE WHEN vivi > 0 AND morti > 0
                             THEN ln(1.0 * vivi / (vivi + morti))
                             ELSE 0 END)
                    OVER ({partition_by_clause} ORDER BY intervallo
                          ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
                ) AS sopravvivenza_cumulata
            FROM dati
        )
        SELECT
            {all_dims_sql_comma}
            min(sopravvivenza_cumulata) AS sopravvivenza
        FROM dati_sopravvivenza
        GROUP BY {all_dims_sql_groupby_body}
        ORDER BY {all_dims_sql_orderby_body}
        ;
    """.format(
        all_dims_select_sql=all_dims_select_sql,
        all_dims_sql_comma=all_dims_sql_comma,
        all_dims_sql_1comma=all_dims_sql_1comma,
        partition_by_clause=partition_by_clause,
        extra_where=extra_where,
        all_dims_sql_groupby_body=all_dims_sql if all_dims else "(SELECT NULL)",
        all_dims_sql_orderby_body=all_dims_sql if all_dims else "1",
    )

    print(estrazione)

    try:
        with get_db() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(estrazione)
                results = [dict(r) for r in cur.fetchall()]
                logger.info(f"Retrieved {len(results)} sopravvivenza records")
                return {"count": len(results), "data": results}
    except Exception as exc:
        logger.error(f"Error fetching sopravvivenza: {exc}")
        raise HTTPException(status_code=500, detail=str(exc))
