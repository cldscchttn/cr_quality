import logging
from fastapi import APIRouter, HTTPException, Query
from psycopg2.extras import RealDictCursor
from fastapi_app.database import get_db
from typing import Optional, List, Any
import re

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/tassi", tags=["tassi"])




@router.get("/valori-campo")
def valori_campo(campo: str = Query(...)):
    """Valori distinti per un campo della CTE combinata."""
    import re
    VALID = {"sede", "followup", "base", "comportamento", "grado", "lateralita", "fascia_eta", "sesso", "anno", "comune"}
    if campo not in VALID:
        raise HTTPException(status_code=400, detail="Campo non valido")

    queries = {
        "anno":          'SELECT DISTINCT "annodicalendario"::text AS v '
                         'FROM popolazione ORDER BY v',
        "fascia_eta":    'SELECT DISTINCT fascia_eta AS v '
                         'FROM eta ORDER BY v',
        "comune":        'SELECT DISTINCT codicecomune AS v '
                         'FROM popolazione ORDER BY v',
        "sesso":         'SELECT DISTINCT sesso AS v '
                         'FROM popolazione ORDER BY v',
        "sede":          'SELECT DISTINCT substring(sededellalesione,1,3) AS v '
                         'FROM scheda_caso ORDER BY v',
        "followup":      'SELECT DISTINCT statoinvita AS v '
                         'FROM scheda_caso ORDER BY v',
        "base":          'SELECT DISTINCT basedelladiagnosi AS v '
                         'FROM scheda_caso ORDER BY v',
        "comportamento": 'SELECT DISTINCT behaviour AS v '
                         'FROM scheda_caso ORDER BY v',
        "grado":         'SELECT DISTINCT gradodellalesione AS v '
                         'FROM scheda_caso ORDER BY v',
        "lateralita":    'SELECT DISTINCT lateralita AS v '
                         'FROM scheda_caso ORDER BY v',
    }

    try:
        with get_db() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(queries[campo])
                results = [row["v"] for row in cur.fetchall()]
                logger.info(f"Retrieved {len(results)} valori campo records")
                return results
    except Exception as exc:
        logger.error(f"Error fetching valori campo: {exc}")
        raise HTTPException(status_code=500, detail=str(exc))
    






@router.get("/grezzi_eta")
def get_tassi_eta(
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
    all_dims_sql_1comma = ", " + all_dims_sql  if all_dims else ""
    all_dims_sql_2comma = ", " + all_dims_sql + ", " if all_dims else ""
    all_dims_sql_orderby = "order by " + all_dims_sql if all_dims else ""
    all_dims_select_sql = ", ".join(all_dims_select) + ", " if all_dims_select else ""
    all_dims_sql_groupby = "group by " + all_dims_sql if all_dims else ""
    all_dims_pop_sql = ", ".join(all_dims_pop) if all_dims_pop else ""
    all_dims_pop_sql_1comma = ", " + all_dims_pop_sql  if all_dims_pop else ""
    all_dims_pop_select_sql = ", ".join(all_dims_pop_select) + ", "  if all_dims_pop_select else ""
    all_dims_pop_sql_groupby = "group by " + all_dims_pop_sql if all_dims_pop else ""


    # ── Costruzione clausola WHERE con parametri psycopg2 (sicura da SQL injection) ──
    conditions_casi = []
    conditions_pop = []

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
            placeholders = ",".join(f"'{s}'" for s in sesso_safe)
            conditions_pop.append(f"sesso::text IN ({placeholders})")
            conditions_casi.append(f"sesso::text IN ({placeholders})")

    if comune:
        comune_safe = [c for c in comune if re.match(r"^\d{6}$", c)]
        if comune_safe:
            placeholders = ",".join(f"'{c}'" for c in comune_safe)
            conditions_pop.append(f"codicecomune::text IN ({placeholders})")
            conditions_casi.append(f"codicecomune::text IN ({placeholders})")

    if fascia_eta:
        fascia_eta_safe = [f for f in fascia_eta if re.match(r"^\d{2}$", f)]
        if fascia_eta_safe:
            placeholders = ",".join(f"'{f}'" for f in fascia_eta_safe)
            conditions_pop.append(f"eta.eta19::text IN ({placeholders})")
            conditions_casi.append(f"eta.eta19::text IN ({placeholders})")

    if sede:
        sede_safe = [s.upper() for s in sede if re.match(r"^[A-Za-z0-9]{3}$", s)]
        if sede_safe:
            placeholders = ",".join(f"'{s}'" for s in sede_safe)
            conditions_casi.append(f"substring(sededellalesione,1,3) IN ({placeholders})")

    if followup:
        followup_safe = [f for f in followup if f in ("2", "3")]
        if followup_safe:
            placeholders = ",".join(f"'{f}'" for f in followup_safe)
            conditions_casi.append(f"statoinvita::text IN ({placeholders})")

    if base:
        base_safe = [b for b in base if b in ("0", "1", "2", "3", "4", "5", "6", "7", "8", "9")]
        if base_safe:
            placeholders = ",".join(f"'{b}'" for b in base_safe)
            conditions_casi.append(f"basedelladiagnosi::text IN ({placeholders})")
    
    if comportamento:
        comportamento_safe = [c for c in comportamento if c in ("0", "1", "2", "3")]
        if comportamento_safe:
            placeholders = ",".join(f"'{c}'" for c in comportamento_safe)
            conditions_casi.append(f"behaviour::text IN ({placeholders})")

    if grado:
        grado_safe = [g for g in grado if g in ("1", "2", "3", "4", "5", "6", "7", "8", "9")]
        if grado_safe:
            placeholders = ",".join(f"'{g}'" for g in grado_safe)
            conditions_casi.append(f"gradodellalesione::text IN ({placeholders})")

    if lateralita:
        lateralita_safe = [l for l in lateralita if l in ("1", "2", "3", "9")]
        if lateralita_safe:
            placeholders = ",".join(f"'{l}'" for l in lateralita_safe)
            conditions_casi.append(f"lateralita::text IN ({placeholders})")
    

    VALID_CASI = {"sede", "followup", "base", "comportamento", "grado", "lateralita"}
    VALID_POP = {"fascia_eta", "sesso", "anno", "comune"}


    where_sql_casi = ("WHERE " + " AND ".join(conditions_casi)) if conditions_casi else ""
    where_sql_pop = ("WHERE " + " AND ".join(conditions_pop)) if conditions_pop else "WHERE annodicalendario between 2014 and 2016"  # limitato per disponibilità dati

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
        {all_dims_sql_orderby}
        ;
    """.format(
                all_dims_sql=all_dims_sql,
                all_dims_sql_comma=all_dims_sql_comma,
                all_dims_sql_1comma=all_dims_sql_1comma,
                all_dims_sql_2comma=all_dims_sql_2comma,
                all_dims_sql_orderby=all_dims_sql_orderby,
                all_dims_select_sql=all_dims_select_sql,
                all_dims_sql_groupby=all_dims_sql_groupby,
                all_dims_pop_sql=all_dims_pop_sql,
                all_dims_pop_sql_1comma=all_dims_pop_sql_1comma,
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
   


@router.get("/std_eta")
def get_tassi_std_eta(
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
    """Recupera i tassi standardizzati per fascia di età."""
    logger.info("GET /api/tassi/std_eta with rows=%s cols=%s", rows, cols)
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
    all_dims_sql_1comma = ", " + all_dims_sql  if all_dims else ""
    all_dims_sql_2comma = ", " + all_dims_sql + ", " if all_dims else ""
    all_dims_sql_orderby = "order by " + all_dims_sql if all_dims else ""
    all_dims_select_sql = ", ".join(all_dims_select) + ", " if all_dims_select else ""
    all_dims_sql_groupby = "group by " + all_dims_sql if all_dims else ""
    all_dims_pop_sql = ", ".join(all_dims_pop) if all_dims_pop else ""
    all_dims_pop_sql_1comma = ", " + all_dims_pop_sql  if all_dims_pop else ""
    all_dims_pop_select_sql = ", ".join(all_dims_pop_select) + ", "  if all_dims_pop_select else ""
    all_dims_pop_sql_groupby = "group by " + all_dims_pop_sql if all_dims_pop else ""


    # ── Costruzione clausola WHERE con parametri psycopg2 (sicura da SQL injection) ──
    conditions_casi = []
    conditions_pop = []

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
            placeholders = ",".join(f"'{s}'" for s in sesso_safe)
            conditions_pop.append(f"sesso::text IN ({placeholders})")
            conditions_casi.append(f"sesso::text IN ({placeholders})")

    if comune:
        comune_safe = [c for c in comune if re.match(r"^\d{6}$", c)]
        if comune_safe:
            placeholders = ",".join(f"'{c}'" for c in comune_safe)
            conditions_pop.append(f"codicecomune::text IN ({placeholders})")
            conditions_casi.append(f"codicecomune::text IN ({placeholders})")

    if fascia_eta:
        fascia_eta_safe = [f for f in fascia_eta if re.match(r"^\d{2}$", f)]
        if fascia_eta_safe:
            placeholders = ",".join(f"'{f}'" for f in fascia_eta_safe)
            conditions_pop.append(f"eta.eta19::text IN ({placeholders})")
            conditions_casi.append(f"eta.eta19::text IN ({placeholders})")

    if sede:
        sede_safe = [s.upper() for s in sede if re.match(r"^[A-Za-z0-9]{3}$", s)]
        if sede_safe:
            placeholders = ",".join(f"'{s}'" for s in sede_safe)
            conditions_casi.append(f"substring(sededellalesione,1,3) IN ({placeholders})")

    if followup:
        followup_safe = [f for f in followup if f in ("2", "3")]
        if followup_safe:
            placeholders = ",".join(f"'{f}'" for f in followup_safe)
            conditions_casi.append(f"statoinvita::text IN ({placeholders})")

    if base:
        base_safe = [b for b in base if b in ("0", "1", "2", "3", "4", "5", "6", "7", "8", "9")]
        if base_safe:
            placeholders = ",".join(f"'{b}'" for b in base_safe)
            conditions_casi.append(f"basedelladiagnosi::text IN ({placeholders})")
    
    if comportamento:
        comportamento_safe = [c for c in comportamento if c in ("0", "1", "2", "3")]
        if comportamento_safe:
            placeholders = ",".join(f"'{c}'" for c in comportamento_safe)
            conditions_casi.append(f"behaviour::text IN ({placeholders})")

    if grado:
        grado_safe = [g for g in grado if g in ("1", "2", "3", "4", "5", "6", "7", "8", "9")]
        if grado_safe:
            placeholders = ",".join(f"'{g}'" for g in grado_safe)
            conditions_casi.append(f"gradodellalesione::text IN ({placeholders})")

    if lateralita:
        lateralita_safe = [l for l in lateralita if l in ("1", "2", "3", "9")]
        if lateralita_safe:
            placeholders = ",".join(f"'{l}'" for l in lateralita_safe)
            conditions_casi.append(f"lateralita::text IN ({placeholders})")
    

    VALID_CASI = {"sede", "followup", "base", "comportamento", "grado", "lateralita"}
    VALID_POP = {"fascia_eta", "sesso", "anno", "comune"}


    where_sql_casi = ("WHERE " + " AND ".join(conditions_casi)) if conditions_casi else ""
    where_sql_pop = ("WHERE " + " AND ".join(conditions_pop)) if conditions_pop else "WHERE annodicalendario between 2014 and 2016"  # limitato per disponibilità dati

    estrazione = """
        with
        casi as (
        select
        eta.eta19,
        {all_dims_select_sql}
        count(*) as conteggio
        FROM scheda_paziente
        natural inner join scheda_caso
        natural inner join eta
        {where_sql_casi}
        group by eta19
        {all_dims_sql_1comma}
        ), 
        popolazione as (
        select
        eta.eta19,
        {all_dims_pop_select_sql}
        sum(popolazione.popolazioneresidente) as popolazione
        from popolazione
        natural inner join eta
        {where_sql_pop}
        group by eta19
        {all_dims_pop_sql_1comma}
        )
        select
        {all_dims_sql_comma}
        eta19,
        c.conteggio,
        p.popolazione,
        (c.conteggio::float / p.popolazione)*100000 as tasso,
        popstd,
        (c.conteggio::float / p.popolazione)*popstd as tasso_spec
        from casi c
        natural left outer join popolazione p
        natural join popstd19
        where popstd19.standard='eu2013'
        order by {all_dims_sql_comma} eta19
        ;
    """.format(
                all_dims_sql=all_dims_sql,
                all_dims_sql_comma=all_dims_sql_comma,
                all_dims_sql_1comma=all_dims_sql_1comma,
                all_dims_sql_2comma=all_dims_sql_2comma,
                all_dims_sql_orderby=all_dims_sql_orderby,
                all_dims_select_sql=all_dims_select_sql,
                all_dims_sql_groupby=all_dims_sql_groupby,
                all_dims_pop_sql=all_dims_pop_sql,
                all_dims_pop_sql_1comma=all_dims_pop_sql_1comma,
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
                grouped = {}

                for row in results:
                    if all_dims:
                        key = tuple(row.get(dim) for dim in all_dims)
                    else:
                        key = ("__all__",)

                    if key not in grouped:
                        grouped[key] = {
                            "conteggio": 0,
                            "popolazione": 0,
                            "popstd": 0,
                            "tasso_std": 0,
                        }
                        for dim in all_dims:
                            grouped[key][dim] = row.get(dim)

                    grouped[key]["conteggio"] += row.get("conteggio") or 0
                    grouped[key]["popolazione"] += row.get("popolazione") or 0
                    grouped[key]["popstd"] += row.get("popstd") or 0
                    grouped[key]["tasso_std"] += row.get("tasso_spec") or 0

                def _build_output(group):
                    output = {dim: group.get(dim) for dim in all_dims}
                    conteggio = group["conteggio"]
                    popolazione = group["popolazione"]
                    output["conteggio"] = conteggio
                    output["popolazione"] = popolazione
                    output["tasso"] = (conteggio / popolazione) * 100000 if popolazione else None
                    output["tasso_std"] = group["tasso_std"] / group["popstd"] * 100000 if group["popstd"] else None
                    return output

                data = list(map(_build_output, grouped.values()))
                if all_dims:
                    data.sort(key=lambda item: tuple(str(item.get(dim, "")) for dim in all_dims))

                logger.info(f"Retrieved {len(data)} tassi records")
                return {"count": len(data), "data": data}
    except Exception as exc:
        logger.error(f"Error fetching tassi: {exc}")
        raise HTTPException(status_code=500, detail=str(exc))
   


