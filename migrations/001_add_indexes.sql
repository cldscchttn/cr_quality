-- Migration 001: Add indexes to scheda_paziente, scheda_caso, and eta tables
--
-- These indexes target the join columns and WHERE-clause filter columns used
-- by the /api/tassi endpoints, which perform multi-table joins and multi-
-- dimensional filtering on potentially large datasets.
--
-- All indexes are created with IF NOT EXISTS so the script is safe to re-run.

-- ─── Join columns ────────────────────────────────────────────────────────────

-- scheda_paziente ↔ scheda_caso join key
CREATE INDEX IF NOT EXISTS idx_scheda_paziente_codiceanonimo
    ON scheda_paziente (codiceanonimodelpazientespecifico);

-- scheda_caso ↔ scheda_paziente join key
CREATE INDEX IF NOT EXISTS idx_scheda_caso_codiceanonimo
    ON scheda_caso (codiceanonimodelpazientespecifico);

-- scheda_caso ↔ eta join key
CREATE INDEX IF NOT EXISTS idx_scheda_caso_annodieta
    ON scheda_caso (annodieta);

-- eta ↔ scheda_caso join key
CREATE INDEX IF NOT EXISTS idx_eta_annodieta
    ON eta (annodieta);

-- ─── Filter columns: scheda_caso ─────────────────────────────────────────────

-- WHERE basedelladiagnosi IN (...)
CREATE INDEX IF NOT EXISTS idx_scheda_caso_basedelladiagnosi
    ON scheda_caso (basedelladiagnosi);

-- WHERE behaviour IN (...)
CREATE INDEX IF NOT EXISTS idx_scheda_caso_behaviour
    ON scheda_caso (behaviour);

-- WHERE gradodellalesione IN (...)
CREATE INDEX IF NOT EXISTS idx_scheda_caso_gradodellalesione
    ON scheda_caso (gradodellalesione);

-- WHERE lateralita IN (...)
CREATE INDEX IF NOT EXISTS idx_scheda_caso_lateralita
    ON scheda_caso (lateralita);

-- WHERE substring(sededellalesione,1,3) IN (...)
-- A plain index on the full column lets Postgres evaluate the expression
-- efficiently; alternatively use the functional index below for exact matches
-- on the 3-character prefix.
CREATE INDEX IF NOT EXISTS idx_scheda_caso_sededellalesione
    ON scheda_caso (sededellalesione);

CREATE INDEX IF NOT EXISTS idx_scheda_caso_sededellalesione_prefix
    ON scheda_caso (substring(sededellalesione, 1, 3));

-- WHERE codicecomune IN (...)
CREATE INDEX IF NOT EXISTS idx_scheda_caso_codicecomune
    ON scheda_caso (codicecomune);

-- WHERE annodicalendario IN (...)
CREATE INDEX IF NOT EXISTS idx_scheda_caso_annodicalendario
    ON scheda_caso (annodicalendario);

-- ─── Filter columns: scheda_paziente ─────────────────────────────────────────

-- WHERE sesso IN (...)
CREATE INDEX IF NOT EXISTS idx_scheda_paziente_sesso
    ON scheda_paziente (sesso);

-- WHERE statoinvita IN (...)
CREATE INDEX IF NOT EXISTS idx_scheda_paziente_statoinvita
    ON scheda_paziente (statoinvita);
