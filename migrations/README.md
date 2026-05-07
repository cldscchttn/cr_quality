# Database Migrations

This directory contains SQL migration files for the PostgreSQL database used by
the FastAPI application. Migrations are plain `.sql` files numbered sequentially
(`001_`, `002_`, …) and must be applied in order.

---

## Files

| File | Description |
|------|-------------|
| `001_add_indexes.sql` | Adds indexes on join and filter columns for `scheda_paziente`, `scheda_caso`, and `eta` to improve query performance in the `/api/tassi` endpoints. |

---

## How to run a migration

### Option 1 — psql (recommended)

Connect to the Railway Postgres instance and pipe the SQL file directly:

```bash
psql "$DATABASE_URL" -f migrations/001_add_indexes.sql
```

Replace `$DATABASE_URL` with the full connection string from the Railway
dashboard (Settings → Variables → `DATABASE_URL`), or export it first:

```bash
export DATABASE_URL="postgresql://user:password@host:port/dbname"
psql "$DATABASE_URL" -f migrations/001_add_indexes.sql
```

### Option 2 — Railway CLI

If you have the [Railway CLI](https://docs.railway.app/develop/cli) installed
and are logged in to the correct project:

```bash
# Open an interactive psql session via the Railway proxy
railway connect postgres

# Then inside psql, run:
\i migrations/001_add_indexes.sql
```

### Option 3 — Copy-paste in a GUI client

Open the migration file, copy its contents, and execute them in any PostgreSQL
GUI (TablePlus, DBeaver, pgAdmin, etc.) connected to the Railway database.

---

## Notes

- Every `CREATE INDEX` statement uses `IF NOT EXISTS`, so migrations are
  **idempotent** — safe to run more than once without causing errors.
- Index creation on large tables can take a few seconds to minutes. The
  statements do **not** use `CONCURRENTLY`, so they will briefly lock the
  affected table for writes. Run during a low-traffic window if the tables are
  large.
- If you need non-blocking index creation on a live production database, prefix
  each statement with `CREATE INDEX CONCURRENTLY IF NOT EXISTS …` and run the
  file outside a transaction block (i.e., do **not** wrap in `BEGIN`/`COMMIT`).
