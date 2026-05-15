# Data Source Loaders

Pluggable data source loaders. Recipes can load from local files (default) or SQL databases.

## File Loader (default)

```yaml
sources:
  vendor_export:
    file: vendor_data.csv
    type: multi_population
```

Supported formats: CSV, TSV, Parquet. Optional `columns` field selects a subset at load time.

## SQL Loader

Load directly from a database. Drivers are lazy-imported -- no errors unless a recipe actually uses one.

### Trino

```yaml
sources:
  warehouse:
    loader: sql
    driver: trino
    connection:
      host: ${TRINO_HOST}
      port: 8080
      user: ${TRINO_USER}
      catalog: hive
      schema: analytics
    query: |
      SELECT vendor_id, vendor_name, address_line1
      FROM vendor_master WHERE status = 'ACTIVE'
    type: multi_population
```

Requires: `pip install trino`

Optional fields: `password`, `http_scheme` (https), `verify` (SSL cert verification).

If `user` or `password` are in the config but unresolved (env var not set), the loader prompts interactively. Password uses hidden input.

### PostgreSQL

```yaml
sources:
  warehouse:
    loader: sql
    driver: postgresql
    connection:
      host: ${DB_HOST}
      port: 5432
      database: analytics
      user: ${DB_USER}
      password: ${DB_PASSWORD}
    query: SELECT vendor_id, vendor_name FROM vendor_master
    type: multi_population
```

Requires: `pip install psycopg2-binary`

### SQLite

```yaml
sources:
  local_db:
    loader: sql
    driver: sqlite
    connection:
      database: local_cache.db
    query: SELECT id, name FROM reference_data
```

No extra dependencies (stdlib).

### Drivers

| Driver | Package | Notes |
|--------|---------|-------|
| sqlite | (stdlib) | Local testing, cached datasets |
| postgresql | psycopg2-binary | Production databases |
| trino | trino | Data warehouses |

## Caching

SQL results are cached as Parquet files to avoid hitting the database on every run. Enabled by default (24h TTL).

```yaml
sources:
  warehouse:
    loader: sql
    driver: trino
    cache_ttl: "24h"     # default
    # cache_ttl: "12h"   # half a day
    # cache_ttl: "7d"    # a week
    # cache_ttl: "off"   # disable
```

Formats: `Nh` (hours), `Nm` (minutes), `Nd` (days), `N` (seconds), `off`.

Cache files stored in `data/.cache/` (git-ignored). Delete the directory to force a fresh fetch. Filenames include recipe name, source name, date and a hash:

```
tpch_parts_catalog_reconciliation_migrated_parts_20260514_a1b2c3d4.parquet
```

## Environment Variables

Connection config supports `${VAR_NAME}` interpolation:

```yaml
connection:
  host: ${PGHOST}
  password: ${PGPASSWORD}
```

Unresolved vars stay as-is, which will cause a connection error -- making missing env vars obvious.

## Behavior

- All values cast to String (consistent with CSV loading)
- SQL NULL becomes Polars null (works with is_not_null/is_null filters)
- Existing recipes without `loader` key work unchanged (backward compatible)
- Password excluded from cache key (never appears in filenames)

## Adding New Drivers

1. Add `_load_<driver>()` in `src/loaders.py`
2. Register in the driver dispatch within `load_sql()`
3. Add tests (use SQLite patterns as template)
4. Update `config/recipe_schema.json`
