---
name: trino
description: >
  Run exploratory SQL queries against the Trino database. Use when the user
  asks to query, explore, count, check, look up, investigate, or analyze data
  in the database, or mentions tables, rates, payers, providers, billing codes,
  or claims.
allowed-tools: Bash(trino *), Read, Write, Glob
---

# Trino Query Skill

## Critical Rules

- **Read-only**, NEVER run DDL/DML (`INSERT`, `UPDATE`, `DELETE`, `DROP`, `CREATE`, `ALTER`)
- **Always LIMIT** exploratory queries (start with `LIMIT 100`)
- **Always use `--file`**, never `--execute`, for multi-line queries
- **Use Trino SQL dialect**: uppercase keywords, double-quoted identifiers, ANSI syntax

## Workflow

1. Write SQL to a file, then execute:

    ```bash
    trino --file /tmp/query_name.sql
    ```

2. Inspect results, refine query, re-run
3. Run independent queries in parallel using background Bash tasks

## Performance

- Avoid `SELECT COUNT(*) FROM table` on large tables - very slow without filters
- Avoid `SELECT *` - select only the columns you need
- Always apply `WHERE` filters to narrow scans before aggregating
- Use `LIMIT` for exploratory queries
- When filtering on partitioned columns (e.g. `year`, `month`), include them in `WHERE` to enable partition pruning

## Table Discovery

Use these commands to explore the database:

```sql
SHOW SCHEMAS FROM tq_production;
SHOW TABLES FROM tq_production.<schema>;
DESCRIBE tq_production.<schema>.<table>;
SHOW COLUMNS FROM tq_production.<schema>.<table>;
```

## Error Handling

| Error | Fix |
|---|---|
| `COLUMN_NOT_FOUND` | Run `SHOW COLUMNS FROM <table>` to check exact column names |
| `TABLE_NOT_FOUND` | Run `SHOW TABLES FROM <schema>` to verify table exists |
| Type mismatch (e.g. `varchar` vs `integer`) | Use `CAST(col AS type)` to align types |
| Query timeout | Add `WHERE` filters, reduce scope, or add `LIMIT` |
| `SCHEMA_NOT_FOUND` for payer data | Check the year/month — use `SHOW SCHEMAS FROM tq_production` to find available months |

## Available Tables

List of most important tables/schemas, not exhaustive. Only reference if necessary:

- **Clear Rates** (cleaned, validated price data): See [references/clear-rates.md](references/clear-rates.md)
- **Hospital Price Transparency** (hospital MRF price data): See [references/hospital.md](references/hospital.md)
- **Payer Data** (payer Transparency in Coverage price data): See [references/payer.md](references/payer.md)
- **Spines** (reference data): See [references/spines.md](references/spines.md)
- **Claims Benchmarks** (anonymized utilization, gross price data): See [references/claims.md](references/claims.md)
