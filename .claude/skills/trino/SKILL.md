---
name: trino
description: >
  Run exploratory SQL queries against the Trino database. Use when the user
  asks to query, explore, count, check, look up, investigate, or analyze data
  in the database, or mentions tables, rates, payers, providers, billing codes,
  or claims.
allowed-tools: Bash(trino *), Read, Write, Glob
---

# Trino query skill

## Running queries

Write SQL to a `.sql` file (end statements with `;`), then execute:

```bash
trino --file /tmp/<filename>.sql
```

Always use `--file`, never `--execute`, for multi-line queries.

## Performance

- Avoid `SELECT COUNT(*) FROM table` - very slow on large tables
- Always use `LIMIT` for exploratory queries
- Run independent queries in parallel using background Bash tasks

## Available tables

List of most important tables/schemas, not exhaustive. Only reference if necessary:

**Clear Rates** (cleaned, validated price data): See [reference/clear-rates.md](reference/clear-rates.md)
**Hospital Price Transparency** (hospital machine-readable file price data): See [reference/hospital.md](reference/hospital.md)
**Payer Data** (payer Transparency in Coverage price data): See [reference/payer.md](reference/payer.md)
**Spines** (reference data): See [reference/spines.md](reference/spines.md)
**Claims Benchmarks** (anonymized utilization, gross price data): See [reference/claims.md](reference/claims.md)
