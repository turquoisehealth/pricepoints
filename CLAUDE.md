# Price Points Research Monorepo

## What This Is

Price Points creates quantitative, public research on healthcare pricing using Turquoise Health's healthcare price transparency data. This research helps illuminate actual healthcare costs, pricing variation, and market dynamics (e.g., 340B drug pricing, delivery costs, regional disparities).

**Monorepo structure:**

- [**projects/**](projects/) - Time-stamped research projects (e.g., `2025_04_delivery_costs`) following the template at [0000_00_proj_template/](projects/0000_00_proj_template/)
- [**analyses/**](analyses/) - One-off exploratory analyses
- [**tq/**](tq/) - Shared Python package for database access and utilities

**Standard project layout:**

```bash
projects/YYYY_MM_project_name/
├── pyproject.toml         # Dependencies
├── analysis.qmd           # Main Quarto notebook
├── ingest.py              # Data ingestion
├── queries/               # SQL files
└── data/                  # Input/output data
```

## Tech Stack

- **Python** - Version in `.python-version` (managed with `uv`, never pip/poetry)
- **Data** - Polars ONLY (never pandas or numpy)
- **Database** - Trino via `tq` package connectors
- **Analysis** - Quarto (`.qmd`) for R-based visualization
- **SQL** - Trino dialect (uppercase keywords, explicit aliases)

## Critical Rules

1. **Polars only** - Do not import or suggest pandas/numpy for data manipulation
2. **Use `uv`** - For all dependency management (`uv sync`, `uv add`, etc.)
3. **Minimal file creation** - Only create files when absolutely necessary
4. **No documentation** - Never proactively create README or .md files
5. **Use Claude plugins** - When working with Python, invoke the relevant /astral:<skill> for uv, ty, and ruff to ensure best practices are followed
6. **Pre-commit handles linting** - Don't worry about code style, it's automated

## Development Workflow

1. Navigate to project directory
2. Run `uv sync` to install dependencies
3. Write SQL queries in `queries/` folder
4. Implement data ingestion in `ingest.py` (uses `tq` package for Trino)
5. Analyze in `analysis.qmd` (Quarto notebook)
6. Let pre-commit hooks handle formatting on commit

## Common Patterns

1. **Database access:**

    ```python
    from tq.db import get_trino_connection
    conn = get_trino_connection()
    ```

2. **Starting a new project:** Copy [0000_00_proj_template/](projects/0000_00_proj_template/) to `projects/YYYY_MM_name/`
3. **Testing:** Run `pytest` in [tq/](tq/) directory when modifying shared utilities
