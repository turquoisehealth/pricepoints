# Price Points monorepo

This is a healthcare price transparency research monorepo containing
the code and data for the Price Points research publication. Price Points
creates novel, quantitative, public research using Turquoise Health price
data and other healthcare datasets.

## Repository structure

- **`projects/`** - Time-stamped research projects based on the template
  at `0000_00_proj_template/` (e.g., `2025_04_delivery_costs`, `2025_06_il_340b`)
- **`analyses/`** - Exploratory and one-off research code
- **`tq/`** - Custom Python package with database connectors and utilities

Each project follows a standardized structure:

- `pyproject.toml` - Python dependencies and configuration
- `analysis.qmd` - Main Quarto analysis notebook
- `ingest.py` - Data ingestion scripts
- `queries/` - SQL queries for data extraction
- `data/` - Input and output data files

## Development guidelines

### Package management

- **Always use `uv`** - Never use `pip`, `poetry`, or similar tools
- Each project has its own `pyproject.toml` with dependencies
- The `tq/` helper package is referenced as a local dependency

### Code quality

- **Code style**: Follow PEP 8 standards enforced by ruff
- **Pre-commit hooks**: ruff runs automatically on every commit
- **Type checking**: pyright configuration inherited from root `pyproject.toml`
- **SQL linting**: sqlfluff for Trino dialect SQL queries

### Technology stack

- **Python**: Version specified in a `.python-version` file
- **Core libraries**: _Only_ use polars for data manipulation. Do not use
  pandas or numpy.
- **Databases**: Trino connections via helpers in the `tq` package
- **Analysis**: Quarto documents (`.qmd`) for creating plots with R
- **SQL**: Trino dialect with uppercase keywords and explicit aliasing

### Testing

- **TQ Package**: Run `pytest` in the `tq/` directory for unit tests
  when necessary
- **Projects**: No specific testing framework required beyond validation
  in notebooks

### Development workflow

1. Install dependencies with `uv sync`
2. Use pre-commit hooks for automatic linting
3. Write SQL queries in `queries/` folder
4. Implement data ingestion in `ingest.py`
5. Conduct analysis in Quarto notebooks
6. Organize output data in `data/output/`

## Important instructions

- Do what has been asked; nothing more, nothing less
- NEVER create files unless absolutely necessary for achieving your goal
- ALWAYS prefer editing an existing file to creating a new one
- NEVER proactively create documentation files (*.md) or
  README files unless explicitly requested
