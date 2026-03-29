# weather-and-emissions-demo

## Setup

1. Copy `.env.example` to `.env` and fill in your credentials (or create `.env` with the variables listed in `fetch.py`).

2. Install dependencies and run with [uv](https://docs.astral.sh/uv/):

```bash
uv run fetch.py
```

`uv` will automatically create a virtual environment and install dependencies from `pyproject.toml` on first run.

To install dependencies without running:

```bash
uv sync
```

To run tests use

```bash
uv run pytest tests/ -v
```
