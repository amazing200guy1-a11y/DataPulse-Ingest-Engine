# Contributing to DataPulse-Ingest-Engine

## Development Setup
```bash
git clone https://github.com/amazing200guy1-a11y/DataPulse-Ingest-Engine
cd DataPulse-Ingest-Engine
python -m venv .venv
source .venv/bin/activate  # or .venv\Scripts\activate on Windows
pip install -r requirements.txt
```

## Running Tests
```bash
pytest --tb=short -v
```

## Architecture Notes
- `pipeline_ingest.py` — main ingestion pipeline entry point
- Ingestion is designed to be idempotent (safe to re-run)
- All data normalization passes through `normalize_tick()` before storage
