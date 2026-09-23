# Changelog

## [1.1.0] — 2026-09-23
### Added
- Shared `conftest.py` with engine, clean_chunk, and spike fixtures.
- `pytest.ini` configuration.
- Fixed backslash tilde syntax error (`\~` → `~`) in spike filter.
- Fixed markdown code-fence syntax error in `pipeline_ingest.py`.
- Added `pandas>=2.0.0` and `numpy>=1.26.0` to `requirements.txt`.
- MIT license applied.

## [1.0.0] — 2026-09-20
### Added
- Chunked CSV ingestion without RAM exhaustion using `pd.read_csv(chunksize=...)`.
- Deterministic price-spike anomaly filter (5% relative deviation threshold).
- Schema validation on every chunk — missing columns raise immediately.
- Generator-based API: accepts file paths or in-memory DataFrame iterables.