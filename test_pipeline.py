"""
Automated tests for DataPulse-Ingest-Engine.

Simulates multi-gigabyte behaviour via generators so tests stay fast
while still exercising chunked processing and anomaly detection.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from pipeline_ingest import (
    ChunkStats,
    DataPulseIngestor,
    PipelineResult,
    sanitise_chunk,
)


# ---------------------------------------------------------------------------
# Helpers — simulate large streams without writing GB files
# ---------------------------------------------------------------------------

def make_clean_chunk(rows: int = 5_000, seed: int = 0) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    ts = pd.date_range("2026-01-01", periods=rows, freq="ms")
    mid = 1.0850 + rng.normal(0, 0.00002, size=rows).cumsum() * 0.00001
    return pd.DataFrame(
        {
            "timestamp": ts,
            "symbol": "EURUSD",
            "bid": mid - 0.00004,
            "ask": mid + 0.00004,
            "volume": rng.uniform(0.5, 3.0, size=rows),
        }
    )


def make_anomalous_chunk(rows: int = 5_000) -> pd.DataFrame:
    df = make_clean_chunk(rows, seed=99)
    # Inject clear anomalies
    df.loc[10, "ask"] = df.loc[10, "ask"] * 1.25          # price spike
    df.loc[20, "bid"] = float("nan")                      # missing bid
    df.loc[30, "ask"] = df.loc[30, "bid"] - 0.0001        # crossed market
    return df


def large_stream_generator(
    total_rows: int = 2_000_000,
    chunk_size: int = 100_000,
) -> list[pd.DataFrame]:
    """
    Materialise a list of chunks that together represent a multi-million-row
    (conceptually multi-GB) stream. Tests stay fast because we never
    write the data to disk and process one chunk at a time.
    """
    chunks = []
    produced = 0
    seed = 0
    while produced < total_rows:
        n = min(chunk_size, total_rows - produced)
        chunks.append(make_clean_chunk(n, seed=seed))
        produced += n
        seed += 1
    # Inject one dirty chunk in the middle
    if len(chunks) > 2:
        chunks[len(chunks) // 2] = make_anomalous_chunk(chunk_size)
    return chunks


# ---------------------------------------------------------------------------
# Unit tests — sanitisation
# ---------------------------------------------------------------------------

def test_sanitise_drops_spike_and_nulls() -> None:
    raw = make_anomalous_chunk(1_000)
    cleaned, dropped = sanitise_chunk(raw)
    assert dropped >= 3
    assert len(cleaned) == len(raw) - dropped
    assert cleaned["bid"].notna().all()
    assert (cleaned["ask"] >= cleaned["bid"]).all()


def test_sanitise_preserves_clean_data() -> None:
    raw = make_clean_chunk(2_000)
    cleaned, dropped = sanitise_chunk(raw)
    assert dropped == 0
    assert len(cleaned) == len(raw)


# ---------------------------------------------------------------------------
# Integration-style tests — chunked engine
# ---------------------------------------------------------------------------

def test_engine_processes_multiple_chunks() -> None:
    chunks = [make_clean_chunk(3_000, seed=i) for i in range(4)]
    engine = DataPulseIngestor(chunk_size=3_000)
    result = engine.run(chunks)

    assert isinstance(result, PipelineResult)
    assert result.chunk_count == 4
    assert result.total_rows_in == 12_000
    assert result.total_rows_out == 12_000
    assert result.total_anomalies == 0
    assert result.total_seconds >= 0.0


def test_engine_detects_anomalies_in_stream() -> None:
    chunks = [
        make_clean_chunk(2_000, seed=1),
        make_anomalous_chunk(2_000),
        make_clean_chunk(2_000, seed=3),
    ]
    engine = DataPulseIngestor(chunk_size=2_000)
    result = engine.run(chunks)

    assert result.chunk_count == 3
    assert result.total_anomalies >= 3
    assert result.total_rows_out < result.total_rows_in


def test_large_simulated_stream_stays_stable() -> None:
    """
    Simulate \~2 M rows (conceptually multi-GB when stored as tick data).
    Verifies that the generator-based pipeline completes and reports
    sensible aggregate metrics without OOM.
    """
    chunks = large_stream_generator(total_rows=2_000_000, chunk_size=100_000)
    engine = DataPulseIngestor(chunk_size=100_000)
    result = engine.run(chunks)

    assert result.chunk_count == 20
    assert result.total_rows_in == 2_000_000
    assert result.total_rows_out < result.total_rows_in          # at least the injected anomalies
    assert result.total_anomalies > 0
    assert result.total_seconds > 0.0


def test_chunk_stats_are_emitted() -> None:
    chunks = [make_clean_chunk(1_500)]
    engine = DataPulseIngestor(chunk_size=1_500)
    outputs = list(engine.process_chunks(chunks))
    assert len(outputs) == 1
    cleaned, stats = outputs[0]
    assert isinstance(stats, ChunkStats)
    assert stats.rows_in == 1_500
    assert stats.rows_out == 1_500
    assert stats.anomalies_dropped == 0
