"""Shared pytest fixtures for DataPulse-Ingest-Engine test suite."""
import pytest
import pandas as pd
from pipeline_ingest import DataPulseIngestor, REQUIRED_COLUMNS


@pytest.fixture()
def engine() -> DataPulseIngestor:
    """Return ingestor with small chunk size for unit tests."""
    return DataPulseIngestor(chunk_size=1_000)


@pytest.fixture()
def clean_chunk() -> pd.DataFrame:
    """Return a minimal valid tick chunk with no anomalies."""
    return pd.DataFrame({
        "timestamp": pd.date_range("2026-01-01", periods=5, freq="1s"),
        "symbol": ["EURUSD"] * 5,
        "bid": [1.0850, 1.0851, 1.0852, 1.0851, 1.0850],
        "ask": [1.0852, 1.0853, 1.0854, 1.0853, 1.0852],
        "volume": [100, 200, 150, 300, 250],
    })


@pytest.fixture()
def chunk_with_spike(clean_chunk) -> pd.DataFrame:
    """Inject a 10% price spike into row 2."""
    df = clean_chunk.copy()
    df.loc[2, "bid"] = 1.1935   # > 5% spike from 1.0851
    df.loc[2, "ask"] = 1.1937
    return df