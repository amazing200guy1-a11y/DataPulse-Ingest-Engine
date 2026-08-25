---

### 2. `pipeline_ingest.py`

```python
"""
DataPulse-Ingest-Engine — High-throughput chunked ingestion pipeline.

Processes multi-hundred-gigabyte tick streams without exhausting RAM
by iterating in controlled blocks, sanitising anomalies early, and
yielding clean structured batches for downstream vectorisation / Redis.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Generator, Iterable, Iterator, List, Optional, Sequence

import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
)
logger = logging.getLogger("datapulse.ingest")

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

DEFAULT_CHUNK_SIZE: int = 250_000          # rows per block
MAX_PRICE_SPIKE_RATIO: float = 0.05        # 5 % jump vs previous tick → anomaly
REQUIRED_COLUMNS: Sequence[str] = ("timestamp", "symbol", "bid", "ask", "volume")


@dataclass(frozen=True, slots=True)
class ChunkStats:
    """Lightweight metrics for a single processed chunk."""

    rows_in: int
    rows_out: int
    anomalies_dropped: int
    elapsed_seconds: float


@dataclass(frozen=True, slots=True)
class PipelineResult:
    """Aggregate result after a full run."""

    total_rows_in: int
    total_rows_out: int
    total_anomalies: int
    total_seconds: float
    chunk_count: int


# ---------------------------------------------------------------------------
# Sanitisation
# ---------------------------------------------------------------------------

def sanitise_chunk(df: pd.DataFrame) -> tuple[pd.DataFrame, int]:
    """
    Remove rows that violate schema or exhibit extreme price spikes.

    Returns
    -------
    cleaned : pd.DataFrame
    dropped : int
        Number of rows removed.
    """
    original_len = len(df)
    if original_len == 0:
        return df, 0

    # 1. Schema enforcement
    missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    mask = pd.Series(True, index=df.index)

    # Drop nulls in critical fields
    mask &= df["timestamp"].notna()
    mask &= df["bid"].notna()
    mask &= df["ask"].notna()
    mask &= df["bid"] > 0
    mask &= df["ask"] > 0
    mask &= df["ask"] >= df["bid"]

    # 2. Price-spike filter (relative to previous valid tick inside the chunk)
    mid = (df["bid"] + df["ask"]) / 2.0
    prev_mid = mid.shift(1)
    rel_change = (mid - prev_mid).abs() / prev_mid.replace(0, pd.NA)
    spike_mask = rel_change > MAX_PRICE_SPIKE_RATIO
    # Keep the first row of the chunk (no previous) and drop clear spikes
    spike_mask = spike_mask.fillna(False)
    mask &= \~spike_mask

    cleaned = df.loc[mask].copy()
    dropped = original_len - len(cleaned)
    return cleaned, dropped


# ---------------------------------------------------------------------------
# Core engine
# ---------------------------------------------------------------------------

class DataPulseIngestor:
    """
    Memory-safe chunked ingestion engine.

    Accepts either a file path (CSV) or an in-memory iterable of DataFrames
    (useful for tests that simulate multi-GB streams via generators).
    """

    def __init__(self, chunk_size: int = DEFAULT_CHUNK_SIZE) -> None:
        if chunk_size < 1_000:
            raise ValueError("chunk_size must be >= 1000")
        self.chunk_size = chunk_size

    def _iter_file_chunks(self, path: Path) -> Iterator[pd.DataFrame]:
        """Yield DataFrame chunks from a CSV without loading the whole file."""
        logger.info("Streaming file %s with chunksize=%d", path, self.chunk_size)
        reader = pd.read_csv(
            path,
            chunksize=self.chunk_size,
            parse_dates=["timestamp"],
            dtype={
                "symbol": "category",
                "bid": "float64",
                "ask": "float64",
                "volume": "float64",
            },
        )
        yield from reader

    def process_chunks(
        self,
        source: Path | Iterable[pd.DataFrame],
    ) -> Generator[tuple[pd.DataFrame, ChunkStats], None, None]:
        """
        Main generator pipeline.

        Yields
        ------
        (cleaned_chunk, stats)
        """
        if isinstance(source, Path):
            chunk_iter: Iterator[pd.DataFrame] = self._iter_file_chunks(source)
        else:
            chunk_iter = iter(source)

        for raw in chunk_iter:
            t0 = time.perf_counter()
            cleaned, dropped = sanitise_chunk(raw)
            elapsed = time.perf_counter() - t0

            stats = ChunkStats(
                rows_in=len(raw),
                rows_out=len(cleaned),
                anomalies_dropped=dropped,
                elapsed_seconds=round(elapsed, 6),
            )
            logger.info(
                "chunk processed | in=%d out=%d dropped=%d time=%.4fs",
                stats.rows_in,
                stats.rows_out,
                stats.anomalies_dropped,
                stats.elapsed_seconds,
            )
            yield cleaned, stats

    def run(self, source: Path | Iterable[pd.DataFrame]) -> PipelineResult:
        """
        Consume the entire generator and return aggregate metrics.
        """
        total_in = 0
        total_out = 0
        total_anom = 0
        chunk_count = 0
        t0 = time.perf_counter()

        for cleaned, stats in self.process_chunks(source):
            total_in += stats.rows_in
            total_out += stats.rows_out
            total_anom += stats.anomalies_dropped
            chunk_count += 1
            # In production the cleaned frame would be vectorised
            # and published to Redis here. We deliberately do not
            # accumulate frames in memory.

        total_seconds = time.perf_counter() - t0
        result = PipelineResult(
            total_rows_in=total_in,
            total_rows_out=total_out,
            total_anomalies=total_anom,
            total_seconds=round(total_seconds, 4),
            chunk_count=chunk_count,
        )
        logger.info("pipeline finished | %s", result)
        return result


# ---------------------------------------------------------------------------
# Demo entry-point
# ---------------------------------------------------------------------------

def _demo_generator(num_chunks: int = 5, rows_per_chunk: int = 10_000) -> Iterator[pd.DataFrame]:
    """Small synthetic stream for local demonstration."""
    import numpy as np

    rng = np.random.default_rng(42)
    base_price = 1.0850
    for i in range(num_chunks):
        n = rows_per_chunk
        ts = pd.date_range("2026-01-01", periods=n, freq="ms") + pd.Timedelta(milliseconds=i * n)
        noise = rng.normal(0, 0.00005, size=n)
        mid = base_price + noise.cumsum() * 0.00001
        bid = mid - 0.00005
        ask = mid + 0.00005
        # Inject a few deliberate anomalies
        if i == 2:
            ask[100] = ask[100] * 1.20          # spike
            bid[200] = float("nan")             # missing
        yield pd.DataFrame(
            {
                "timestamp": ts,
                "symbol": "EURUSD",
                "bid": bid,
                "ask": ask,
                "volume": rng.uniform(0.1, 5.0, size=n),
            }
        )


def main() -> None:
    engine = DataPulseIngestor(chunk_size=10_000)
    result = engine.run(_demo_generator())
    print(result)


if __name__ == "__main__":
    main()
