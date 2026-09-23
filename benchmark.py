"""
Benchmark: ingestion throughput across chunk sizes.
Run with: python benchmark.py
"""
import time
import pandas as pd
from pipeline_ingest import DataPulseIngestor


def make_chunks(total_rows: int, chunk_size: int):
    for _ in range(total_rows // chunk_size):
        yield pd.DataFrame({
            "timestamp": pd.date_range("2026-01-01", periods=chunk_size, freq="1ms"),
            "symbol": ["EURUSD"] * chunk_size,
            "bid": [1.0850] * chunk_size,
            "ask": [1.0852] * chunk_size,
            "volume": [100] * chunk_size,
        })


if __name__ == "__main__":
    for chunk_size in (10_000, 50_000, 250_000):
        engine = DataPulseIngestor(chunk_size=chunk_size)
        chunks = make_chunks(total_rows=500_000, chunk_size=chunk_size)
        t0 = time.perf_counter()
        result = engine.run(chunks)
        elapsed = time.perf_counter() - t0
        throughput = result.total_rows_out / elapsed
        print(f"chunk={chunk_size:>7,} | rows_out={result.total_rows_out:>7,} | "
              f"{elapsed:.2f}s | {throughput:,.0f} rows/s")