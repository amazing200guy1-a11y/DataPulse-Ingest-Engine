# DataPulse-Ingest-Engine: High-Throughput Big Data Pipeline for Quantitative AI Models

![Python](https://img.shields.io/badge/Python-3.12-3776AB?style=for-the-badge&logo=python&logoColor=white)
![Pandas](https://img.shields.io/badge/Pandas-2.x-150458?style=for-the-badge&logo=pandas&logoColor=white)
![Polars](https://img.shields.io/badge/Polars-Ready-CD7935?style=for-the-badge)
![Redis](https://img.shields.io/badge/Redis-Pub%2FSub-DC382D?style=for-the-badge&logo=redis&logoColor=white)
![Pytest](https://img.shields.io/badge/Pytest-Async-0A9EDC?style=for-the-badge&logo=pytest&logoColor=white)

**High-throughput ingestion and sanitisation layer** designed for quantitative AI systems that must process multi-hundred-gigabyte tick streams without exhausting RAM.

The engine streams raw market data in controlled blocks, applies deterministic anomaly filters, materialises clean vector-ready arrays, and hands off via Redis Pub/Sub to downstream C++/Rust execution kernels.

> Live venue credentials, proprietary anomaly models and production Redis topology remain private.  
> This repository is an architectural showcase of memory-safe big-data patterns for Lead / Staff ML Infrastructure roles.

---

## System Pipeline
[ RAW UNCOMPRESSED MARKET TICK DATA (200GB+ STREAMS) ]
                              │
                              ▼
   ┌──────────────────────────────────────────────────────┐
   │         HIGH-PERFORMANCE DATA STREAM INGESTION       │
   │   Python 3.12 / Chunked Processing (Pandas/Polars)   │
   │   - Prevents RAM exhaustion by loading data in blocks│
   └──────────────────────────┬───────────────────────────┘
                              │
                              ▼
   ┌──────────────────────────────────────────────────────┐
   │         THREATJAIL DATA SANITIZATION LAYER            │
   │   Filters out broker anomalies & corrupted blocks    │
   └──────────────────────────┬───────────────────────────┘
                              │
                              ▼
   ┌──────────────────────────────────────────────────────┐
   │         VECTOR EMBEDDING & MATRIX PROCESSING         │
   │   Serializes tick streams into structured arrays     │
   └──────────────────────────┬───────────────────────────┘
                              │
                              ▼
   ┌──────────────────────────────────────────────────────┐
   │           REDIS PUB/SUB SYSTEM BROADCAST              │
   │   Streams clean, multi-gigabyte data blocks down     │
   │   to local C++/Rust worker kernels for execution     │
   └──────────────────────────────────────────────────────┘
---

## Memory Mitigation Strategy

Processing 200–300 GB of uncompressed tick data in a single `pd.read_csv` or in-memory list is a guaranteed OOM on typical cloud instances.

DataPulse-Ingest-Engine avoids this through three complementary techniques:

1. **Chunked iteration**  
   Files (or generators) are read in fixed-size blocks (default 100 k–500 k rows). Only one block resides in RAM at any moment.

2. **Generator-based pipelines**  
   Each stage yields cleaned rows or small DataFrames instead of accumulating the full dataset. Downstream consumers pull at their own pace.

3. **Early sanitisation + drop**  
   Anomalous rows (price spikes, missing fields, non-monotonic timestamps) are discarded inside the chunk before any further allocation occurs.

Result: peak RSS stays roughly proportional to chunk size, independent of total stream volume.

---

## Design Principles

- **Fail-closed sanitisation** — any row that fails schema or statistical checks is dropped; never forwarded.
- **Explicit timing** — every major stage records wall-clock duration for observability.
- **Typed contracts** — full type hints and structured return objects.
- **Testable at scale** — unit tests simulate multi-gigabyte behaviour via generators without writing actual GB files.

---

## Quick Start

```bash
pip install -r requirements.txt

# Run the ingestion demo (uses a small synthetic stream)
python pipeline_ingest.py

# Execute the test suite
pytest test_pipeline.py -v
Repository Layout
DataPulse-Ingest-Engine/
├── README.md
├── pipeline_ingest.py      # Core chunked ingestion + sanitisation
├── test_pipeline.py        # pytest coverage with simulated large streams
└── requirements.txt
Attribution
Architected by a Machine Learning Infrastructure & Big Data Architect.
This repository demonstrates production-grade memory-safe pipelines for quantitative AI systems.
Protected under proprietary guidelines. All rights reserved.
