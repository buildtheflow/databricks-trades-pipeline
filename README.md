# Trades Medallion Pipeline — Databricks

End-to-end Bronze → Silver → Gold pipeline using Unity Catalog Volumes,
managed Delta tables, and the hybrid notebook-orchestrator pattern.
Deployed via Databricks Asset Bundles (DAB) to dev and prod targets.

## Architecture

```
Databricks Workflow
    └── pipeline_orchestrator.py      (notebook — orchestration only)
            ├── bronze.py             (Python module — raw ingest, MERGE)
            ├── silver.py             (Python module — cleanse, DQ, quarantine)
            └── gold.py               (Python module — aggregation)
```

**Hybrid pattern:** the notebook owns orchestration order, logging, and
display. All Spark logic lives in testable Python modules under `src/pipeline/`.

## Unity Catalog structure

```
finmarket/
├── landing/
│   └── raw_trades/  (Volume)
│       └── trades_20240115.json     ← raw files land here
├── bronze/
│   └── trades                       ← Delta table (MERGE on trade_id)
├── silver/
│   ├── trades                       ← Delta table (DQ-passing rows, overwrite)
│   └── trades_quarantine            ← Delta table (DQ-failing rows, append)
└── gold/
    └── trade_summary                ← Delta table (aggregated, overwrite)
```

## Project structure

```
databricks-trades-pipeline/
├── databricks.yml                          ← DAB bundle config (dev + prod targets)
├── notebooks/
│   ├── pipeline_orchestrator.py            ← Workflow entry point (hybrid orchestrator)
│   └── trades_medallion_exploration.py     ← Exploration only — not deployed
├── src/
│   └── pipeline/
│       ├── config.py       ← YAML config loader (env-aware)
│       ├── schema.py       ← Explicit schema definitions (never infer)
│       ├── bronze.py       ← Raw ingest — MERGE on trade_id (idempotent)
│       ├── silver.py       ← Cleanse, DQ flags, quarantine split
│       └── gold.py         ← Daily aggregation by trader/instrument/currency/side
├── config/
│   ├── dev.yml             ← Dev paths, DQ threshold 10%, warn only
│   └── prod.yml            ← Prod paths, DQ threshold 5%, fail on breach
├── sample_data/
│   └── trades_20240115.json   ← 10 sample rows (2 intentional DQ failures)
└── tests/
    └── test_silver.py          ← Unit tests for Silver transformations
```

## Key design decisions

| Decision | Approach |
|---|---|
| Bronze write mode | MERGE on trade_id — idempotent, no duplicates on rerun |
| Silver write mode | Overwrite — full reload each run |
| Gold write mode | Overwrite — rebuilt from Silver each run |
| Quarantine write mode | Append — preserves full failure history |
| Source file tracking | `_metadata.file_path` — correct for Unity Catalog Volumes |
| Schema | Explicit — never inferred in production |
| DQ rules | trade_id, trader_id, instrument_id, quantity > 0, price > 0, side, trade_date |

## Validated counts (sample data)

```
Bronze     : 10 rows  (all raw records)
Silver     :  8 rows  (80% retention)
Quarantine :  2 rows  (T-009 null trader_id, T-010 negative quantity)
Gold       :  8 rows  (aggregated grain)
```

## Prerequisites

- Databricks workspace (AWS)
- Unity Catalog enabled with `finmarket` catalog
- Volume created: `finmarket.landing.raw_trades`
- Databricks CLI v1.16+ installed locally
- GitHub repo connected

## Setup steps

### 1. Upload sample data to Volume

Run in a Databricks notebook:
```python
import json

trades = [...]  # paste from sample_data/trades_20240115.json

with open("/Volumes/finmarket/landing/raw_trades/trades_20240115.json", "w") as f:
    json.dump(trades, f)

import os
print(os.listdir("/Volumes/finmarket/landing/raw_trades/"))
# Expected: ['trades_20240115.json']
```

### 2. Install Databricks CLI v2

```powershell
# Windows
winget install Databricks.DatabricksCLI

# Verify
databricks --version
# Expected: Databricks CLI v1.16.x or higher
```

### 3. Authenticate

```bash
databricks auth login --host https://<your-workspace>.cloud.databricks.com
# Press Enter to accept default profile name
# Browser opens — log in with your Databricks credentials
```

### 4. Update databricks.yml

Update the following in `databricks.yml`:
- `host` — your workspace URL
- `repo_path` default — path where DAB uploads bundle files

### 5. Deploy to dev

```bash
databricks bundle deploy --target dev
```

Expected output:
```
Created jobs.trades_medallion_full
Created jobs.trades_medallion_bronze_only
Created jobs.trades_medallion_silver_only
Created jobs.trades_medallion_gold_only
Files: 14 uploaded, 0 deleted
Resources: 4 created
```

### 6. Run full pipeline on dev

```bash
databricks bundle run trades_medallion_full --target dev
```

Expected output:
```json
{"bronze": {"rows_written": 10}, "silver": {"rows_written": 8, "rows_quarantined": 2}, "gold": {"rows_written": 8}, "status": "SUCCESS"}
```

### 7. Deploy to prod

```bash
databricks bundle deploy --target prod
databricks bundle run trades_medallion_full --target prod
```

### 8. Verify in Unity Catalog

```sql
SELECT COUNT(*) FROM finmarket.bronze.trades;           -- 10
SELECT COUNT(*) FROM finmarket.silver.trades;           -- 8
SELECT COUNT(*) FROM finmarket.silver.trades_quarantine; -- 2
SELECT COUNT(*) FROM finmarket.gold.trade_summary;      -- 8

-- Check quarantine failures
SELECT trade_id, trader_id, quantity, side, dq_pass
FROM finmarket.silver.trades_quarantine;
```

## Rerunning a single layer

```bash
# Rerun only Silver (e.g. after a DQ rule fix)
databricks bundle run trades_medallion_silver_only --target dev
databricks bundle run trades_medallion_silver_only --target prod
```

## DAB bundle structure (after deploy)

DAB uploads files to `.bundle` under your user:
```
/Workspace/Users/<email>/.bundle/trades-medallion-pipeline/
├── dev/
│   ├── files/    ← your code
│   ├── artifacts/
│   └── state/    ← DAB tracks deployed resource IDs here
└── prod/
    ├── files/
    ├── artifacts/
    └── state/
```

Never edit files inside `.bundle` directly.
Source of truth is always: **local machine → GitHub → `databricks bundle deploy`**

## Running tests locally

```bash
pip install pyspark pytest pyyaml
pytest tests/ -v
```

## Next steps

- [ ] GitHub Actions CI/CD — auto deploy on push
- [ ] Project 2 — API ingestion + Auto Loader streaming
