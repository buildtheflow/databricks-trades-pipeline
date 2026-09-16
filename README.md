# Trades Medallion Pipeline — Databricks

End-to-end Bronze → Silver → Gold pipeline using Unity Catalog Volumes
and the hybrid notebook-orchestrator pattern.

## Architecture

```
Databricks Workflow
    └── pipeline_orchestrator.py      (notebook — orchestration only)
            ├── bronze.py             (Python module — raw ingest)
            ├── silver.py             (Python module — cleanse + DQ)
            └── gold.py               (Python module — aggregation)
```

## Unity Catalog structure

```
finmarket/
├── landing/
│   └── raw_trades/  (Volume)
│       └── trades_20240115.json     ← raw files land here
├── bronze/
│   └── trades                       ← Delta table (written by pipeline)
├── silver/
│   ├── trades                       ← Delta table (DQ-passing rows)
│   └── trades_quarantine            ← Delta table (DQ-failing rows)
└── gold/
    └── trade_summary                ← Delta table (aggregated)
```

## Project structure

```
trades-pipeline/
├── databricks.yml                          ← DAB bundle config
├── notebooks/
│   ├── pipeline_orchestrator.py            ← Workflow entry point
│   └── trades_medallion_exploration.py     ← Exploration only
├── src/pipeline/
│   ├── config.py       ← YAML config loader
│   ├── schema.py       ← Schema definitions
│   ├── bronze.py       ← Raw ingest logic
│   ├── silver.py       ← Cleanse, DQ, quarantine
│   └── gold.py         ← Aggregation
├── config/
│   ├── dev.yml         ← Dev paths and settings
│   └── prod.yml        ← Prod paths and settings
├── sample_data/
│   └── trades_20240115.json   ← Upload to Volume to test
└── tests/
    └── test_silver.py
```

## Setup steps

### 1. Upload sample data to Volume
Run this in a notebook:
```python
import json
with open("/Volumes/finmarket/landing/raw_trades/trades_20240115.json") as f:
    print(json.load(f))   # verify file is readable
```

### 2. Push repo to Databricks Repos
In the workspace: Repos → Add Repo → paste your GitHub URL

### 3. Get your cluster ID
Compute → your cluster → Configuration tab → copy the Cluster ID

### 4. Update databricks.yml
Replace `your-cluster-id` with your actual cluster ID.
Replace the repo path email with yours.

### 5. Deploy via DAB
```bash
pip install databricks-cli
databricks configure --token
databricks bundle deploy --target dev
```

### 6. Run manually to test
```bash
databricks bundle run trades_medallion_full --target dev
```

### 7. Verify in Unity Catalog
```sql
SELECT COUNT(*) FROM finmarket.bronze.trades;
SELECT COUNT(*) FROM finmarket.silver.trades;
SELECT COUNT(*) FROM finmarket.gold.trade_summary;
SELECT * FROM finmarket.silver.trades_quarantine;
```

## Rerunning a single layer
```bash
databricks bundle run trades_medallion_silver_only --target dev
```

## Running tests locally
```bash
pip install pyspark pytest
pytest tests/ -v
```
