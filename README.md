# Mini Lakehouse Quality Lab

A small, reproducible data engineering example: ingest order events from CSV, validate them, keep rejected rows visible, and publish Bronze, Silver, and Gold Parquet outputs. All included data is synthetic.

This is a **local teaching lab**, not an Apache Iceberg or Delta Lake implementation. DuckDB keeps the working state; Parquet files show the three layers without a cloud account or external data.

## Run it

Requires Python 3.11 or newer.

```bash
python -m venv .venv
# Activate the environment using your shell's usual command.
python -m pip install -e '.[test]'
python -m lakehouse_lab data/orders_batch1.csv --output build
python -m lakehouse_lab data/orders_batch2.csv --output build
python -m pytest -q
```

Running the same file again reports `skipped_existing_file: true`. Inspect the outputs with DuckDB:

For a CI quality gate, add `--fail-on-rejected`. The command still writes the quarantine output and JSON report, then exits with status 1 when any rows were rejected. The supplied first batch intentionally contains a bad row, so it is useful for demonstrating this failure mode.

```sql
SELECT * FROM read_parquet('build/bronze.parquet');
SELECT * FROM read_parquet('build/quarantine.parquet');
SELECT * FROM read_parquet('build/silver.parquet');
SELECT * FROM read_parquet('build/gold.parquet');
```

Expected after both sample batches: 7 Bronze events; 1 quarantined negative amount; 4 current Silver orders. `O-100` has amount 140.00 because batch 2 is newer; `O-101` stays at 80.00 because the batch 2 event is older.

## Quality rules

The input columns must be `order_id,customer_id,amount,status,updated_at` in that order. IDs must be present. An order ID repeated within one batch is quarantined in all its rows. Amount must be nonnegative with at most two decimal places. Status must be `paid` or `refunded`. Timestamp must include a time zone. For the same order ID across batches, only a strictly newer timestamp updates Silver. A byte-identical file is ingested once using its SHA-256 hash.

`bronze.parquet` retains the raw strings and an error code for each bad row. `quarantine.parquet` contains only bad rows. `silver.parquet` contains the latest accepted record for each order ID. `gold.parquet` summarizes current orders by status. The included tests cover replay, stale updates, duplicate IDs, rejected values, and missing time zones.

## Limits

This lab runs one writer at a time. A changed file is a new batch. It does not handle deletes, schema evolution, distributed execution, late-arriving corrections with identical timestamps, or production security controls. Exported Parquet files are snapshots derived from the DuckDB state, not a transactional table format.

## Why this exists

The companion [PostgreSQL exercises](https://github.com/MertAErntrk/PatikaDev_SQL) show SQL queries. This lab adds a complete, testable data flow with explicit data quality and replay behavior. Contributions that improve the example or document a real failure are welcome.
