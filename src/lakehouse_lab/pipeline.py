"""Validate order events, upsert current state, and export Parquet layers."""

from __future__ import annotations

import csv
import hashlib
from collections import Counter
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path

import duckdb

FIELDS = ("order_id", "customer_id", "amount", "status", "updated_at")
STATUSES = {"paid", "refunded"}


def _validate(row: dict[str, str | None], duplicate_ids: set[str]) -> tuple[str | None, tuple | None]:
    order_id = (row["order_id"] or "").strip()
    customer_id = (row["customer_id"] or "").strip()
    if not order_id or not customer_id:
        return "missing_order_or_customer_id", None
    if order_id in duplicate_ids:
        return "duplicate_order_id_in_batch", None
    try:
        amount = Decimal((row["amount"] or "").strip())
    except InvalidOperation:
        return "invalid_amount", None
    if not amount.is_finite() or amount < 0 or amount > Decimal("9999999999.99") or amount.as_tuple().exponent < -2:
        return "invalid_amount", None
    status = (row["status"] or "").strip().lower()
    if status not in STATUSES:
        return "invalid_status", None
    try:
        timestamp = datetime.fromisoformat((row["updated_at"] or "").strip())
    except ValueError:
        return "invalid_timestamp", None
    if timestamp.tzinfo is None:
        return "timestamp_requires_timezone", None
    return None, (order_id, customer_id, amount, status, timestamp.astimezone(timezone.utc))


def _schema(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS ingested_files (
            file_sha VARCHAR PRIMARY KEY, source_name VARCHAR NOT NULL,
            accepted INTEGER NOT NULL, rejected INTEGER NOT NULL
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS bronze_events (
            file_sha VARCHAR NOT NULL, line_number INTEGER NOT NULL,
            order_id VARCHAR, customer_id VARCHAR, amount VARCHAR,
            status VARCHAR, updated_at VARCHAR, error VARCHAR,
            PRIMARY KEY (file_sha, line_number)
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS silver_orders (
            order_id VARCHAR PRIMARY KEY, customer_id VARCHAR NOT NULL,
            amount DECIMAL(12,2) NOT NULL, status VARCHAR NOT NULL,
            updated_at TIMESTAMPTZ NOT NULL
        )
    """)
    conn.execute("""
        CREATE OR REPLACE VIEW gold_order_summary AS
        SELECT status, COUNT(*) AS order_count, SUM(amount) AS total_amount
        FROM silver_orders GROUP BY status
    """)


def _export(conn: duckdb.DuckDBPyConnection, output: Path) -> None:
    layers = {
        "bronze": "SELECT * FROM bronze_events ORDER BY file_sha, line_number",
        "silver": "SELECT * FROM silver_orders ORDER BY order_id",
        "gold": "SELECT * FROM gold_order_summary ORDER BY status",
        "quarantine": "SELECT * FROM bronze_events WHERE error IS NOT NULL ORDER BY file_sha, line_number",
    }
    for name, query in layers.items():
        target = output / f"{name}.parquet"
        temporary = output / f".{name}.tmp.parquet"
        quoted = str(temporary.resolve()).replace("'", "''")
        conn.execute(f"COPY ({query}) TO '{quoted}' (FORMAT PARQUET)")
        temporary.replace(target)


def ingest(source: Path, output: Path) -> dict[str, int | bool | str]:
    """Ingest a CSV once by content hash; keep bad rows visible in quarantine."""
    source = source.resolve()
    output = output.resolve()
    file_sha = hashlib.sha256(source.read_bytes()).hexdigest()
    with source.open("r", newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        if reader.fieldnames is None or tuple(reader.fieldnames) != FIELDS:
            raise ValueError(f"Expected CSV columns in this order: {', '.join(FIELDS)}")
        rows = list(reader)
    if any(None in row for row in rows):
        raise ValueError("A row has more fields than the CSV header")
    ids = Counter((row["order_id"] or "").strip() for row in rows)
    duplicate_ids = {order_id for order_id, count in ids.items() if order_id and count > 1}
    checked = [(_validate(row, duplicate_ids), row) for row in rows]
    accepted = [parsed for (error, parsed), _ in checked if error is None]
    rejected = len(rows) - len(accepted)

    output.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(str(output / "state.duckdb"))
    try:
        _schema(conn)
        existing = conn.execute(
            "SELECT accepted, rejected FROM ingested_files WHERE file_sha = ?", [file_sha]
        ).fetchone()
        skipped = existing is not None
        if existing:
            accepted_count, rejected_count = existing
        else:
            conn.execute("BEGIN TRANSACTION")
            try:
                if rows:
                    conn.executemany(
                        "INSERT INTO bronze_events VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                        [
                            (file_sha, line_number, *(row[field] for field in FIELDS), error)
                            for line_number, ((error, _), row) in enumerate(checked, start=2)
                        ],
                    )
                conn.execute("""
                    CREATE TEMP TABLE staging (
                        order_id VARCHAR, customer_id VARCHAR, amount DECIMAL(12,2),
                        status VARCHAR, updated_at TIMESTAMPTZ
                    )
                """)
                if accepted:
                    conn.executemany("INSERT INTO staging VALUES (?, ?, ?, ?, ?)", accepted)
                    conn.execute("""
                        MERGE INTO silver_orders AS current
                        USING staging AS incoming
                        ON current.order_id = incoming.order_id
                        WHEN MATCHED AND incoming.updated_at > current.updated_at
                            THEN UPDATE SET customer_id = incoming.customer_id,
                                amount = incoming.amount, status = incoming.status,
                                updated_at = incoming.updated_at
                        WHEN NOT MATCHED THEN INSERT
                            (order_id, customer_id, amount, status, updated_at)
                            VALUES (incoming.order_id, incoming.customer_id,
                                    incoming.amount, incoming.status, incoming.updated_at)
                    """)
                conn.execute(
                    "INSERT INTO ingested_files VALUES (?, ?, ?, ?)",
                    [file_sha, source.name, len(accepted), rejected],
                )
                conn.execute("COMMIT")
            except Exception:
                conn.execute("ROLLBACK")
                raise
            accepted_count, rejected_count = len(accepted), rejected
        _export(conn, output)
        silver_count = conn.execute("SELECT COUNT(*) FROM silver_orders").fetchone()[0]
        return {
            "source": source.name,
            "skipped_existing_file": skipped,
            "accepted_rows": accepted_count,
            "rejected_rows": rejected_count,
            "current_orders": silver_count,
        }
    finally:
        conn.close()
