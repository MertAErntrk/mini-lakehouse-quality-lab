import csv
import json
import subprocess
import sys

import duckdb
import pytest

from lakehouse_lab import ingest


def write_csv(path, records):
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(["order_id", "customer_id", "amount", "status", "updated_at"])
        writer.writerows(records)


def test_replay_upsert_and_quarantine(tmp_path):
    first = tmp_path / "first.csv"
    second = tmp_path / "second.csv"
    output = tmp_path / "build"
    write_csv(first, [
        ("O-1", "C-1", "10.00", "paid", "2026-10-01T00:00:00+00:00"),
        ("O-2", "C-2", "20.00", "refunded", "2026-10-01T00:00:00+00:00"),
        ("O-3", "C-3", "-1", "paid", "2026-10-01T00:00:00+00:00"),
        ("O-4", "C-4", "1", "paid", "2026-10-01T00:00:00+00:00"),
        ("O-4", "C-4", "2", "paid", "2026-10-02T00:00:00+00:00"),
    ])
    write_csv(second, [
        ("O-1", "C-1", "15.00", "paid", "2026-10-02T00:00:00+00:00"),
        ("O-2", "C-2", "99.00", "refunded", "2026-09-30T00:00:00+00:00"),
        ("O-5", "C-5", "5.00", "paid", "2026-10-02T00:00:00+00:00"),
    ])
    assert ingest(first, output)["rejected_rows"] == 3
    assert ingest(first, output)["skipped_existing_file"] is True
    assert ingest(second, output)["current_orders"] == 3

    conn = duckdb.connect(str(output / "state.duckdb"), read_only=True)
    try:
        assert conn.execute("SELECT COUNT(*) FROM bronze_events").fetchone()[0] == 8
        assert conn.execute("SELECT amount FROM silver_orders WHERE order_id='O-1'").fetchone()[0] == 15
        assert conn.execute("SELECT amount FROM silver_orders WHERE order_id='O-2'").fetchone()[0] == 20
        assert conn.execute("SELECT COUNT(*) FROM read_parquet(?)", [str(output / "quarantine.parquet")]).fetchone()[0] == 3
        assert conn.execute("SELECT status, order_count, total_amount FROM read_parquet(?) ORDER BY status", [str(output / "gold.parquet")]).fetchall() == [
            ("paid", 2, 20), ("refunded", 1, 20)
        ]
    finally:
        conn.close()


def test_bad_header_does_not_create_state(tmp_path):
    source = tmp_path / "bad.csv"
    source.write_text("order_id,amount\nO-1,10\n", encoding="utf-8")
    with pytest.raises(ValueError, match="Expected CSV columns"):
        ingest(source, tmp_path / "build")
    assert not (tmp_path / "build").exists()


def test_unclosed_quote_does_not_create_state(tmp_path):
    source = tmp_path / "malformed.csv"
    source.write_text(
        'order_id,customer_id,amount,status,updated_at\n'
        'O-1,C-1,"10,paid,2026-10-01T00:00:00+00:00\n',
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="Malformed CSV"):
        ingest(source, tmp_path / "build")
    assert not (tmp_path / "build").exists()


def test_timestamp_must_be_timezone_aware(tmp_path):
    source = tmp_path / "naive.csv"
    write_csv(source, [("O-1", "C-1", "10", "paid", "2026-10-01T00:00:00")])
    result = ingest(source, tmp_path / "build")
    assert result["accepted_rows"] == 0
    assert result["rejected_rows"] == 1


def test_timestamp_outside_utc_range_is_quarantined(tmp_path):
    source = tmp_path / "orders.csv"
    output = tmp_path / "build"
    write_csv(source, [
        ("O-1", "C-1", "10", "paid", "0001-01-01T00:00:00+01:00"),
        ("O-2", "C-2", "20", "paid", "2026-10-01T00:00:00+00:00"),
    ])

    result = ingest(source, output)

    assert result["accepted_rows"] == 1
    assert result["rejected_rows"] == 1
    conn = duckdb.connect(str(output / "state.duckdb"), read_only=True)
    try:
        assert conn.execute("SELECT order_id, error FROM bronze_events ORDER BY line_number").fetchall() == [
            ("O-1", "invalid_timestamp"), ("O-2", None)
        ]
        assert conn.execute("SELECT order_id FROM silver_orders").fetchall() == [("O-2",)]
    finally:
        conn.close()


def test_short_row_is_quarantined(tmp_path):
    source = tmp_path / "short.csv"
    source.write_text(
        "order_id,customer_id,amount,status,updated_at\nO-1,C-1,10\n",
        encoding="utf-8",
    )
    result = ingest(source, tmp_path / "build")
    assert result["accepted_rows"] == 0
    assert result["rejected_rows"] == 1


def test_cli_can_fail_a_quality_gate_after_export(tmp_path):
    source = tmp_path / "orders.csv"
    output = tmp_path / "build"
    write_csv(source, [("O-1", "C-1", "-1", "paid", "2026-10-01T00:00:00+00:00")])
    run = subprocess.run(
        [sys.executable, "-m", "lakehouse_lab", str(source), "--output", str(output), "--fail-on-rejected"],
        capture_output=True,
        text=True,
        check=False,
    )
    assert run.returncode == 1
    assert json.loads(run.stdout)["rejected_rows"] == 1
    assert (output / "quarantine.parquet").exists()
