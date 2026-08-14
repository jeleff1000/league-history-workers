"""Aggregate immutable candidate-cell shard artifacts without opening a cache."""

from __future__ import annotations

import json
from collections import Counter
from pathlib import Path
from typing import Iterable

import duckdb


EMPTY_CANDIDATE_SQL = """
SELECT
  NULL::VARCHAR AS artifact_id,
  NULL::VARCHAR AS artifact_file,
  NULL::VARCHAR AS file_instance,
  NULL::VARCHAR AS source_grain,
  NULL::VARCHAR AS db_name,
  NULL::VARCHAR AS year,
  NULL::VARCHAR AS week,
  NULL::VARCHAR AS NFL_player_id,
  NULL::VARCHAR AS team_key,
  NULL::VARCHAR AS manager,
  NULL::VARCHAR AS source_column,
  NULL::VARCHAR AS target_column,
  NULL::VARCHAR AS source_value_text
WHERE FALSE
"""


def aggregate_candidate_cells(
    *, shard_root: Path, expected_artifact_ids: Iterable[str], out_parquet: Path
) -> dict:
    """Write a fixed-schema candidate table and coverage receipt for every shard run.

    A coverage miss is represented in the receipt instead of being silently treated
    as a complete artifact pass. The caller decides whether that is fatal.
    """
    expected = {str(value) for value in expected_artifact_ids}
    receipt_paths = sorted(shard_root.rglob("receipts/*.json"))
    receipts = [json.loads(path.read_text(encoding="utf-8")) for path in receipt_paths]
    received = {str(item["artifact_id"]) for item in receipts if item.get("artifact_id") is not None}
    candidates = sorted(shard_root.rglob("candidates/*.parquet"))

    con = duckdb.connect()
    if candidates:
        con.execute(
            "CREATE TABLE candidate_cells AS SELECT * FROM read_parquet(?)",
            [[str(path) for path in candidates]],
        )
    else:
        con.execute(f"CREATE TABLE candidate_cells AS {EMPTY_CANDIDATE_SQL}")
    out_parquet.parent.mkdir(parents=True, exist_ok=True)
    con.execute("COPY candidate_cells TO ? (FORMAT PARQUET)", [str(out_parquet)])
    candidate_count = int(con.execute("SELECT COUNT(*) FROM candidate_cells").fetchone()[0])
    by_grain = dict(
        con.execute(
            "SELECT source_grain, COUNT(*) FROM candidate_cells GROUP BY 1 ORDER BY 1"
        ).fetchall()
    )
    con.close()

    missing = sorted(expected - received)
    unexpected = sorted(received - expected)
    return {
        "expected_data_artifact_receipts": len(expected),
        "data_artifact_receipts": len(receipts),
        "distinct_received_artifact_ids": len(received),
        "receipt_coverage_complete": not missing and not unexpected,
        "missing_artifact_ids": missing,
        "unexpected_artifact_ids": unexpected,
        "candidate_cells": candidate_count,
        "candidate_cells_by_grain": by_grain,
        "receipt_statuses": dict(
            Counter(item.get("materialization_status", "candidate_cells_materialized") for item in receipts)
        ),
        "cache_mutated": False,
        "new_lineage": False,
    }
