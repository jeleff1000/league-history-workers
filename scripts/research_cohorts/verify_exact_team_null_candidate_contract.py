"""Fail-closed verification for a team-to-player NULL-fill candidate.

The verifier makes the promotion contract explicit: all candidate rows must
have NULL canonical loss/tie cells, and no other source-backed field may still
be NULL in the canonical snapshot captured in the delta.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import duckdb


KEY_COLUMNS = (
    "db_name", "year", "week", "NFL_player_id", "manager",
    "team_key", "team_name", "platform",
)
OTHER_FIELDS = (
    ("source_win", "canonical_win"),
    ("source_team_points", "canonical_team_points"),
    ("source_playoffs", "canonical_is_playoffs"),
    ("source_champion", "canonical_champion"),
    ("source_final_playoff_seed", "canonical_final_playoff_seed"),
)
SOURCE_VALUE_COLUMNS = (
    "source_win", "source_loss", "source_tie", "source_team_points",
    "source_playoffs", "source_champion", "source_final_playoff_seed",
)


def _one(root: Path, name: str) -> Path:
    files = list(root.rglob(name))
    if len(files) != 1:
        raise SystemExit(f"expected exactly one {name}, found {len(files)}")
    return files[0]


def verify(candidate_root: Path) -> dict[str, int]:
    """Return fail-closed cardinality evidence for one candidate bundle."""
    report_path = _one(candidate_root, "team_signal_candidate_report.json")
    delta_path = _one(candidate_root, "team_promotable_player_delta.parquet")
    report = json.loads(report_path.read_text(encoding="utf-8"))
    required = {
        "read_only": True,
        "cache_mutated": False,
        "new_lineage": False,
        "canonical_schema_unchanged": True,
    }
    for key, expected in required.items():
        if report.get(key) != expected:
            raise SystemExit(f"candidate invariant {key}={report.get(key)!r}, expected {expected!r}")

    con = duckdb.connect()
    try:
        escaped = str(delta_path.resolve()).replace("'", "''")
        con.execute(f"CREATE TEMP VIEW candidate AS SELECT * FROM read_parquet('{escaped}')")
        columns = {row[0] for row in con.execute("DESCRIBE candidate").fetchall()}
        required_columns = set(KEY_COLUMNS) | {"source_loss", "source_tie", "canonical_loss", "canonical_tie"}
        required_columns |= {source for source, _ in OTHER_FIELDS} | {canonical for _, canonical in OTHER_FIELDS}
        missing = sorted(required_columns - columns)
        if missing:
            raise SystemExit(f"candidate schema missing: {missing}")
        key = ", ".join('"' + column + '"' for column in KEY_COLUMNS)
        other_fill = " + ".join(
            f"CASE WHEN {source} IS NOT NULL AND {canonical} IS NULL THEN 1 ELSE 0 END"
            for source, canonical in OTHER_FIELDS
        )
        raw_rows, loss_null_rows, tie_null_rows, other_fill_rows = con.execute(f"""
            SELECT
              COUNT(*),
              COUNT(*) FILTER (WHERE source_loss IS NOT NULL AND canonical_loss IS NULL),
              COUNT(*) FILTER (WHERE source_tie IS NOT NULL AND canonical_tie IS NULL),
              COALESCE(SUM({other_fill}), 0)
            FROM candidate
        """).fetchone()
        unique_rows = con.execute(
            f"SELECT COUNT(*) FROM (SELECT {key} FROM candidate GROUP BY {key})"
        ).fetchone()[0]
        duplicate_variants = " OR ".join(
            f"COUNT(DISTINCT COALESCE(CAST({column} AS VARCHAR), '<NULL>')) > 1"
            for column in SOURCE_VALUE_COLUMNS
        )
        conflicting_duplicate_keys = con.execute(f"""
            SELECT COUNT(*) FROM (
              SELECT {key} FROM candidate GROUP BY {key} HAVING {duplicate_variants}
            )
        """).fetchone()[0]
    finally:
        con.close()
    result = {
        "raw_rows": int(raw_rows),
        "unique_rows": int(unique_rows),
        "loss_null_rows": int(loss_null_rows),
        "tie_null_rows": int(tie_null_rows),
        "other_fill_rows": int(other_fill_rows),
        "conflicting_duplicate_keys": int(conflicting_duplicate_keys),
    }
    if not result["raw_rows"]:
        raise SystemExit("candidate is empty")
    if result["loss_null_rows"] != result["raw_rows"]:
        raise SystemExit(f"candidate contains non-loss-null rows: {result}")
    if result["tie_null_rows"] != result["raw_rows"]:
        raise SystemExit(f"candidate contains non-tie-null rows: {result}")
    if result["other_fill_rows"]:
        raise SystemExit(f"candidate can fill fields beyond loss/tie: {result}")
    if result["conflicting_duplicate_keys"]:
        raise SystemExit(f"candidate contains conflicting duplicate source values: {result}")
    return result


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--candidate-root", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()
    result = verify(args.candidate_root)
    args.out.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    main()
