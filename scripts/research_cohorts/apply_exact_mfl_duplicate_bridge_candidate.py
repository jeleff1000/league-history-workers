"""Apply a source-proven MFL duplicate-recipient repair in place.

The candidate carries the canonical values observed in the approved cache.  A
write is allowed only when every candidate rowid still has that exact snapshot;
this prevents a stale candidate from overwriting a later repair.  The source
cards are direct MFL team-week facts and replace the previously anonymous team
assignment for only those existing player rows.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import duckdb

try:
    from .canonical_player_schema import validate_player_schema
except ImportError:  # direct script execution in Actions
    from canonical_player_schema import validate_player_schema


_SNAPSHOT_FIELDS = (
    "manager", "team_key", "team_name", "mfl_player_id", "win", "loss",
    "tie", "team_points", "is_playoffs",
)
_SOURCE_FIELDS = (
    "manager", "team_key", "team_name", "mfl_player_id", "win", "loss",
    "tie", "team_points", "is_playoffs",
)


def _path(path: Path) -> str:
    return str(path.resolve()).replace("'", "''")


def _non_null_mismatch(field: str) -> str:
    return (
        f"CAST(p.{field} AS VARCHAR) IS DISTINCT FROM "
        f"CAST(c.canonical_{field} AS VARCHAR)"
    )


def apply(*, base: Path, candidate: Path, report: Path) -> dict:
    """Apply one exact direct-MFL candidate, refusing stale or broad writes."""
    con = duckdb.connect(str(base))
    columns_before = [row[0] for row in con.execute("DESCRIBE public.player_fantasy").fetchall()]
    validate_player_schema(columns_before)
    required_player = {"loss", "tie"}
    missing_player = required_player - set(columns_before)
    if missing_player:
        raise ValueError(f"direct MFL repair requires canonical columns: {sorted(missing_player)}")

    con.execute(f"CREATE TEMP VIEW candidate_raw AS SELECT * FROM read_parquet('{_path(candidate)}')")
    candidate_columns = {row[0] for row in con.execute("DESCRIBE candidate_raw").fetchall()}
    required_candidate = {
        "player_rowid", "db_name", "year", "week", "NFL_player_id",
        *{f"canonical_{field}" for field in _SNAPSHOT_FIELDS},
        *{f"source_{field}" for field in _SOURCE_FIELDS},
    }
    missing_candidate = required_candidate - candidate_columns
    if missing_candidate:
        raise ValueError(f"candidate schema missing fields: {sorted(missing_candidate)}")
    candidate_rows = int(con.execute("SELECT COUNT(*) FROM candidate_raw").fetchone()[0])
    if candidate_rows == 0:
        raise ValueError("refusing an empty direct-MFL candidate")
    duplicate_recipients = int(con.execute("""
      SELECT COUNT(*) FROM (
        SELECT player_rowid FROM candidate_raw GROUP BY player_rowid HAVING COUNT(*) > 1
      )
    """).fetchone()[0])
    if duplicate_recipients:
        raise ValueError(f"candidate has duplicate recipient rowids: {duplicate_recipients}")
    null_source_fields = {
        field: int(con.execute(
            f"SELECT COUNT(*) FROM candidate_raw WHERE source_{field} IS NULL"
        ).fetchone()[0])
        for field in _SOURCE_FIELDS
    }
    if any(null_source_fields.values()):
        raise ValueError(f"candidate has null direct-source fields: {null_source_fields}")

    con.execute("CREATE TEMP TABLE candidate AS SELECT * FROM candidate_raw")
    snapshot_mismatch = int(con.execute("""
      SELECT COUNT(*)
      FROM candidate c
      LEFT JOIN public.player_fantasy p ON p.rowid=c.player_rowid
      WHERE p.rowid IS NULL
         OR CAST(p.db_name AS VARCHAR) IS DISTINCT FROM CAST(c.db_name AS VARCHAR)
         OR TRY_CAST(p.year AS INTEGER) IS DISTINCT FROM TRY_CAST(c.year AS INTEGER)
         OR TRY_CAST(p.week AS INTEGER) IS DISTINCT FROM TRY_CAST(c.week AS INTEGER)
         OR CAST(p.NFL_player_id AS VARCHAR) IS DISTINCT FROM CAST(c.NFL_player_id AS VARCHAR)
         OR LOWER(TRIM(CAST(p.platform AS VARCHAR))) <> 'mfl'
         OR """ + " OR ".join(_non_null_mismatch(field) for field in _SNAPSHOT_FIELDS) + """
    """).fetchone()[0])
    if snapshot_mismatch:
        raise ValueError(f"candidate/cache snapshot mismatch: {snapshot_mismatch}")

    identity_fields = ("manager", "team_key", "team_name", "mfl_player_id")
    identity_not_null = {
        field: int(con.execute(
            f"SELECT COUNT(*) FROM candidate WHERE canonical_{field} IS NOT NULL"
        ).fetchone()[0])
        for field in identity_fields
    }
    if any(identity_not_null.values()):
        raise ValueError(f"candidate would overwrite known cache identity: {identity_not_null}")

    replacements = {
        field: int(con.execute(
            f"SELECT COUNT(*) FROM candidate WHERE "
            f"CAST(canonical_{field} AS VARCHAR) IS DISTINCT FROM CAST(source_{field} AS VARCHAR)"
        ).fetchone()[0])
        for field in ("win", "loss", "tie", "team_points", "is_playoffs")
    }
    identity_fills = {
        field: int(con.execute(
            f"SELECT COUNT(*) FROM candidate WHERE source_{field} IS NOT NULL"
        ).fetchone()[0])
        for field in identity_fields
    }
    rows_before = int(con.execute("SELECT COUNT(*) FROM public.player_fantasy").fetchone()[0])
    con.execute("BEGIN")
    try:
        con.execute("""
          UPDATE public.player_fantasy p SET
            manager=c.source_manager,
            team_key=c.source_team_key,
            team_name=c.source_team_name,
            mfl_player_id=c.source_mfl_player_id,
            win=c.source_win,
            loss=c.source_loss,
            tie=c.source_tie,
            team_points=c.source_team_points,
            is_playoffs=c.source_is_playoffs,
            has_po_signal=1
          FROM candidate c
          WHERE p.rowid=c.player_rowid
        """)
        rows_after = int(con.execute("SELECT COUNT(*) FROM public.player_fantasy").fetchone()[0])
        columns_after = [row[0] for row in con.execute("DESCRIBE public.player_fantasy").fetchall()]
        if rows_after != rows_before:
            raise ValueError(f"player row count changed: {rows_before} -> {rows_after}")
        if columns_after != columns_before:
            raise ValueError("canonical player schema changed")
        remaining_source_mismatches = {
            field: int(con.execute(
                f"""SELECT COUNT(*) FROM public.player_fantasy p JOIN candidate c ON p.rowid=c.player_rowid
                    WHERE CAST(p.{field} AS VARCHAR) IS DISTINCT FROM CAST(c.source_{field} AS VARCHAR)"""
            ).fetchone()[0])
            for field in _SOURCE_FIELDS
        }
        remaining_po_signal = int(con.execute("""
          SELECT COUNT(*) FROM public.player_fantasy p JOIN candidate c ON p.rowid=c.player_rowid
          WHERE TRY_CAST(p.has_po_signal AS INTEGER) IS DISTINCT FROM 1
        """).fetchone()[0])
        if any(remaining_source_mismatches.values()) or remaining_po_signal:
            raise ValueError(
                "direct-MFL readback failed: "
                f"fields={remaining_source_mismatches} has_po_signal={remaining_po_signal}"
            )
        con.execute("COMMIT")
    except Exception:
        con.execute("ROLLBACK")
        raise
    finally:
        con.close()

    result = {
        "in_place": True,
        "cache_mutated": True,
        "new_lineage": False,
        "schema_unchanged": True,
        "player_rows_unchanged": True,
        "candidate_rows": candidate_rows,
        "recipient_rows": candidate_rows,
        "snapshot_mismatch_rows": 0,
        "identity_fills": identity_fills,
        "source_replacements": replacements,
        "readback_remaining_source_mismatches": {field: 0 for field in _SOURCE_FIELDS},
        "readback_remaining_has_po_signal": 0,
    }
    report.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    return result


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base", type=Path, required=True)
    parser.add_argument("--candidate", type=Path, required=True)
    parser.add_argument("--report", type=Path, required=True)
    args = parser.parse_args()
    print(json.dumps(apply(base=args.base, candidate=args.candidate, report=args.report), sort_keys=True))


if __name__ == "__main__":
    main()
