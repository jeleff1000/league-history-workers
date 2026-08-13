"""Materialize exact MFL league-weeks needing a source-roster identity bridge.

Classification artifacts identify a player-week and MFL source league, but do
not carry the source franchise/player identity needed for a safe team fan-out.
This read-only step emits only the league-weeks where a source signal exists
and an existing canonical player row still has a NULL supported outcome cell.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import duckdb


KEY = ("db_name", "year", "week", "NFL_player_id")
FIELDS = {
    "source_win": "win",
    "source_loss": "loss",
    "source_tie": "tie",
    "source_team_points": "team_points",
    "source_is_playoffs": "is_playoffs",
}


def _path(path: Path) -> str:
    return "'" + str(path.resolve()).replace("\\", "/").replace("'", "''") + "'"


def _columns(con: duckdb.DuckDBPyConnection, relation: str) -> set[str]:
    return {str(row[0]) for row in con.execute(f"DESCRIBE {relation}").fetchall()}


def materialize(*, base: Path, sources: list[Path], out: Path, report: Path) -> dict:
    if not sources:
        raise ValueError("at least one classification source is required")
    con = duckdb.connect(str(base), read_only=True)
    try:
        cache_columns = _columns(con, "public.player_fantasy")
        missing_cache = set(KEY) | set(FIELDS.values())
        missing_cache -= cache_columns
        if missing_cache:
            raise ValueError(f"canonical schema missing: {sorted(missing_cache)}")
        source_paths = [_path(path) for path in sources]
        con.execute(
            "CREATE TEMP VIEW source AS SELECT * FROM read_parquet(["
            + ",".join(source_paths) + "], union_by_name=true)"
        )
        source_columns = _columns(con, "source")
        required = set(KEY) | {"source_id"} | set(FIELDS)
        missing_source = required - source_columns
        if missing_source:
            raise ValueError(f"classification schema missing: {sorted(missing_source)}")
        signal_predicate = " OR ".join(f"s.{source} IS NOT NULL" for source in FIELDS)
        null_predicate = " OR ".join(
            f"(s.{source} IS NOT NULL AND p.{target} IS NULL)"
            for source, target in FIELDS.items()
        )
        con.execute(f"""
            CREATE TEMP TABLE candidate_rows AS
            SELECT DISTINCT
              CAST(s.db_name AS VARCHAR) AS db_name,
              CAST(s.year AS INTEGER) AS year,
              CAST(s.week AS INTEGER) AS week,
              CAST(s.source_id AS VARCHAR) AS source_id,
              CAST(s.NFL_player_id AS VARCHAR) AS NFL_player_id
            FROM source s
            JOIN public.player_fantasy p
              ON CAST(p.db_name AS VARCHAR)=CAST(s.db_name AS VARCHAR)
             AND CAST(p.year AS INTEGER)=CAST(s.year AS INTEGER)
             AND CAST(p.week AS INTEGER)=CAST(s.week AS INTEGER)
             AND CAST(p.NFL_player_id AS VARCHAR)=CAST(s.NFL_player_id AS VARCHAR)
            WHERE s.source_id IS NOT NULL
              AND ({signal_predicate})
              AND ({null_predicate})
        """)
        rows = con.execute("""
            SELECT db_name, year, week, source_id
            FROM candidate_rows
            GROUP BY 1,2,3,4
            ORDER BY 1,2,3,4
        """).fetchall()
        groups = [
            {"db_name": db_name, "year": int(year), "week": int(week), "source_id": source_id}
            for db_name, year, week, source_id in rows
        ]
        result = {
            "read_only": True,
            "cache_mutated": False,
            "new_lineage": False,
            "canonical_schema_unchanged": True,
            "source_files": len(sources),
            "source_rows": int(con.execute("SELECT COUNT(*) FROM source").fetchone()[0]),
            "source_rows_with_signal": int(con.execute(
                f"SELECT COUNT(*) FROM source s WHERE {signal_predicate}"
            ).fetchone()[0]),
            "candidate_cache_rows": int(con.execute("SELECT COUNT(*) FROM candidate_rows").fetchone()[0]),
            "target_groups": len(groups),
        }
    finally:
        con.close()
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps({"groups": groups}, indent=2) + "\n", encoding="utf-8")
    report.parent.mkdir(parents=True, exist_ok=True)
    report.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return result


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--base", type=Path, required=True)
    ap.add_argument("--source", type=Path, action="append", required=True)
    ap.add_argument("--out", type=Path, required=True)
    ap.add_argument("--report", type=Path, required=True)
    args = ap.parse_args()
    print(json.dumps(materialize(
        base=args.base, sources=args.source, out=args.out, report=args.report,
    ), sort_keys=True))


if __name__ == "__main__":
    main()
