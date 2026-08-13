"""Derive source team-week signals from MFL classification and raw rosters.

The classification artifact supplies source outcomes by NFL player; the MFL
weekly roster supplies the missing franchise identity.  A signal is emitted
only when a classified NFL player has exactly one recovered source franchise
and every non-null outcome field agrees within that franchise/week.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import duckdb


CLASSIFICATION_REQUIRED = {
    "db_name", "year", "week", "NFL_player_id", "source_win", "source_loss",
    "source_tie", "source_team_points", "source_is_playoffs",
}
MEMBERSHIP_REQUIRED = {
    "db_name", "year", "week", "source_year", "source_franchise_id", "mfl_player_id",
}
CROSSWALK_REQUIRED = {"source_year", "mfl_player_id", "NFL_player_id"}
FIELDS = {
    "source_win": ("win", "INTEGER"),
    "source_loss": ("loss", "INTEGER"),
    "source_tie": ("tie", "INTEGER"),
    "source_team_points": ("team_points", "DOUBLE"),
    "source_is_playoffs": ("is_playoffs", "INTEGER"),
}


def _path(path: Path) -> str:
    return "'" + str(path.resolve()).replace("\\", "/").replace("'", "''") + "'"


def _columns(con: duckdb.DuckDBPyConnection, relation: str) -> set[str]:
    return {str(row[0]) for row in con.execute(f"DESCRIBE {relation}").fetchall()}


def _require(con: duckdb.DuckDBPyConnection, relation: str, required: set[str], label: str) -> None:
    missing = sorted(required - _columns(con, relation))
    if missing:
        raise ValueError(f"{label} missing required columns: {missing}")


def build(
    *, classifications: list[Path], memberships: Path, crosswalk: Path, out: Path, report: Path,
) -> dict:
    if not classifications:
        raise ValueError("at least one classification file is required")
    con = duckdb.connect()
    try:
        classification_paths = ",".join(_path(path) for path in classifications)
        con.execute(
            "CREATE TEMP VIEW classifications AS SELECT * FROM read_parquet(["
            + classification_paths + "], union_by_name=true)"
        )
        con.execute(f"CREATE TEMP VIEW memberships AS SELECT * FROM read_parquet({_path(memberships)})")
        con.execute(f"CREATE TEMP VIEW crosswalk AS SELECT * FROM read_parquet({_path(crosswalk)})")
        _require(con, "classifications", CLASSIFICATION_REQUIRED, "classification")
        _require(con, "memberships", MEMBERSHIP_REQUIRED, "memberships")
        _require(con, "crosswalk", CROSSWALK_REQUIRED, "crosswalk")
        con.execute("""
            CREATE TEMP TABLE membership_nfl_raw AS
            SELECT
              CAST(m.db_name AS VARCHAR) AS db_name,
              CAST(m.year AS INTEGER) AS year,
              CAST(m.week AS INTEGER) AS week,
              NULLIF(TRIM(CAST(m.source_franchise_id AS VARCHAR)), '') AS team_key,
              NULLIF(TRIM(CAST(c.NFL_player_id AS VARCHAR)), '') AS NFL_player_id
            FROM memberships m
            JOIN crosswalk c
              ON CAST(c.source_year AS INTEGER)=CAST(m.source_year AS INTEGER)
             AND CAST(c.mfl_player_id AS VARCHAR)=CAST(m.mfl_player_id AS VARCHAR)
            WHERE NULLIF(TRIM(CAST(m.source_franchise_id AS VARCHAR)), '') IS NOT NULL
              AND NULLIF(TRIM(CAST(c.NFL_player_id AS VARCHAR)), '') IS NOT NULL
        """)
        con.execute("""
            CREATE TEMP TABLE membership_nfl AS
            SELECT db_name, year, week, NFL_player_id,
                   COUNT(DISTINCT team_key) AS team_count,
                   MIN(team_key) AS team_key
            FROM membership_nfl_raw
            GROUP BY 1,2,3,4
        """)
        con.execute("""
            CREATE TEMP TABLE classified_members AS
            SELECT
              CAST(c.db_name AS VARCHAR) AS db_name,
              CAST(c.year AS INTEGER) AS year,
              CAST(c.week AS INTEGER) AS week,
              CAST(c.NFL_player_id AS VARCHAR) AS NFL_player_id,
              m.team_count, m.team_key,
              TRY_CAST(c.source_win AS INTEGER) AS source_win,
              TRY_CAST(c.source_loss AS INTEGER) AS source_loss,
              TRY_CAST(c.source_tie AS INTEGER) AS source_tie,
              TRY_CAST(c.source_team_points AS DOUBLE) AS source_team_points,
              CASE
                WHEN LOWER(TRIM(CAST(c.source_is_playoffs AS VARCHAR))) IN ('1','true','t','yes','y') THEN 1
                WHEN LOWER(TRIM(CAST(c.source_is_playoffs AS VARCHAR))) IN ('0','false','f','no','n') THEN 0
                ELSE TRY_CAST(c.source_is_playoffs AS INTEGER)
              END AS source_is_playoffs
            FROM classifications c
            LEFT JOIN membership_nfl m
              ON m.db_name=CAST(c.db_name AS VARCHAR)
             AND m.year=CAST(c.year AS INTEGER)
             AND m.week=CAST(c.week AS INTEGER)
             AND m.NFL_player_id=CAST(c.NFL_player_id AS VARCHAR)
        """)
        variants = []
        values = []
        for source, (target, _) in FIELDS.items():
            variants.append(
                f"COUNT(DISTINCT {source}) FILTER (WHERE {source} IS NOT NULL) AS {source}_variants"
            )
            values.append(f"MAX({source}) FILTER (WHERE {source} IS NOT NULL) AS {target}")
        con.execute(f"""
            CREATE TEMP TABLE signals AS
            SELECT db_name, year, week, team_key,
                   {', '.join(variants)},
                   {', '.join(values)},
                   COUNT(*) AS classified_player_rows
            FROM classified_members
            WHERE team_count=1
            GROUP BY 1,2,3,4
        """)
        select_values = []
        for source, (target, _) in FIELDS.items():
            select_values.append(
                f"CASE WHEN {source}_variants<=1 THEN {target} ELSE NULL END AS {target}"
            )
        out.parent.mkdir(parents=True, exist_ok=True)
        con.execute(f"""
            COPY (
              SELECT db_name, year, week, team_key,
                     {', '.join(select_values)}
              FROM signals
              ORDER BY db_name, year, week, team_key
            ) TO {_path(out)} (FORMAT PARQUET, COMPRESSION ZSTD)
        """)
        result = {
            "read_only": True,
            "cache_mutated": False,
            "new_lineage": False,
            "classification_files": len(classifications),
            "classification_rows": int(con.execute("SELECT COUNT(*) FROM classifications").fetchone()[0]),
            "resolved_classification_rows": int(con.execute(
                "SELECT COUNT(*) FROM classified_members WHERE team_count=1"
            ).fetchone()[0]),
            "unresolved_classification_rows": int(con.execute(
                "SELECT COUNT(*) FROM classified_members WHERE team_count IS NULL OR team_count<>1"
            ).fetchone()[0]),
            "emitted_team_signals": int(con.execute("SELECT COUNT(*) FROM signals").fetchone()[0]),
            "field_conflicting_team_signals": {
                target: int(con.execute(
                    f"SELECT COUNT(*) FROM signals WHERE {source}_variants>1"
                ).fetchone()[0])
                for source, (target, _) in FIELDS.items()
            },
        }
    finally:
        con.close()
    report.parent.mkdir(parents=True, exist_ok=True)
    report.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return result


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--classification", type=Path, action="append", required=True)
    ap.add_argument("--memberships", type=Path, required=True)
    ap.add_argument("--crosswalk", type=Path, required=True)
    ap.add_argument("--out", type=Path, required=True)
    ap.add_argument("--report", type=Path, required=True)
    args = ap.parse_args()
    print(json.dumps(build(
        classifications=args.classification, memberships=args.memberships,
        crosswalk=args.crosswalk, out=args.out, report=args.report,
    ), sort_keys=True))


if __name__ == "__main__":
    main()
