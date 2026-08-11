"""Refresh affected materialized player cohorts from canonical league settings.

This is a same-schema, NULL-fill follow-up: the settings values already exist in
``league_settings`` and this writes their deterministic row-level fan-out onto
the existing player rows.  It never inserts or deletes player rows.
"""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from typing import Any

import duckdb

from canonical_player_schema import EXPECTED_COHORT_COLUMNS, validate_player_schema


SOURCE_FIELDS = (
    "source_scoring_pass_td",
    "source_playoff_teams",
    "source_roster_FLX",
    "source_roster_SUPER_FLEX",
    "source_roster_IDP",
    "source_sleeper_best_ball",
)


def _q(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _groups_for(base: str) -> list[str]:
    """Return the one approved materialized truth column for a dimension.

    The cache still contains legacy ``_1``…``_12`` parallel fields.  Their
    values are not stable copies of the base field (especially eligibility),
    and no active builder reads them.  A settings fill may update only the
    single base truth column; changing the legacy fields would reintroduce the
    exact parallel-field drift this cache contract forbids.
    """
    if base not in EXPECTED_COHORT_COLUMNS:
        raise ValueError(f"unknown cohort truth column: {base}")
    return [base]


def _count(con: duckdb.DuckDBPyConnection, sql: str) -> int:
    return int(con.execute(sql).fetchone()[0])


def _dimension_specs() -> dict[str, dict[str, Any]]:
    roster_expr = """CASE
      WHEN COALESCE(s.roster_IDP, 0) + COALESCE(s.roster_DL, 0)
         + COALESCE(s.roster_LB, 0) + COALESCE(s.roster_DB, 0)
         + COALESCE(s.roster_DB_LB, 0) + COALESCE(s.roster_DL_LB, 0) > 0 THEN 'idp'
      WHEN COALESCE(s.roster_SUPER_FLEX, 0) > 0 THEN 'sflx'
      ELSE 'flx' END"""
    eligibility_expr = """CAST(CASE
      WHEN UPPER(TRIM(CAST(p.position AS VARCHAR))) IN ('QB','RB','WR','TE') THEN 1
      WHEN UPPER(TRIM(CAST(p.position AS VARCHAR))) IN ('K','PK')
           AND COALESCE(s.roster_K, 0) > 0 THEN 1
      WHEN UPPER(TRIM(CAST(p.position AS VARCHAR))) IN ('DEF','DST','D/ST')
           AND COALESCE(s.roster_DEF, 0) > 0 THEN 1
      WHEN UPPER(TRIM(CAST(p.position AS VARCHAR))) NOT IN ('QB','RB','WR','TE','K','PK','DEF','DST','D/ST')
           AND COALESCE(s.roster_IDP, 0) + COALESCE(s.roster_DL, 0)
             + COALESCE(s.roster_LB, 0) + COALESCE(s.roster_DB, 0)
             + COALESCE(s.roster_DB_LB, 0) + COALESCE(s.roster_DL_LB, 0) > 0 THEN 1
      ELSE 0 END AS VARCHAR)"""
    return {
        "pass_td": {
            "source_fields": ("source_scoring_pass_td",),
            "columns": _groups_for("cohort_pass_td"),
            "expected": "CASE WHEN s.scoring_pass_td >= 5 THEN '6pt' ELSE '4pt' END",
        },
        "playoff_teams": {
            "source_fields": ("source_playoff_teams",),
            "columns": _groups_for("cohort_playoff_teams"),
            "expected": "CASE WHEN s.playoff_teams <= 5 THEN '4po' WHEN s.playoff_teams <= 7 THEN '6po' ELSE '8po' END",
        },
        "roster": {
            "source_fields": ("source_roster_FLX", "source_roster_SUPER_FLEX", "source_roster_IDP"),
            "columns": _groups_for("cohort_roster"),
            "expected": roster_expr,
        },
        "position_eligible": {
            "source_fields": ("source_roster_FLX", "source_roster_SUPER_FLEX", "source_roster_IDP"),
            "columns": _groups_for("cohort_position_eligible"),
            "expected": eligibility_expr,
        },
        "best_ball": {
            "source_fields": ("source_sleeper_best_ball",),
            "columns": _groups_for("cohort_best_ball"),
            "expected": "CASE WHEN CAST(s.sleeper_best_ball AS BOOLEAN) THEN 'best_ball' ELSE 'managed' END",
        },
    }


def apply(base: Path, ops: Path, delta: Path) -> dict[str, Any]:
    before_ops = _sha256(ops)
    con = duckdb.connect(str(base))
    try:
        player_columns = [row[0] for row in con.execute("DESCRIBE public.player_fantasy").fetchall()]
        validate_player_schema(player_columns)
        settings_columns = {row[0] for row in con.execute("DESCRIBE public.league_settings").fetchall()}
        required_settings = {
            "db_name", "year", "scoring_pass_td", "playoff_teams", "roster_FLX",
            "roster_SUPER_FLEX", "roster_IDP", "roster_DL", "roster_LB", "roster_DB",
            "roster_DB_LB", "roster_DL_LB", "roster_K", "roster_DEF", "sleeper_best_ball",
        }
        missing_settings = required_settings - settings_columns
        if missing_settings:
            raise SystemExit(f"league_settings missing fan-out inputs: {sorted(missing_settings)}")
        before_rows = _count(con, "SELECT COUNT(*) FROM public.player_fantasy")
        delta_path = str(delta.resolve()).replace("'", "''")
        con.execute(f"CREATE OR REPLACE TEMP VIEW settings_delta AS SELECT * FROM read_parquet('{delta_path}')")
        delta_columns = {row[0] for row in con.execute("DESCRIBE settings_delta").fetchall()}
        missing_delta = {"db_name", "year", *SOURCE_FIELDS} - delta_columns
        if missing_delta:
            raise SystemExit(f"settings delta missing required columns: {sorted(missing_delta)}")
        duplicate_keys = _count(con, """
          SELECT COUNT(*) FROM (
            SELECT db_name, CAST(year AS INTEGER) AS year
            FROM settings_delta GROUP BY 1, 2 HAVING COUNT(*) > 1
          )
        """)
        if duplicate_keys:
            raise SystemExit(f"settings delta duplicate keys: {duplicate_keys}")
        unmatched_settings = _count(con, """
          SELECT COUNT(*) FROM settings_delta d
          WHERE NOT EXISTS (
            SELECT 1 FROM public.league_settings s
            WHERE s.db_name=d.db_name AND CAST(s.year AS INTEGER)=CAST(d.year AS INTEGER)
          )
        """)
        if unmatched_settings:
            raise SystemExit(f"settings delta keys absent from canonical settings: {unmatched_settings}")

        report: dict[str, Any] = {
            "working_copy_only": True,
            "canonical_cache_replaced": False,
            "new_lineage": False,
            "schema_unchanged": False,
            "rows_unchanged": False,
            "ops_unchanged": False,
            "updates": {},
            "affected_rows": {},
            "pre_mismatches": {},
            "readback_remaining_mismatches": {},
        }
        con.execute("BEGIN")
        try:
            for name, spec in _dimension_specs().items():
                changed = " OR ".join(f"d.{_q(field)} IS NOT NULL" for field in spec["source_fields"])
                expected = spec["expected"]
                columns = spec["columns"]
                mismatch = " OR ".join(f"p.{_q(column)} IS DISTINCT FROM ({expected})" for column in columns)
                joins = f"""
                  FROM public.player_fantasy p
                  JOIN public.league_settings s
                    ON s.db_name=p.db_name AND CAST(s.year AS INTEGER)=CAST(p.year AS INTEGER)
                  JOIN settings_delta d
                    ON d.db_name=s.db_name AND CAST(d.year AS INTEGER)=CAST(s.year AS INTEGER)
                """
                where = f"WHERE ({changed})"
                update_scope = f"""
                  FROM public.league_settings s
                  JOIN settings_delta d ON d.db_name=s.db_name
                    AND CAST(d.year AS INTEGER)=CAST(s.year AS INTEGER)
                  WHERE p.db_name=s.db_name AND CAST(p.year AS INTEGER)=CAST(s.year AS INTEGER)
                    AND ({changed})
                """
                report["affected_rows"][name] = _count(con, f"SELECT COUNT(*) {joins} {where}")
                report["pre_mismatches"][name] = _count(con, f"SELECT COUNT(*) {joins} {where} AND ({mismatch})")
                assignments = ", ".join(f"{_q(column)}=({expected})" for column in columns)
                con.execute(f"UPDATE public.player_fantasy p SET {assignments} {update_scope} AND ({mismatch})")
                report["updates"][name] = len(columns)
                report["readback_remaining_mismatches"][name] = _count(
                    con, f"SELECT COUNT(*) {joins} {where} AND ({mismatch})"
                )
            if any(value != 0 for value in report["readback_remaining_mismatches"].values()):
                raise SystemExit(f"cohort fan-out readback mismatch: {report['readback_remaining_mismatches']}")
            con.execute("COMMIT")
        except Exception:
            con.execute("ROLLBACK")
            raise

        after_columns = [row[0] for row in con.execute("DESCRIBE public.player_fantasy").fetchall()]
        after_rows = _count(con, "SELECT COUNT(*) FROM public.player_fantasy")
        report["schema_unchanged"] = after_columns == player_columns
        report["rows_unchanged"] = after_rows == before_rows
        report["ops_unchanged"] = _sha256(ops) == before_ops
        if not (report["schema_unchanged"] and report["rows_unchanged"] and report["ops_unchanged"]):
            raise SystemExit(f"cohort fan-out preservation invariant failed: {report}")
        return report
    finally:
        con.close()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base", type=Path, required=True)
    parser.add_argument("--ops", type=Path, required=True)
    parser.add_argument("--settings-delta", type=Path, required=True)
    parser.add_argument("--report", type=Path, required=True)
    args = parser.parse_args()
    result = apply(args.base, args.ops, args.settings_delta)
    args.report.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    main()
