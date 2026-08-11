"""Read-only audit of settings-to-player cohort fan-out.

The canonical player table has fixed materialized cohort columns.  Settings
fills therefore need an explicit player-row refresh; this audit proves the
exact rows and columns that are stale before any same-key cache replacement is
permitted.
"""
from __future__ import annotations

import argparse
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

DIMENSIONS = {
    "pass_td": {
        "base": "cohort_pass_td",
        "source_fields": ("source_scoring_pass_td",),
        "expected": "CASE WHEN scoring_pass_td >= 5 THEN '6pt' ELSE '4pt' END",
    },
    "playoff_teams": {
        "base": "cohort_playoff_teams",
        "source_fields": ("source_playoff_teams",),
        "expected": "CASE WHEN playoff_teams <= 5 THEN '4po' WHEN playoff_teams <= 7 THEN '6po' ELSE '8po' END",
    },
    "roster": {
        "base": "cohort_roster",
        "source_fields": ("source_roster_FLX", "source_roster_SUPER_FLEX", "source_roster_IDP"),
        "expected": """CASE
          WHEN COALESCE(roster_IDP, 0) + COALESCE(roster_DL, 0)
             + COALESCE(roster_LB, 0) + COALESCE(roster_DB, 0)
             + COALESCE(roster_DB_LB, 0) + COALESCE(roster_DL_LB, 0) > 0 THEN 'idp'
          WHEN COALESCE(roster_SUPER_FLEX, 0) > 0 THEN 'sflx'
          ELSE 'flx' END""",
        "dependent_columns": {
            "cohort_position_eligible": """CASE
              WHEN UPPER(TRIM(CAST(position AS VARCHAR))) IN ('QB','RB','WR','TE') THEN 1
              WHEN UPPER(TRIM(CAST(position AS VARCHAR))) IN ('K','PK')
                   AND COALESCE(roster_K, 0) > 0 THEN 1
              WHEN UPPER(TRIM(CAST(position AS VARCHAR))) IN ('DEF','DST','D/ST')
                   AND COALESCE(roster_DEF, 0) > 0 THEN 1
              WHEN UPPER(TRIM(CAST(position AS VARCHAR))) NOT IN ('QB','RB','WR','TE','K','PK','DEF','DST','D/ST')
                   AND COALESCE(roster_IDP, 0) + COALESCE(roster_DL, 0)
                     + COALESCE(roster_LB, 0) + COALESCE(roster_DB, 0)
                     + COALESCE(roster_DB_LB, 0) + COALESCE(roster_DL_LB, 0) > 0 THEN 1
              ELSE 0 END""",
        },
    },
    "best_ball": {
        "base": "cohort_best_ball",
        "source_fields": ("source_sleeper_best_ball",),
        "expected": "CASE WHEN CAST(sleeper_best_ball AS BOOLEAN) THEN 'best_ball' ELSE 'managed' END",
    },
}


def _q(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _count(con: duckdb.DuckDBPyConnection, sql: str) -> int:
    return int(con.execute(sql).fetchone()[0])


def _groups_for(base: str) -> list[str]:
    prefix = base + "_"
    return sorted(column for column in EXPECTED_COHORT_COLUMNS if column.startswith(prefix))


def audit(base: Path, delta: Path, *, include_legacy_groups: bool = True) -> dict[str, Any]:
    con = duckdb.connect(str(base), read_only=True)
    try:
        player_columns = [row[0] for row in con.execute("DESCRIBE public.player_fantasy").fetchall()]
        validate_player_schema(player_columns)
        settings_columns = {row[0] for row in con.execute("DESCRIBE public.league_settings").fetchall()}
        missing_settings = {
            "db_name", "year", "scoring_pass_td", "playoff_teams", "roster_FLX",
            "roster_SUPER_FLEX", "roster_IDP", "roster_DL", "roster_LB", "roster_DB",
            "roster_DB_LB", "roster_DL_LB", "sleeper_best_ball",
        } - settings_columns
        if missing_settings:
            raise SystemExit(f"league_settings missing fan-out inputs: {sorted(missing_settings)}")

        delta_path = str(delta.resolve()).replace("'", "''")
        con.execute(f"CREATE OR REPLACE TEMP VIEW settings_delta AS SELECT * FROM read_parquet('{delta_path}')")
        delta_columns = {row[0] for row in con.execute("DESCRIBE settings_delta").fetchall()}
        missing_delta = {"db_name", "year"} - delta_columns
        if missing_delta:
            raise SystemExit(f"settings delta missing key columns: {sorted(missing_delta)}")
        source_fields = [field for field in SOURCE_FIELDS if field in delta_columns]
        if not source_fields:
            raise SystemExit("settings delta has no supported source fields")
        duplicate_keys = _count(con, """
          SELECT COUNT(*) FROM (
            SELECT db_name, CAST(year AS INTEGER) AS year
            FROM settings_delta GROUP BY 1, 2 HAVING COUNT(*) > 1
          )
        """)
        if duplicate_keys:
            raise SystemExit(f"settings delta duplicate keys: {duplicate_keys}")
        changed_terms = " OR ".join(f"d.{_q(field)} IS NOT NULL" for field in source_fields)
        unmatched_keys = _count(con, f"""
          SELECT COUNT(*) FROM settings_delta d
          WHERE ({changed_terms}) AND NOT EXISTS (
            SELECT 1 FROM public.league_settings s
            WHERE s.db_name=d.db_name AND CAST(s.year AS INTEGER)=CAST(d.year AS INTEGER)
          )
        """)
        if unmatched_keys:
            raise SystemExit(f"settings delta keys absent from canonical settings: {unmatched_keys}")
        con.execute(f"""
          CREATE OR REPLACE TEMP VIEW affected_settings AS
          SELECT s.*
          FROM public.league_settings s JOIN settings_delta d
            ON s.db_name=d.db_name AND CAST(s.year AS INTEGER)=CAST(d.year AS INTEGER)
          WHERE {changed_terms}
        """)
        affected_keys = _count(con, "SELECT COUNT(*) FROM affected_settings")
        con.execute("""
          CREATE OR REPLACE TEMP VIEW affected_players AS
          SELECT p.*, s.* EXCLUDE (db_name, year)
          FROM public.player_fantasy p JOIN affected_settings s
            ON s.db_name=p.db_name AND CAST(s.year AS INTEGER)=CAST(p.year AS INTEGER)
        """)

        report: dict[str, Any] = {
            "cache_mutated": False,
            "new_lineage": False,
            "schema_unchanged": True,
            "player_schema_columns": len(player_columns),
            "affected_setting_keys": affected_keys,
            "affected_player_rows": _count(con, "SELECT COUNT(*) FROM affected_players"),
            "dimensions": {},
        }
        for name, spec in DIMENSIONS.items():
            relevant_sources = [field for field in spec["source_fields"] if field in delta_columns]
            if not relevant_sources:
                continue
            source_changed = " OR ".join(f"d.{_q(field)} IS NOT NULL" for field in relevant_sources)
            con.execute(f"""
              CREATE OR REPLACE TEMP VIEW {name}_players AS
              SELECT p.*, s.* EXCLUDE (db_name, year)
              FROM public.player_fantasy p
              JOIN public.league_settings s
                ON s.db_name=p.db_name AND CAST(s.year AS INTEGER)=CAST(p.year AS INTEGER)
              JOIN settings_delta d
                ON d.db_name=s.db_name AND CAST(d.year AS INTEGER)=CAST(s.year AS INTEGER)
              WHERE {source_changed}
            """)
            base = spec["base"]
            expected = spec["expected"]
            groups: dict[str, dict[str, int]] = {}
            if include_legacy_groups:
                for group in _groups_for(base):
                    groups[group] = {
                        "same_as_base": _count(con, f"SELECT COUNT(*) FROM {name}_players WHERE {_q(group)} IS NOT DISTINCT FROM {_q(base)}"),
                        "different_from_base": _count(con, f"SELECT COUNT(*) FROM {name}_players WHERE {_q(group)} IS DISTINCT FROM {_q(base)}"),
                        "matches_expected": _count(con, f"SELECT COUNT(*) FROM {name}_players WHERE {_q(group)} IS NOT DISTINCT FROM ({expected})"),
                        "mismatches_expected": _count(con, f"SELECT COUNT(*) FROM {name}_players WHERE {_q(group)} IS DISTINCT FROM ({expected})"),
                    }
            labels = {
                str(value): int(count)
                for value, count in con.execute(
                    f"SELECT ({expected}) AS value, COUNT(*) FROM {name}_players GROUP BY 1 ORDER BY 1"
                ).fetchall()
            }
            dimension_report = {
                "base_column": base,
                "player_rows": _count(con, f"SELECT COUNT(*) FROM {name}_players"),
                "base_matches": _count(con, f"SELECT COUNT(*) FROM {name}_players WHERE {_q(base)} IS NOT DISTINCT FROM ({expected})"),
                "base_mismatches": _count(con, f"SELECT COUNT(*) FROM {name}_players WHERE {_q(base)} IS DISTINCT FROM ({expected})"),
                "expected_label_counts": labels,
            }
            if include_legacy_groups:
                dimension_report["groups"] = groups
            report["dimensions"][name] = dimension_report
            dependent_columns = spec.get("dependent_columns", {})
            if dependent_columns:
                dependent_report: dict[str, dict[str, Any]] = {}
                for dependent_base, dependent_expected in dependent_columns.items():
                    dependent_entry: dict[str, Any] = {
                        "base_matches": _count(con, f"SELECT COUNT(*) FROM {name}_players WHERE {_q(dependent_base)} IS NOT DISTINCT FROM ({dependent_expected})"),
                        "base_mismatches": _count(con, f"SELECT COUNT(*) FROM {name}_players WHERE {_q(dependent_base)} IS DISTINCT FROM ({dependent_expected})"),
                    }
                    if include_legacy_groups:
                        dependent_groups: dict[str, dict[str, Any]] = {}
                        groups = _groups_for(dependent_base)
                        structs = ", ".join(
                            f"struct_pack(group_name := '{group}', value := COALESCE(CAST({_q(group)} AS VARCHAR), '<NULL>'))"
                            for group in groups
                        )
                        # One scan of the affected roster rows replaces 13 x 5 repeated
                        # scans.  Include base/expected in the grouping so Python can derive
                        # every equality count without reading the cache again.
                        rows = con.execute(f"""
                          SELECT item.group_name,
                                 COALESCE(NULLIF(TRIM(CAST(position AS VARCHAR)), ''), '<NULL>') AS position,
                                 item.value,
                                 COALESCE(CAST({_q(dependent_base)} AS VARCHAR), '<NULL>') AS base_value,
                                 COALESCE(CAST(({dependent_expected}) AS VARCHAR), '<NULL>') AS expected_value,
                                 COUNT(*) AS n
                          FROM {name}_players
                          CROSS JOIN UNNEST([{structs}]) AS u(item)
                          GROUP BY 1, 2, 3, 4, 5
                        """).fetchall()
                        for group in groups:
                            dependent_groups[group] = {
                                "same_as_base": 0,
                                "different_from_base": 0,
                                "matches_expected": 0,
                                "mismatches_expected": 0,
                                "position_value_counts": {},
                            }
                        for group, position, value, base_value, expected_value, count in rows:
                            entry = dependent_groups[str(group)]
                            n = int(count)
                            entry["same_as_base"] += n if value == base_value else 0
                            entry["different_from_base"] += n if value != base_value else 0
                            entry["matches_expected"] += n if value == expected_value else 0
                            entry["mismatches_expected"] += n if value != expected_value else 0
                            entry["position_value_counts"].setdefault(str(position), {})[str(value)] = n
                        dependent_entry["groups"] = dependent_groups
                    dependent_report[dependent_base] = dependent_entry
                report["dimensions"][name]["dependent_columns"] = dependent_report
        return report
    finally:
        con.close()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base", type=Path, required=True)
    parser.add_argument("--settings-delta", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--base-only", action="store_true")
    args = parser.parse_args()
    result = audit(args.base, args.settings_delta, include_legacy_groups=not args.base_only)
    args.out.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    main()
