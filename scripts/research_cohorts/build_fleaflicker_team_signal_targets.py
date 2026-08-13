"""Materialize current Fleaflicker team-week bridge targets without mutating a cache.

The research player table can contain a stable Fleaflicker player id while its
manager/team columns are blank.  Matchup signals are at team-week grain.  This
tool finds only source team-weeks where a roster-membership lookup can bridge
those two facts.  It is deliberately read-only: a later workflow must fetch
roster memberships, form an exact player delta, and independently read it back
before any cache mutation is allowed.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import duckdb


REQUIRED_PLAYER_COLUMNS = {
    "db_name", "year", "week", "is_playoffs", "champion", "platform",
    "manager", "team_key", "team_name", "fleaflicker_player_id",
}
REQUIRED_MATCHUP_COLUMNS = {
    "db_name", "year", "week", "team_key", "is_playoffs", "champion",
    "is_championship",
}


def _columns(con: duckdb.DuckDBPyConnection, relation: str) -> set[str]:
    return {str(row[0]) for row in con.execute(f"DESCRIBE {relation}").fetchall()}


def build(*, base: Path, out: Path, report: Path) -> dict[str, int]:
    con = duckdb.connect(str(base), read_only=True)
    try:
        player_columns = _columns(con, "public.player_fantasy")
        matchup_columns = _columns(con, "public.matchup")
        missing_player = sorted(REQUIRED_PLAYER_COLUMNS - player_columns)
        missing_matchup = sorted(REQUIRED_MATCHUP_COLUMNS - matchup_columns)
        if missing_player or missing_matchup:
            raise ValueError(
                "required canonical columns missing: "
                f"player={missing_player}, matchup={missing_matchup}"
            )

        # A championship winner is valid only in a matchup explicitly marked
        # as the championship game.  A season-level champion flag alone is
        # never allowed to become player championship credit.
        con.execute(
            """
            CREATE TEMP TABLE source_signals AS
            SELECT
              CAST(m.db_name AS VARCHAR) AS db_name,
              CAST(m.year AS INTEGER) AS year,
              CAST(m.week AS INTEGER) AS week,
              NULLIF(TRIM(CAST(m.team_key AS VARCHAR)), '') AS team_key,
              MAX(CAST(m.is_playoffs AS INTEGER))
                FILTER (WHERE m.is_playoffs IS NOT NULL) AS source_is_playoffs,
              MAX(CASE WHEN CAST(m.is_championship AS INTEGER)=1
                             AND CAST(m.champion AS INTEGER)=1 THEN 1 END)
                AS source_champion,
              MAX(CASE WHEN m.is_playoffs IS NOT NULL THEN 1 END) AS source_has_po_signal,
              COUNT(DISTINCT CAST(m.is_playoffs AS INTEGER))
                FILTER (WHERE m.is_playoffs IS NOT NULL) AS playoff_value_count,
              COUNT(DISTINCT CASE WHEN CAST(m.is_championship AS INTEGER)=1
                                       AND m.champion IS NOT NULL
                                  THEN CAST(m.champion AS INTEGER) END)
                AS championship_value_count
            FROM public.matchup m
            JOIN (
              SELECT DISTINCT CAST(db_name AS VARCHAR) AS db_name,
                              CAST(year AS INTEGER) AS year
              FROM public.player_fantasy
              WHERE LOWER(TRIM(CAST(platform AS VARCHAR)))='fleaflicker'
            ) ly
              ON ly.db_name=CAST(m.db_name AS VARCHAR)
             AND ly.year=CAST(m.year AS INTEGER)
            WHERE NULLIF(TRIM(CAST(m.team_key AS VARCHAR)), '') IS NOT NULL
            GROUP BY 1,2,3,4
            """
        )
        conflict_count = int(
            con.execute(
                """
                SELECT COUNT(*) FROM source_signals
                WHERE playoff_value_count > 1 OR championship_value_count > 1
                """
            ).fetchone()[0]
        )
        if conflict_count:
            raise ValueError(f"conflicting source team-week signals: {conflict_count}")

        # Team identity is missing on the cache player record.  The source
        # roster membership lookup supplies it through fleaflicker_player_id;
        # no manager or display-name fallback is admitted here.
        con.execute(
            """
            CREATE TEMP TABLE unbridged_player_weeks AS
            SELECT DISTINCT CAST(db_name AS VARCHAR) AS db_name,
                   CAST(year AS INTEGER) AS year,
                   CAST(week AS INTEGER) AS week,
                   NULLIF(TRIM(CAST(fleaflicker_player_id AS VARCHAR)), '')
                     AS fleaflicker_player_id
            FROM public.player_fantasy
            WHERE LOWER(TRIM(CAST(platform AS VARCHAR)))='fleaflicker'
              AND NULLIF(TRIM(CAST(fleaflicker_player_id AS VARCHAR)), '') IS NOT NULL
              AND NULLIF(TRIM(CAST(manager AS VARCHAR)), '') IS NULL
              AND NULLIF(TRIM(CAST(team_key AS VARCHAR)), '') IS NULL
              AND NULLIF(TRIM(CAST(team_name AS VARCHAR)), '') IS NULL
            """
        )
        con.execute(
            """
            CREATE TEMP TABLE candidates AS
            SELECT s.db_name, s.year, s.week, s.team_key,
                   REGEXP_EXTRACT(s.db_name, '^smpl_ffl_([0-9]+)$', 1) AS source_league_id,
                   s.source_is_playoffs, s.source_champion, s.source_has_po_signal,
                   COUNT(DISTINCT p.fleaflicker_player_id) AS unbridged_player_ids
            FROM source_signals s
            JOIN unbridged_player_weeks p
              ON p.db_name=s.db_name AND p.year=s.year AND p.week=s.week
            JOIN public.player_fantasy full_p
              ON CAST(full_p.db_name AS VARCHAR)=p.db_name
             AND CAST(full_p.year AS INTEGER)=p.year
             AND CAST(full_p.week AS INTEGER)=p.week
             AND NULLIF(TRIM(CAST(full_p.fleaflicker_player_id AS VARCHAR)), '')=p.fleaflicker_player_id
             AND LOWER(TRIM(CAST(full_p.platform AS VARCHAR)))='fleaflicker'
            WHERE (full_p.is_playoffs IS NULL AND s.source_is_playoffs IS NOT NULL)
               OR (full_p.champion IS NULL AND s.source_champion IS NOT NULL)
            GROUP BY 1,2,3,4,5,6,7,8
            """
        )
        out.parent.mkdir(parents=True, exist_ok=True)
        safe_out = str(out.resolve()).replace("'", "''")
        con.execute(
            f"COPY (SELECT * FROM candidates ORDER BY db_name,year,week,team_key) "
            f"TO '{safe_out}' (FORMAT PARQUET)"
        )
        counts = con.execute(
            """
            SELECT
              COUNT(*) AS target_team_weeks,
              COUNT(DISTINCT (db_name, year)) AS target_league_years,
              COUNT(DISTINCT (db_name, year, week)) AS target_league_weeks,
              COALESCE(SUM(unbridged_player_ids), 0) AS target_unbridged_player_ids,
              COUNT(*) FILTER (WHERE source_is_playoffs IS NOT NULL) AS playoff_signal_team_weeks,
              COUNT(*) FILTER (WHERE source_champion IS NOT NULL) AS championship_winner_team_weeks
            FROM candidates
            """
        ).fetchone()
        result = {
            "read_only": True,
            "cache_mutated": False,
            "new_lineage": False,
            "source_signal_conflicts": conflict_count,
            "target_team_weeks": int(counts[0]),
            "target_league_years": int(counts[1]),
            "target_league_weeks": int(counts[2]),
            "target_unbridged_player_ids": int(counts[3]),
            "playoff_signal_team_weeks": int(counts[4]),
            "championship_winner_team_weeks": int(counts[5]),
            "join_contract": "db_name|year|week|team_key -> fleaflicker_player_id -> existing cache player",
            "champion_contract": "is_championship=1 AND champion=1 only",
        }
        report.parent.mkdir(parents=True, exist_ok=True)
        report.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
        return result
    finally:
        con.close()


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--base", required=True, type=Path)
    ap.add_argument("--out", required=True, type=Path)
    ap.add_argument("--report", required=True, type=Path)
    args = ap.parse_args()
    print(json.dumps(build(base=args.base, out=args.out, report=args.report), sort_keys=True))


if __name__ == "__main__":
    main()
