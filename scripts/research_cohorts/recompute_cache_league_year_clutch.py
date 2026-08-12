"""Recompute clutch on one cache league-year without changing its schema.

The cache has manager identity but no franchise-id column, so this intentionally
uses the exact manager names only after proving every manager-week carries at
most one non-null championship-odds value in ``public.matchup``.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import duckdb


def _q(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def recompute(*, base: Path, db_name: str, year: int, report: Path) -> dict[str, Any]:
    con = duckdb.connect(str(base))
    try:
        player_columns = {row[0] for row in con.execute("DESCRIBE public.player_fantasy").fetchall()}
        matchup_columns = {row[0] for row in con.execute("DESCRIBE public.matchup").fetchall()}
        required_player = {"db_name", "year", "week", "manager", "is_started", "manager_lamar", "position", "clutch_equity"}
        required_matchup = {"db_name", "year", "week", "manager", "p_champ"}
        if missing := sorted(required_player - player_columns):
            raise ValueError(f"player cache missing clutch inputs: {missing}")
        if missing := sorted(required_matchup - matchup_columns):
            raise ValueError(f"matchup cache missing clutch inputs: {missing}")
        ambiguous = con.execute("""
          SELECT manager, week, COUNT(DISTINCT p_champ) FILTER (WHERE p_champ IS NOT NULL) AS n
          FROM public.matchup
          WHERE db_name=? AND year=? AND manager IS NOT NULL
          GROUP BY manager, week
          HAVING COUNT(DISTINCT p_champ) FILTER (WHERE p_champ IS NOT NULL) > 1
          ORDER BY manager, week
        """, [db_name, year]).fetchall()
        if ambiguous:
            raise ValueError(f"ambiguous non-null p_champ values by manager/week: {ambiguous[:10]}")
        team_count = int(con.execute("""
          SELECT COUNT(DISTINCT manager)
          FROM public.matchup WHERE db_name=? AND year=? AND manager IS NOT NULL
        """, [db_name, year]).fetchone()[0])
        if team_count < 2:
            raise ValueError(f"invalid team count for clutch: {team_count}")
        before_nonzero = int(con.execute("""
          SELECT COUNT(*) FROM public.player_fantasy
          WHERE db_name=? AND year=? AND COALESCE(clutch_equity, 0) <> 0
        """, [db_name, year]).fetchone()[0])
        before_rows = int(con.execute("SELECT COUNT(*) FROM public.player_fantasy WHERE db_name=? AND year=?", [db_name, year]).fetchone()[0])
        con.execute("BEGIN")
        try:
            con.execute("""
              CREATE OR REPLACE TEMP TABLE clutch_update AS
              WITH manager_week AS (
                SELECT manager, week, MAX(p_champ) AS p_champ
                FROM public.matchup
                WHERE db_name=? AND year=? AND manager IS NOT NULL
                GROUP BY manager, week
              ), odds AS (
                SELECT manager, week, p_champ,
                  p_champ - COALESCE(
                    LAG(p_champ) OVER (PARTITION BY manager ORDER BY week),
                    100.0 / ?
                  ) AS odds_delta
                FROM manager_week
              ), starter_baseline AS (
                SELECT week, position, AVG(manager_lamar) AS baseline
                FROM public.player_fantasy
                WHERE db_name=? AND year=? AND CAST(is_started AS INTEGER)=1
                  AND manager_lamar IS NOT NULL AND position IS NOT NULL
                GROUP BY week, position
              ), scored AS (
                SELECT p.NFL_player_id, p.manager, p.week,
                  CAST(p.is_started AS INTEGER) AS is_started,
                  GREATEST(COALESCE(p.manager_lamar, 0) - COALESCE(b.baseline, 0), 0) AS above,
                  GREATEST(COALESCE(b.baseline, 0) - COALESCE(p.manager_lamar, 0), 0) AS below,
                  o.odds_delta
                FROM public.player_fantasy p
                LEFT JOIN starter_baseline b ON p.week=b.week AND p.position=b.position
                LEFT JOIN odds o ON p.manager=o.manager AND p.week=o.week
                WHERE p.db_name=? AND p.year=?
              ), totals AS (
                SELECT manager, week, SUM(above) AS total_above, SUM(below) AS total_below
                FROM scored WHERE is_started=1 GROUP BY manager, week
              )
              SELECT s.NFL_player_id, s.manager, s.week,
                CASE
                  WHEN s.is_started=1 AND s.odds_delta > 0 AND t.total_above > 0
                    THEN s.odds_delta * s.above / t.total_above
                  WHEN s.is_started=1 AND s.odds_delta < 0 AND t.total_below > 0
                    THEN s.odds_delta * s.below / t.total_below
                  ELSE 0.0
                END AS clutch_equity
              FROM scored s LEFT JOIN totals t USING (manager, week)
            """, [db_name, year, team_count, db_name, year, db_name, year])
            updates = int(con.execute("SELECT COUNT(*) FROM clutch_update").fetchone()[0])
            if updates != before_rows:
                raise ValueError(f"clutch candidate rows {updates} != cache league-year rows {before_rows}")
            con.execute("""
              UPDATE public.player_fantasy p SET clutch_equity=u.clutch_equity
              FROM clutch_update u
              WHERE p.db_name=? AND p.year=? AND p.NFL_player_id=u.NFL_player_id
                AND p.manager=u.manager AND p.week=u.week
            """, [db_name, year])
            after_rows = int(con.execute("SELECT COUNT(*) FROM public.player_fantasy WHERE db_name=? AND year=?", [db_name, year]).fetchone()[0])
            if after_rows != before_rows:
                raise ValueError("player row count changed during clutch recalculation")
            missing = int(con.execute("""
              SELECT COUNT(*) FROM public.player_fantasy
              WHERE db_name=? AND year=? AND clutch_equity IS NULL
            """, [db_name, year]).fetchone()[0])
            if missing:
                raise ValueError(f"clutch recalculation left {missing} null player rows")
            con.execute("COMMIT")
        except Exception:
            con.execute("ROLLBACK")
            raise
        after_nonzero = int(con.execute("""
          SELECT COUNT(*) FROM public.player_fantasy
          WHERE db_name=? AND year=? AND COALESCE(clutch_equity, 0) <> 0
        """, [db_name, year]).fetchone()[0])
        result = {
            "cache_mutated": True,
            "new_lineage": False,
            "schema_unchanged": True,
            "db_name": db_name,
            "year": year,
            "team_count": team_count,
            "player_rows": before_rows,
            "clutch_rows_recomputed": updates,
            "null_clutch_rows_after": 0,
            "nonzero_clutch_before": before_nonzero,
            "nonzero_clutch_after": after_nonzero,
            "odds_identity": "manager",
        }
        report.parent.mkdir(parents=True, exist_ok=True)
        report.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        return result
    finally:
        con.close()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base", type=Path, required=True)
    parser.add_argument("--db-name", required=True)
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--report", type=Path, required=True)
    args = parser.parse_args()
    print(json.dumps(recompute(base=args.base, db_name=args.db_name, year=args.year, report=args.report), sort_keys=True))


if __name__ == "__main__":
    main()
