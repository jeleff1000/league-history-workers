"""Build a read-only, exact-grain MFL duplicate-recipient candidate.

This is deliberately narrower than the general team-signal candidate builder.
It only considers cache player-week duplicates when the retained MFL roster
memberships prove an exact one-to-one multiset match on rostered/start/points.
The MFL source has pseudo-games against BYE; those never supply W/L values.
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


def _parquet_list(paths: list[Path]) -> str:
    if not paths:
        raise ValueError("at least one parquet path is required")
    return "[" + ",".join("'" + str(path.resolve()).replace("'", "''") + "'" for path in paths) + "]"


def build(
    *,
    base: Path,
    memberships: list[Path],
    crosswalks: list[Path],
    source: Path,
    out: Path,
) -> dict:
    """Emit only exact, source-backed duplicate-recipient rows; never mutate base."""
    con = duckdb.connect(str(base), read_only=True)
    player_columns = [row[0] for row in con.execute("DESCRIBE public.player_fantasy").fetchall()]
    validate_player_schema(player_columns)
    has_loss = "loss" in player_columns
    has_tie = "tie" in player_columns

    con.execute(
        "CREATE TEMP VIEW raw_memberships AS SELECT * FROM read_parquet("
        + _parquet_list(memberships)
        + ", union_by_name=true)"
    )
    con.execute(
        "CREATE TEMP VIEW raw_crosswalk AS SELECT * FROM read_parquet("
        + _parquet_list(crosswalks)
        + ", union_by_name=true)"
    )
    source_sql = "'" + str(source.resolve()).replace("'", "''") + "'"
    con.execute(f"CREATE TEMP VIEW raw_source AS SELECT * FROM read_parquet({source_sql})")
    source_columns = {row[0] for row in con.execute("DESCRIBE raw_source").fetchall()}
    opponent_franchise_expr = (
        "CAST(opponent_franchise_id AS VARCHAR)"
        if "opponent_franchise_id" in source_columns else "CAST(NULL AS VARCHAR)"
    )
    opponent_team_expr = (
        "CAST(opponent_team_key AS VARCHAR)"
        if "opponent_team_key" in source_columns else "CAST(NULL AS VARCHAR)"
    )

    # Canonical source cards are only ordinary opponent matchups.  MFL emits a
    # pseudo-game for a real franchise against a synthetic BYE franchise; it
    # must never turn an otherwise-opponentless team into a win or loss.
    con.execute("""
      CREATE TEMP TABLE actual_source_cards AS
      SELECT *
      FROM raw_source
      WHERE UPPER(TRIM(CAST(team_key AS VARCHAR))) <> 'BYE'
        AND NOT (
          UPPER(TRIM(COALESCE(""" + opponent_franchise_expr + """, ''))) LIKE '%_BYE'
          OR UPPER(TRIM(COALESCE(""" + opponent_team_expr + """, ''))) = 'BYE'
        )
    """)
    con.execute("""
      CREATE TEMP TABLE safe_source_cards AS
      SELECT
        CAST(db_name AS VARCHAR) AS db_name,
        CAST(year AS INTEGER) AS year,
        CAST(week AS INTEGER) AS week,
        CAST(team_key AS VARCHAR) AS source_team_key,
        MIN(CAST(manager AS VARCHAR)) AS source_manager,
        MIN(CAST(team_name AS VARCHAR)) AS source_team_name,
        MIN(TRY_CAST(win AS INTEGER)) AS source_win,
        MIN(TRY_CAST(loss AS INTEGER)) AS source_loss,
        MIN(TRY_CAST(tie AS INTEGER)) AS source_tie,
        MIN(TRY_CAST(team_points AS DOUBLE)) AS source_team_points,
        MIN(TRY_CAST(is_playoffs AS INTEGER)) AS source_is_playoffs,
        COUNT(*) AS actual_source_card_count,
        COUNT(DISTINCT CONCAT_WS('|',
          CAST(win AS VARCHAR), CAST(loss AS VARCHAR), CAST(tie AS VARCHAR),
          CAST(team_points AS VARCHAR), CAST(is_playoffs AS VARCHAR)
        )) AS source_signal_patterns
      FROM actual_source_cards
      GROUP BY 1,2,3,4
      HAVING COUNT(*)=1
         AND COUNT(DISTINCT CONCAT_WS('|',
          CAST(win AS VARCHAR), CAST(loss AS VARCHAR), CAST(tie AS VARCHAR),
          CAST(team_points AS VARCHAR), CAST(is_playoffs AS VARCHAR)
        ))=1
    """)
    con.execute("""
      CREATE TEMP TABLE source_memberships AS
      SELECT
        CAST(m.db_name AS VARCHAR) AS db_name,
        CAST(m.year AS INTEGER) AS year,
        CAST(m.week AS INTEGER) AS week,
        CAST(c.NFL_player_id AS VARCHAR) AS NFL_player_id,
        CAST(m.source_franchise_id AS VARCHAR) AS source_team_key,
        CAST(m.source_manager AS VARCHAR) AS membership_manager,
        CAST(m.mfl_player_id AS VARCHAR) AS source_mfl_player_id,
        TRY_CAST(m.is_started AS INTEGER) AS source_is_started,
        TRY_CAST(m.fantasy_points AS DOUBLE) AS source_fantasy_points
      FROM raw_memberships m
      JOIN (
        SELECT DISTINCT CAST(source_year AS INTEGER) AS source_year,
                        CAST(mfl_player_id AS VARCHAR) AS mfl_player_id,
                        CAST(NFL_player_id AS VARCHAR) AS NFL_player_id
        FROM raw_crosswalk
        WHERE NFL_player_id IS NOT NULL
      ) c ON c.source_year=CAST(m.source_year AS INTEGER)
         AND c.mfl_player_id=CAST(m.mfl_player_id AS VARCHAR)
    """)
    # Scope canonical access *before* materializing any cache rows.  These
    # bridge artifacts describe a few explicit MFL player-weeks; scanning the
    # entire 269M-row cache would turn an exact rescue into a broad rebuild.
    con.execute("""
      CREATE TEMP TABLE target_player_weeks AS
      SELECT DISTINCT db_name,year,week,NFL_player_id
      FROM source_memberships
    """)
    con.execute("""
      CREATE TEMP TABLE cache_rows AS
      SELECT
        p.rowid AS player_rowid,
        CAST(p.db_name AS VARCHAR) AS db_name,
        CAST(p.year AS INTEGER) AS year,
        CAST(p.week AS INTEGER) AS week,
        CAST(p.NFL_player_id AS VARCHAR) AS NFL_player_id,
        TRY_CAST(p.is_rostered AS INTEGER) AS canonical_is_rostered,
        TRY_CAST(p.is_started AS INTEGER) AS canonical_is_started,
        TRY_CAST(p.fantasy_points AS DOUBLE) AS canonical_fantasy_points,
        CAST(p.manager AS VARCHAR) AS canonical_manager,
        CAST(p.team_key AS VARCHAR) AS canonical_team_key,
        CAST(p.team_name AS VARCHAR) AS canonical_team_name,
        TRY_CAST(p.win AS INTEGER) AS canonical_win,
        TRY_CAST(p.team_points AS DOUBLE) AS canonical_team_points,
        TRY_CAST(p.is_playoffs AS INTEGER) AS canonical_is_playoffs,
        TRY_CAST(p.mfl_player_id AS VARCHAR) AS canonical_mfl_player_id,
        """ + ("TRY_CAST(p.loss AS INTEGER) AS canonical_loss," if has_loss else "CAST(NULL AS INTEGER) AS canonical_loss,") + """
        """ + ("TRY_CAST(p.tie AS INTEGER) AS canonical_tie" if has_tie else "CAST(NULL AS INTEGER) AS canonical_tie") + """
      FROM public.player_fantasy p
      JOIN target_player_weeks t
        ON t.db_name=CAST(p.db_name AS VARCHAR)
       AND t.year=CAST(p.year AS INTEGER)
       AND t.week=CAST(p.week AS INTEGER)
       AND t.NFL_player_id=CAST(p.NFL_player_id AS VARCHAR)
    """)
    con.execute("""
      CREATE TEMP TABLE cache_signature AS
      SELECT db_name,year,week,NFL_player_id,
             COALESCE(canonical_is_rostered,-1) AS is_rostered,
             COALESCE(canonical_is_started,-1) AS is_started,
             ROUND(COALESCE(canonical_fantasy_points,-999999.0),3) AS fantasy_points,
             COUNT(*) AS n
      FROM cache_rows
      GROUP BY ALL
    """)
    con.execute("""
      CREATE TEMP TABLE source_signature AS
      SELECT db_name,year,week,NFL_player_id,
             1 AS is_rostered,
             COALESCE(source_is_started,-1) AS is_started,
             ROUND(COALESCE(source_fantasy_points,-999999.0),3) AS fantasy_points,
             COUNT(*) AS n
      FROM source_memberships
      GROUP BY ALL
    """)
    con.execute("""
      CREATE TEMP TABLE exact_player_weeks AS
      WITH cache_totals AS (
        SELECT db_name,year,week,NFL_player_id,SUM(n) AS cache_n
        FROM cache_signature GROUP BY ALL
      ), source_totals AS (
        SELECT db_name,year,week,NFL_player_id,SUM(n) AS source_n
        FROM source_signature GROUP BY ALL
      ), signature_differences AS (
        SELECT COALESCE(c.db_name,s.db_name) AS db_name,
               COALESCE(c.year,s.year) AS year,
               COALESCE(c.week,s.week) AS week,
               COALESCE(c.NFL_player_id,s.NFL_player_id) AS NFL_player_id
        FROM cache_signature c FULL OUTER JOIN source_signature s
          USING (db_name,year,week,NFL_player_id,is_rostered,is_started,fantasy_points)
        WHERE c.n IS DISTINCT FROM s.n
      )
      SELECT c.db_name,c.year,c.week,c.NFL_player_id
      FROM cache_totals c JOIN source_totals s USING (db_name,year,week,NFL_player_id)
      WHERE c.cache_n=s.source_n
        AND c.cache_n > 1
        AND NOT EXISTS (
          SELECT 1 FROM signature_differences d
          WHERE d.db_name=c.db_name AND d.year=c.year AND d.week=c.week
            AND d.NFL_player_id=c.NFL_player_id
        )
    """)
    con.execute("""
      CREATE TEMP TABLE ranked_cache AS
      SELECT c.*,
        ROW_NUMBER() OVER (
          PARTITION BY c.db_name,c.year,c.week,c.NFL_player_id,
                       COALESCE(c.canonical_is_rostered,-1),COALESCE(c.canonical_is_started,-1),
                       ROUND(COALESCE(c.canonical_fantasy_points,-999999.0),3)
          ORDER BY c.player_rowid
        ) AS signature_ordinal
      FROM cache_rows c JOIN exact_player_weeks e USING (db_name,year,week,NFL_player_id)
    """)
    con.execute("""
      CREATE TEMP TABLE ranked_memberships AS
      SELECT m.*,
        ROW_NUMBER() OVER (
          PARTITION BY m.db_name,m.year,m.week,m.NFL_player_id,
                       COALESCE(m.source_is_started,-1),ROUND(COALESCE(m.source_fantasy_points,-999999.0),3)
          ORDER BY m.source_team_key,m.source_mfl_player_id
        ) AS signature_ordinal
      FROM source_memberships m JOIN exact_player_weeks e USING (db_name,year,week,NFL_player_id)
    """)

    out.mkdir(parents=True, exist_ok=True)
    con.execute("""
      COPY (
        SELECT
          c.player_rowid,c.db_name,c.year,c.week,c.NFL_player_id,
          c.canonical_manager,c.canonical_team_key,c.canonical_team_name,c.canonical_mfl_player_id,
          c.canonical_win,c.canonical_loss,c.canonical_tie,c.canonical_team_points,c.canonical_is_playoffs,
          m.source_team_key,COALESCE(s.source_manager,m.membership_manager) AS source_manager,
          s.source_team_name,m.source_mfl_player_id,
          s.source_win,s.source_loss,s.source_tie,s.source_team_points,s.source_is_playoffs
        FROM ranked_cache c
        JOIN ranked_memberships m
          ON m.db_name=c.db_name AND m.year=c.year AND m.week=c.week
         AND m.NFL_player_id=c.NFL_player_id
         AND COALESCE(m.source_is_started,-1)=COALESCE(c.canonical_is_started,-1)
         AND ROUND(COALESCE(m.source_fantasy_points,-999999.0),3)=ROUND(COALESCE(c.canonical_fantasy_points,-999999.0),3)
         AND m.signature_ordinal=c.signature_ordinal
        JOIN safe_source_cards s
          ON s.db_name=m.db_name AND s.year=m.year AND s.week=m.week
         AND s.source_team_key=m.source_team_key
      ) TO ? (FORMAT PARQUET)
    """, [str(out / "mfl_exact_duplicate_candidate.parquet")])
    candidate_rows = int(con.execute(
        "SELECT COUNT(*) FROM read_parquet(?)", [str(out / "mfl_exact_duplicate_candidate.parquet")]
    ).fetchone()[0])
    exact_keys = int(con.execute("SELECT COUNT(*) FROM exact_player_weeks").fetchone()[0])
    report = {
        "read_only": True,
        "cache_mutated": False,
        "new_lineage": False,
        "canonical_schema_unchanged": True,
        "exact_duplicate_player_weeks": exact_keys,
        "scoped_cache_rows": int(con.execute("SELECT COUNT(*) FROM cache_rows").fetchone()[0]),
        "candidate_rows": candidate_rows,
        "source_actual_cards": int(con.execute("SELECT COUNT(*) FROM actual_source_cards").fetchone()[0]),
        "safe_source_cards": int(con.execute("SELECT COUNT(*) FROM safe_source_cards").fetchone()[0]),
        "policy": "exact rostered/start/points multiplicity only; synthetic BYE outcomes excluded",
    }
    (out / "mfl_exact_duplicate_candidate_report.json").write_text(
        json.dumps(report, indent=2) + "\n", encoding="utf-8"
    )
    con.close()
    return report


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--base", type=Path, required=True)
    ap.add_argument("--memberships", type=Path, action="append", required=True)
    ap.add_argument("--crosswalk", type=Path, action="append", required=True)
    ap.add_argument("--source", type=Path, required=True)
    ap.add_argument("--out", type=Path, required=True)
    args = ap.parse_args()
    print(json.dumps(build(
        base=args.base, memberships=args.memberships, crosswalks=args.crosswalk,
        source=args.source, out=args.out,
    ), sort_keys=True))


if __name__ == "__main__":
    main()
