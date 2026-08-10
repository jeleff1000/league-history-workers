"""Read-only audit of team/week artifacts against the frozen player table.

The source grain is one team-week; the canonical player table is many rows per
team-week.  This script deliberately fans source signals out to existing player
rows, never inserts player rows, never changes schema, and never writes the lake.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import duckdb

try:
    from .canonical_player_schema import BASE_PLAYER_COLUMNS, OPTIONAL_PLAYER_COLUMNS, validate_player_schema
except ImportError:  # direct script execution in Actions
    from canonical_player_schema import BASE_PLAYER_COLUMNS, OPTIONAL_PLAYER_COLUMNS, validate_player_schema




def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--base", type=Path, required=True)
    ap.add_argument("--candidates", type=Path, required=True)
    ap.add_argument("--out", type=Path, required=True)
    args = ap.parse_args()

    con = duckdb.connect(str(args.base), read_only=True)
    pcols = [r[0] for r in con.execute("DESCRIBE public.player_fantasy").fetchall()]
    cohort_columns, optional = validate_player_schema(pcols)
    canonical_loss = "p.loss" if "loss" in pcols else "CAST(NULL AS INTEGER)"
    canonical_tie = "p.tie" if "tie" in pcols else "CAST(NULL AS INTEGER)"
    all_files = sorted(args.candidates.rglob("*.parquet"))
    files = []
    for path in all_files:
        parquet_path = "'" + str(path.resolve()).replace("'", "''") + "'"
        cols = {r[0] for r in con.execute(f"DESCRIBE SELECT * FROM read_parquet({parquet_path})").fetchall()}
        # Exact/player-grain artifacts are audited by the separate player pass;
        # this pass must consume only team/week-grain sources.
        if {"db_name", "year", "week", "team_key"}.issubset(cols) and "NFL_player_id" not in cols:
            files.append(path)
    if not files:
        raise SystemExit("no team candidate parquet files")

    paths = [str(p.resolve()).replace("'", "''") for p in files]
    con.execute(
        "CREATE OR REPLACE TEMP VIEW source_raw AS "
        "SELECT * FROM read_parquet([" + ",".join("'" + p + "'" for p in paths) + "], union_by_name=true, filename=true)"
    )
    cols = {r[0] for r in con.execute("DESCRIBE source_raw").fetchall()}
    required = {"db_name", "year", "week", "team_key"}
    missing = required - cols
    if missing:
        raise SystemExit(f"team candidate missing required columns: {sorted(missing)}")

    # Normalize the source to the only signals that can safely fill canonical
    # player cells.  Championship credit requires the explicit championship
    # marker; a playoff row alone never becomes a championship.
    def expr(col: str, typ: str = "VARCHAR") -> str:
        return f"TRY_CAST({col} AS {typ})" if col in cols else f"CAST(NULL AS {typ})"

    def flag_expr(col: str) -> str:
        if col not in cols:
            return "CAST(NULL AS INTEGER)"
        raw = f"LOWER(TRIM(CAST({col} AS VARCHAR)))"
        return f"CASE WHEN {raw} IN ('1','true','t','yes','y') THEN 1 WHEN {raw} IN ('0','false','f','no','n') THEN 0 ELSE TRY_CAST({col} AS INTEGER) END"

    con.execute(f"""
      CREATE OR REPLACE TEMP TABLE source_signals AS
      SELECT
        CAST(db_name AS VARCHAR) db_name,
        CAST("year" AS INTEGER) AS year,
        CAST("week" AS INTEGER) AS week,
        NULLIF(TRIM(CAST(team_key AS VARCHAR)), '') team_key,
        NULLIF(LOWER(TRIM(CAST(manager AS VARCHAR))), '') manager_key,
        NULLIF(LOWER(TRIM(CAST(team_name AS VARCHAR))), '') team_name_key,
        MAX({flag_expr('win')}) FILTER (WHERE {flag_expr('win')} IS NOT NULL) source_win,
        MAX({flag_expr('loss')}) FILTER (WHERE {flag_expr('loss')} IS NOT NULL) source_loss,
        MAX({flag_expr('tie')}) FILTER (WHERE {flag_expr('tie')} IS NOT NULL) source_tie,
        MAX({expr('team_points','DOUBLE')}) FILTER (WHERE {expr('team_points','DOUBLE')} IS NOT NULL) source_team_points,
        {(
            f"MAX({flag_expr('is_playoffs')}) FILTER (WHERE {flag_expr('is_playoffs')} IS NOT NULL)"
            if 'is_playoffs' in cols else
            "CAST(NULL AS INTEGER)"
        )} source_playoffs,
        {(
            f"MAX(CASE WHEN {flag_expr('is_championship')}=1 AND {flag_expr('champion')}=1 THEN 1 ELSE 0 END)"
            if 'is_championship' in cols and 'champion' in cols else
            "CAST(NULL AS INTEGER)"
        )} source_champion,
        MAX({expr('final_playoff_seed','INTEGER')}) FILTER (WHERE {expr('final_playoff_seed','INTEGER')} IS NOT NULL) source_final_playoff_seed,
        STRING_AGG(DISTINCT CAST(filename AS VARCHAR), '|') source_files,
        COUNT(*) source_rows
      FROM source_raw
      GROUP BY 1,2,3,4,5,6
    """)
    source_keys = con.execute("SELECT COUNT(*) FROM source_signals").fetchone()[0]
    source_duplicate_rows = con.execute("SELECT COALESCE(SUM(source_rows-1),0) FROM source_signals").fetchone()[0]

    # A team-key match is authoritative when it exists in the canonical table.
    # Some source APIs emit a different representation of the same team key
    # (notably MFL's short franchise number).  In that case the scoped
    # manager/team-name identity is the next valid key, but only when it maps
    # to exactly one source team signal for that canonical player row.
    delta_filters = [
        "(p.win IS NULL AND m.source_win IS NOT NULL)",
        "(p.team_points IS NULL AND m.source_team_points IS NOT NULL)",
        "(p.is_playoffs IS NULL AND m.source_playoffs=1)",
        "(p.champion IS NULL AND m.source_champion=1)",
        "(p.final_playoff_seed IS NULL AND m.source_final_playoff_seed IS NOT NULL)",
        "(p.made_playoffs IS NULL AND m.source_final_playoff_seed IS NOT NULL)",
    ]
    if "loss" in pcols:
        delta_filters.insert(1, "(p.loss IS NULL AND m.source_loss IS NOT NULL)")
    if "tie" in pcols:
        delta_filters.insert(2 if "loss" in pcols else 1, "(p.tie IS NULL AND m.source_tie IS NOT NULL)")

    con.execute("""
      CREATE OR REPLACE TEMP TABLE player_match_candidates AS
      SELECT
        p.rowid AS player_rowid,
        s.*,
        CASE
          WHEN s.team_key IS NOT NULL
           AND s.team_key=NULLIF(TRIM(CAST(p.team_key AS VARCHAR)),'')
            THEN 'team_key'
          ELSE 'manager_team'
        END AS join_method,
        COUNT(*) OVER (PARTITION BY p.rowid) AS candidate_count,
        MAX(CASE WHEN s.team_key IS NOT NULL
                       AND s.team_key=NULLIF(TRIM(CAST(p.team_key AS VARCHAR)),'')
                 THEN 1 ELSE 0 END)
          OVER (PARTITION BY p.rowid) AS has_exact_key
      FROM public.player_fantasy p
      JOIN source_signals s
        ON s.db_name=p.db_name
       AND s.year=CAST(p.year AS INTEGER)
       AND s.week=CAST(p.week AS INTEGER)
       AND (
         (s.team_key IS NOT NULL
          AND s.team_key=NULLIF(TRIM(CAST(p.team_key AS VARCHAR)),''))
         OR
         (s.manager_key IS NOT NULL
          AND s.team_name_key IS NOT NULL
          AND s.manager_key=LOWER(NULLIF(TRIM(CAST(p.manager AS VARCHAR)),''))
          AND s.team_name_key=LOWER(NULLIF(TRIM(CAST(p.team_name AS VARCHAR)),'')))
       )
    """)
    con.execute("""
      CREATE OR REPLACE TEMP TABLE player_matches AS
      SELECT * EXCLUDE (candidate_count, has_exact_key)
      FROM player_match_candidates
      WHERE has_exact_key=1 OR candidate_count=1
    """)
    matched = con.execute("SELECT COUNT(*) FROM player_matches").fetchone()[0]

    fields = {
        "win": "source_win",
        "loss": "source_loss",
        "tie": "source_tie",
        "team_points": "source_team_points",
        "is_playoffs": "source_playoffs",
        "champion": "source_champion",
        "final_playoff_seed": "source_final_playoff_seed",
        "made_playoffs": "source_final_playoff_seed",
    }
    args.out.mkdir(parents=True, exist_ok=True)
    improvements = {}
    positive_mismatches = {}
    for field, src in fields.items():
        if field not in pcols:
            continue
        improvements[field] = con.execute(f"""
          SELECT COUNT(*) FROM public.player_fantasy p JOIN player_matches m ON p.rowid=m.player_rowid
          WHERE p.{field} IS NULL AND m.{src} IS NOT NULL
        """).fetchone()[0]
        positive_mismatches[field] = con.execute(f"""
          SELECT COUNT(*) FROM public.player_fantasy p JOIN player_matches m ON p.rowid=m.player_rowid
          WHERE CAST(p.{field} AS VARCHAR)='0' AND CAST(m.{src} AS VARCHAR)='1'
        """).fetchone()[0]

    con.execute(f"""
      COPY (
        SELECT p.db_name,p.year,p.week,p.NFL_player_id,p.manager,p.team_key,p.team_name,
               p.platform,p.win canonical_win,m.source_win,{canonical_loss} canonical_loss,m.source_loss,
               {canonical_tie} canonical_tie,m.source_tie,p.team_points canonical_team_points,
               m.source_team_points,p.is_playoffs canonical_is_playoffs,m.source_playoffs,
               p.champion canonical_champion,m.source_champion,
               p.final_playoff_seed canonical_final_playoff_seed,m.source_final_playoff_seed,
               p.made_playoffs canonical_made_playoffs,
               CASE WHEN m.source_final_playoff_seed IS NOT NULL THEN 1 ELSE NULL END source_made_playoffs,
               m.source_files,m.join_method
        FROM public.player_fantasy p JOIN player_matches m ON p.rowid=m.player_rowid
        WHERE {" OR ".join(delta_filters)}
      ) TO ? (FORMAT PARQUET)
    """, [str(args.out / "team_promotable_player_delta.parquet")])
    report = {
        "read_only": True,
        "cache_mutated": False,
        "new_lineage": False,
        "canonical_schema_unchanged": True,
        "candidate_files": len(files),
        "source_rows": int(con.execute("SELECT COUNT(*) FROM source_raw").fetchone()[0]),
        "source_team_keys": int(source_keys),
        "duplicate_source_rows_collapsed": int(source_duplicate_rows),
        "matched_player_rows": int(matched),
        "unmatched_source_team_keys": int(con.execute("""
          SELECT COUNT(*) FROM source_signals s
          WHERE NOT EXISTS (SELECT 1 FROM player_matches m
                            JOIN public.player_fantasy p ON p.rowid=m.player_rowid
                            WHERE p.db_name=s.db_name AND CAST(p.year AS INTEGER)=s.year
                              AND CAST(p.week AS INTEGER)=s.week
                              AND ((s.team_key IS NOT NULL AND s.team_key=NULLIF(TRIM(CAST(p.team_key AS VARCHAR)),''))
                                OR (s.manager_key IS NOT NULL AND s.team_name_key IS NOT NULL
                                    AND s.manager_key=LOWER(NULLIF(TRIM(CAST(p.manager AS VARCHAR)),''))
                                    AND s.team_name_key=LOWER(NULLIF(TRIM(CAST(p.team_name AS VARCHAR)),'')))))
        """).fetchone()[0]),
        "improvements_by_field": {k: int(v) for k,v in improvements.items()},
        "positive_mismatches_not_auto_promoted": {k: int(v) for k,v in positive_mismatches.items()},
        "output": "team_promotable_player_delta.parquet",
        "policy": "NULL fills only; positive signal mismatches and source conflicts remain quarantined",
    }
    (args.out / "team_signal_candidate_report.json").write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(report, sort_keys=True))


if __name__ == "__main__":
    main()
