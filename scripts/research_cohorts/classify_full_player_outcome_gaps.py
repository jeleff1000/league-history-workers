"""Classify missing started-player outcomes using only the canonical player table.

This is an audit, not a lake writer and not an API fetcher.  It answers which
missing outcome rows can already be resolved from the player table itself,
which have a same-team teammate outcome, which look like no-opponent
postseason/bye cases, and which are genuine API candidates.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import duckdb


def qident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def first(candidates: tuple[str, ...], available: set[str]) -> str | None:
    return next((c for c in candidates if c in available), None)


def classify(snapshot: Path, out_dir: Path) -> dict[str, object]:
    con = duckdb.connect(str(snapshot), read_only=True)
    tables = {r[0] for r in con.execute(
        "SELECT table_name FROM duckdb_tables() WHERE schema_name='public'"
    ).fetchall()}
    if "player_fantasy" not in tables:
        raise SystemExit("canonical lake has no public.player_fantasy table")

    available = {r[0] for r in con.execute("DESCRIBE public.player_fantasy").fetchall()}
    required = {"db_name", "year", "week", "is_started"}
    missing = required - available
    if missing:
        raise SystemExit(f"player_fantasy missing required columns: {sorted(missing)}")

    win = first(("win", "is_win", "won"), available)
    loss = first(("loss", "is_loss", "lost"), available)
    tie = first(("tie", "is_tie", "tied"), available)
    team_points = first(("team_points", "fantasy_team_points", "manager_points"), available)
    opponent_points = first(("opponent_points", "opp_points"), available)
    playoff = first(("is_playoffs", "is_playoff", "playoff", "playoff_start", "started_playoffs"), available)
    team_key = first(("franchise_id", "manager_id", "roster_id", "team_id", "fantasy_team_id", "manager"), available)
    platform = first(("platform", "source_platform", "db_platform"), available)

    if not team_key:
        raise SystemExit("cannot classify teammates: no team identity column exists")

    outcome_terms: list[str] = []
    if win and loss:
        terms = [f"{qident(win)} IS NOT NULL", f"{qident(loss)} IS NOT NULL"]
        if tie:
            terms.append(f"{qident(tie)} IS NOT NULL")
        outcome_terms.append("(" + " OR ".join(terms) + ")")
    if team_points and opponent_points:
        outcome_terms.append(
            f"({qident(team_points)} IS NOT NULL AND {qident(opponent_points)} IS NOT NULL)"
        )
    outcome_present = " OR ".join(outcome_terms) if outcome_terms else "FALSE"
    started = "CAST(is_started AS INTEGER)=1"
    team_expr = f"NULLIF(TRIM(CAST({qident(team_key)} AS VARCHAR)), '')"
    group_expr = f"CAST(db_name AS VARCHAR)||'|'||CAST(year AS VARCHAR)||'|'||CAST(week AS VARCHAR)||'|'||COALESCE({team_expr}, '<NULL>')"
    own_missing = f"{started} AND NOT ({outcome_present})"
    playoff_expr = f"CAST({qident(playoff)} AS INTEGER)=1" if playoff else "FALSE"

    out_dir.mkdir(parents=True, exist_ok=True)
    con.execute(f"""
        CREATE OR REPLACE TEMP VIEW player_rows AS
        SELECT *,
               {outcome_present} AS has_outcome,
               {team_expr} AS team_identity,
               {group_expr} AS team_week_key,
               {playoff_expr} AS playoff_signal
        FROM public.player_fantasy
    """)
    # Existing outcome evidence on any player in the same fantasy team/week.
    con.execute("""
        CREATE OR REPLACE TEMP VIEW team_week_evidence AS
        SELECT team_week_key,
               BOOL_OR(has_outcome) AS teammate_has_outcome,
               COUNT(*) FILTER (WHERE has_outcome) AS outcome_rows,
               COUNT(*) AS team_week_rows,
               BOOL_OR(playoff_signal) AS any_playoff_signal
        FROM player_rows
        WHERE team_identity IS NOT NULL
        GROUP BY 1
    """)

    classification_sql = f"""
        SELECT
          CAST(p.db_name AS VARCHAR) AS db_name,
          CAST(p.year AS INTEGER) AS year,
          CAST(p.week AS INTEGER) AS week,
          p.team_identity,
          p.team_week_key,
          p.NFL_player_id,
          CASE
            WHEN p.has_outcome THEN 'already_resolved'
            WHEN p.team_identity IS NOT NULL AND COALESCE(e.teammate_has_outcome, FALSE)
              THEN 'teammate_outcome_available'
            WHEN p.team_identity IS NULL
              THEN 'missing_team_identity_api_candidate'
            WHEN COALESCE(e.any_playoff_signal, FALSE) OR p.playoff_signal
              THEN 'no_opponent_postseason_candidate'
            ELSE 'api_candidate'
          END AS classification,
          p.has_outcome,
          COALESCE(e.teammate_has_outcome, FALSE) AS teammate_has_outcome,
          COALESCE(e.outcome_rows, 0) AS teammate_outcome_rows,
          COALESCE(e.team_week_rows, 0) AS team_week_rows,
          p.playoff_signal
          {', CAST(p.' + qident(platform) + ' AS VARCHAR) AS platform' if platform else ", NULL::VARCHAR AS platform"}
        FROM player_rows p
        LEFT JOIN team_week_evidence e USING (team_week_key)
        WHERE {own_missing}
    """
    con.execute(f"COPY ({classification_sql}) TO ? (FORMAT PARQUET, COMPRESSION ZSTD)",
                [str(out_dir / "player_outcome_gap_classification.parquet")])

    total = con.execute("SELECT COUNT(*) FROM player_rows WHERE " + own_missing).fetchone()[0]
    rows = con.execute("""
        SELECT classification, COUNT(*) AS rows,
               COUNT(DISTINCT db_name||'|'||CAST(year AS VARCHAR)) AS league_seasons
        FROM read_parquet(?) GROUP BY 1 ORDER BY 1
    """, [str(out_dir / "player_outcome_gap_classification.parquet")]).fetchall()
    by_year = con.execute("""
        SELECT year, classification, COUNT(*) AS rows
        FROM read_parquet(?) GROUP BY 1,2 ORDER BY 1,2
    """, [str(out_dir / "player_outcome_gap_classification.parquet")]).fetchall()
    by_platform = con.execute("""
        SELECT COALESCE(platform, 'unknown') AS platform, classification, COUNT(*) AS rows
        FROM read_parquet(?) GROUP BY 1,2 ORDER BY 1,2
    """, [str(out_dir / "player_outcome_gap_classification.parquet")]).fetchall()
    classified = sum(int(r[1]) for r in rows)
    if classified != int(total):
        raise SystemExit(f"classification conservation failed: total={total}, classified={classified}")
    result = {
        "population": "started player rows with no win/loss/tie and no team/opponent points",
        "missing_outcome_rows": int(total),
        "resolved_columns": {
            "win": win, "loss": loss, "tie": tie,
            "team_points": team_points, "opponent_points": opponent_points,
            "playoff": playoff, "team_identity": team_key, "platform": platform,
        },
        "classifications": [
            {"classification": r[0], "rows": int(r[1]), "league_seasons": int(r[2])}
            for r in rows
        ],
        "by_year": [{"year": int(r[0]), "classification": r[1], "rows": int(r[2])} for r in by_year],
        "by_platform": [{"platform": r[0], "classification": r[1], "rows": int(r[2])} for r in by_platform],
        "interpretation": {
            "teammate_outcome_available": "safe in-cache recovery candidate; requires copying the teammate's team outcome, not an API pull",
            "no_opponent_postseason_candidate": "candidate for legitimate elimination/no-opponent handling; not automatically a W/L",
            "missing_team_identity_api_candidate": "cannot use teammate evidence; requires source/API or identity repair",
            "api_candidate": "no in-cache evidence found; source/API pull candidate",
        },
    }
    (out_dir / "player_outcome_gap_classification_summary.json").write_text(
        json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    con.close()
    print(json.dumps({"missing_outcome_rows": total, "classifications": result["classifications"]}, indent=2))
    return result


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--snapshot", type=Path, required=True)
    ap.add_argument("--out-dir", type=Path, required=True)
    args = ap.parse_args()
    classify(args.snapshot, args.out_dir)


if __name__ == "__main__":
    main()
