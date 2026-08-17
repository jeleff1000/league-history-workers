"""Fail-closed validation for the public-only research lake sent to GitHub runners."""
from __future__ import annotations

import argparse
from pathlib import Path

import duckdb

ALLOWED_PLATFORMS = {"sleeper", "mfl", "fleaflicker"}
REQUIRED_TABLES = {"league_settings", "draft", "transactions", "player_fantasy", "matchup"}
REQUIRED_COHORT_COLUMNS = {
    "cohort_teams", "cohort_roster", "cohort_scoring", "cohort_pass_td",
    "cohort_playoff_teams", "cohort_dynasty", "cohort_best_ball",
    "cohort_position_eligible",
}


def required_input_paths(root: Path, *, require_auxiliary_sidecars: bool) -> tuple[Path, ...]:
    corpus = root / "corpus_snapshot.duckdb"
    ops = root / "ops_cache.duckdb"
    market = root / "nfl_market_adp.parquet"
    thresholds = root / "ladder_thresholds.json"
    return (corpus, ops, market, thresholds) if require_auxiliary_sidecars else (corpus, ops)


def validate(root: Path, *, require_auxiliary_sidecars: bool = True) -> None:
    corpus = root / "corpus_snapshot.duckdb"
    ops = root / "ops_cache.duckdb"
    private = root / "leagues_snapshot.duckdb"
    if private.exists():
        raise RuntimeError("private real-league snapshot must never enter the public runner bundle")
    for path in required_input_paths(root, require_auxiliary_sidecars=require_auxiliary_sidecars):
        if not path.is_file() or not path.stat().st_size:
            raise FileNotFoundError(path)

    con = duckdb.connect(str(corpus), read_only=True)
    tables = {
        row[0] for row in con.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema='public'"
        ).fetchall()
    }
    missing = REQUIRED_TABLES - tables
    if missing:
        raise RuntimeError(f"corpus missing tables: {sorted(missing)}")
    matchup_cols = {
        row[0] for row in con.execute("DESCRIBE public.matchup").fetchall()
    }
    player_cols = {
        row[0] for row in con.execute("DESCRIBE public.player_fantasy").fetchall()
    }
    forbidden_player_columns = {"is_playoffs_bf", "made_po_bf", "made_po"} & player_cols
    if forbidden_player_columns:
        raise RuntimeError(
            "canonical player schema contains parallel playoff fields: "
            f"{sorted(forbidden_player_columns)}"
        )
    missing_player_truth = {"is_playoffs", "made_playoffs"} - player_cols
    if missing_player_truth:
        raise RuntimeError(f"canonical player schema missing resolved fields: {sorted(missing_player_truth)}")
    missing_cohort = REQUIRED_COHORT_COLUMNS - player_cols
    if missing_cohort:
        raise RuntimeError(f"canonical player schema missing materialized cohort fields: {sorted(missing_cohort)}")
    null_cohort = con.execute(
        "SELECT COUNT(*) FROM public.player_fantasy WHERE "
        + " OR ".join(f'"{c}" IS NULL' for c in sorted(REQUIRED_COHORT_COLUMNS))
    ).fetchone()[0]
    if null_cohort:
        raise RuntimeError(f"canonical player schema has {null_cohort:,} rows with null cohort fields")
    required_matchup_outcomes = {
        "opponent", "opponent_points", "win", "loss", "tie"
    }
    missing_outcomes = required_matchup_outcomes - matchup_cols
    if missing_outcomes:
        raise RuntimeError(
            f"corpus matchup missing outcome-recovery columns: {sorted(missing_outcomes)}"
        )
    platforms = {
        str(row[0]).strip().lower()
        for row in con.execute(
            "SELECT DISTINCT platform FROM public.league_settings WHERE platform IS NOT NULL"
        ).fetchall()
    }
    unexpected = platforms - ALLOWED_PLATFORMS
    if unexpected:
        raise RuntimeError(f"non-public platform(s) in corpus: {sorted(unexpected)}")
    bad_names = con.execute(
        "SELECT COUNT(*) FROM public.league_settings WHERE db_name NOT LIKE 'smpl_%'"
    ).fetchone()[0]
    if bad_names:
        raise RuntimeError(f"{bad_names:,} non-sampled db_name rows found")
    rows = con.execute("SELECT COUNT(*) FROM public.league_settings").fetchone()[0]
    # Presence of tables is not enough for a runner cache. LocalReader applies the
    # same identified-rostered-player gate used by the cohort builders; if no
    # league-year survives it, denominator SQL returns an empty lattice and every
    # apparently successful shard is just a typed-empty artifact.
    population = con.execute("""
        SELECT
          COUNT(*) AS player_rows,
          COUNT(*) FILTER (WHERE NFL_player_id IS NOT NULL) AS identified_rows,
          COUNT(*) FILTER (WHERE CAST(is_rostered AS INT)=1
                           AND NFL_player_id IS NOT NULL) AS identified_rostered_rows,
          COUNT(DISTINCT db_name || ':' || CAST(year AS VARCHAR)) AS player_league_years
        FROM public.player_fantasy
    """).fetchone()
    usable = con.execute("""
        SELECT COUNT(*) FROM (
          SELECT db_name, year
          FROM public.player_fantasy
          GROUP BY 1, 2
          HAVING COUNT(NFL_player_id) FILTER (WHERE CAST(is_rostered AS INT)=1) >= 100
             AND COUNT(NFL_player_id) FILTER (WHERE CAST(is_rostered AS INT)=1)
                 >= 0.85 * COUNT(*) FILTER (WHERE CAST(is_rostered AS INT)=1)
        )
    """).fetchone()[0]
    if not population[0] or not population[2] or not usable:
        raise RuntimeError(
            "public player_fantasy population is unusable: "
            f"rows={population[0]:,}, identified={population[1]:,}, "
            f"identified_rostered={population[2]:,}, league_years={population[3]:,}, "
            f"reader_usable_league_years={usable:,}"
        )
    con.close()
    # Presence/size is not integrity: a killed multi-GB copy can retain the expected length
    # with zeroed/corrupt blocks. Force DuckDB to read both ends of the public ops database.
    ops_con = duckdb.connect(str(ops), read_only=True)
    ops_rows, min_year, max_year = ops_con.execute("""
        SELECT COUNT(*), MIN(year), MAX(year)
        FROM nfl_historical.nfl_player_stats_all
    """).fetchone()
    if not ops_rows or min_year is None or max_year is None:
        raise RuntimeError("ops cache super table is empty or unreadable")
    ops_con.close()
    print(f"public research lake valid: {rows:,} league-years; "
          f"player_rows={population[0]:,}; identified_rostered={population[2]:,}; "
          f"usable_league_years={usable:,}; {ops_rows:,} NFL rows; "
          f"platforms={sorted(platforms)}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("root", type=Path)
    parser.add_argument(
        "--without-auxiliary-sidecars",
        action="store_true",
        help="validate only corpus and ops; intended for an ephemeral merge workspace",
    )
    args = parser.parse_args()
    validate(args.root, require_auxiliary_sidecars=not args.without_auxiliary_sidecars)
