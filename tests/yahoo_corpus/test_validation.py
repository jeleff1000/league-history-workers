from __future__ import annotations

from pathlib import Path

import duckdb
import pytest

from scripts.sleeper_corpus.build_corpus_snapshot import fold_database, open_snapshot
from scripts.yahoo_corpus.scheduler import Candidate
from scripts.yahoo_corpus.validation import SourceValidationError, validate_source


def task(cohort: str = "10t_flx_half_4pt") -> Candidate:
    return Candidate(
        task_id="yahoo-abcd",
        grant_id="grant-1",
        league_key="423.l.999",
        season=2023,
        cohort_slug=cohort,
        lineage_id="423.l.999",
    )


def make_source(path: Path, *, champions: tuple[str, ...] = ("f1",)) -> None:
    con = duckdb.connect(str(path))
    con.execute("CREATE SCHEMA public")
    con.execute("""
        CREATE TABLE public.league_settings AS SELECT
          'smpl_yahoo_abcd'::VARCHAR AS db_name, 2023::INTEGER AS year, 10::INTEGER AS num_teams,
          false::BOOLEAN AS is_dynasty, NULL::INTEGER AS max_keepers, false::BOOLEAN AS sleeper_best_ball,
          100::INTEGER AS waiver_budget, 'faab'::VARCHAR AS waiver_type,
          1::INTEGER AS roster_QB, 2::INTEGER AS roster_RB, 2::INTEGER AS roster_WR,
          1::INTEGER AS roster_TE, 1::INTEGER AS roster_FLX, 0::INTEGER AS roster_SUPER_FLEX,
          0::INTEGER AS roster_IDP, 0::INTEGER AS roster_DL, 0::INTEGER AS roster_LB,
          0::INTEGER AS roster_DB, 0::INTEGER AS roster_DB_LB, 0::INTEGER AS roster_DL_LB,
          0.5::DOUBLE AS scoring_rec, 4.0::DOUBLE AS scoring_pass_td,
          15::INTEGER AS playoff_start_week, 6::INTEGER AS playoff_teams,
          14::INTEGER AS regular_season_weeks, 17::INTEGER AS end_week,
          false::BOOLEAN AS has_multiweek_championship, 1::INTEGER AS roster_K,
          1::INTEGER AS roster_DEF, 6::INTEGER AS roster_BN, 0::INTEGER AS roster_TAXI,
          2::INTEGER AS roster_IR, 'yahoo'::VARCHAR AS platform, '423.l.999'::VARCHAR AS league_key
    """)
    con.execute("""
        CREATE TABLE public.draft AS SELECT
          'smpl_yahoo_abcd'::VARCHAR AS db_name, 2023::INTEGER AS year, 'p1'::VARCHAR AS NFL_player_id,
          1::INTEGER AS pick, NULL::DOUBLE AS cost, 0.0::DOUBLE AS draft_value_zscore,
          0.0::DOUBLE AS pick_quality_zscore, 1.0::DOUBLE AS manager_lamar,
          100.0::DOUBLE AS total_fantasy_points, false::BOOLEAN AS is_keeper,
          1::INTEGER AS round, 1::INTEGER AS pick_in_round
    """)
    con.execute("""
        CREATE TABLE public.transactions AS SELECT
          'smpl_yahoo_abcd'::VARCHAR AS db_name, 2023::INTEGER AS year, 1::INTEGER AS week,
          'p1'::VARCHAR AS NFL_player_id, 'private-franchise'::VARCHAR AS franchise_id,
          'add'::VARCHAR AS transaction_type, 1.0::DOUBLE AS faab_bid,
          1.0::DOUBLE AS transaction_score, 1.0::DOUBLE AS manager_lamar_ros_managed,
          1.0::DOUBLE AS player_lamar_ros_total
    """)
    con.execute("""
        CREATE TABLE public.player_fantasy AS SELECT
          'smpl_yahoo_abcd'::VARCHAR AS db_name, 2023::INTEGER AS year, 1::INTEGER AS week,
          'p1'::VARCHAR AS NFL_player_id, 1::INTEGER AS is_started, 1::INTEGER AS is_rostered,
          10.0::DOUBLE AS fantasy_points, 1::INTEGER AS win, 1::INTEGER AS champion,
          0.5::DOUBLE AS clutch_equity, 1.0::DOUBLE AS manager_lamar,
          'Private Manager'::VARCHAR AS manager
    """)
    con.execute("""
        CREATE TABLE public.matchup(
          db_name VARCHAR, year INTEGER, franchise_id VARCHAR, champion INTEGER
        )
    """)
    for franchise in champions:
        con.execute("INSERT INTO public.matchup VALUES ('smpl_yahoo_abcd', 2023, ?, 1)", [franchise])
    con.close()


def test_validate_source_accepts_one_champion_and_matching_cohort(tmp_path: Path) -> None:
    source = tmp_path / "source.duckdb"
    make_source(source)

    summary = validate_source(source, task())

    assert summary["champions"] == 1
    assert summary["rostered_rows"] == 1
    assert summary["cohort_slug"] == "10t_flx_half_4pt"


def test_validate_source_rejects_multiple_champions(tmp_path: Path) -> None:
    source = tmp_path / "source.duckdb"
    make_source(source, champions=("f1", "f2"))

    with pytest.raises(SourceValidationError, match="champion") as error:
        validate_source(source, task())
    assert error.value.code == "ChampionCount"


def test_validate_source_rejects_cohort_mismatch(tmp_path: Path) -> None:
    source = tmp_path / "source.duckdb"
    make_source(source)

    with pytest.raises(SourceValidationError, match="cohort") as error:
        validate_source(source, task("12t_sflx_ppr_6pt"))
    assert error.value.code == "CohortMismatch"


def test_fold_database_strips_identity_columns(tmp_path: Path) -> None:
    source = tmp_path / "source.duckdb"
    output = tmp_path / "slice.duckdb"
    make_source(source)
    snapshot = open_snapshot(output)

    ok, message = fold_database(
        snapshot,
        source,
        "smpl_yahoo_abcd",
        anonymize_identity=True,
    )
    assert (ok, message) == (True, "")
    columns = {
        row[0] for row in snapshot.execute("DESCRIBE public.player_fantasy").fetchall()
    }
    transaction_columns = {
        row[0] for row in snapshot.execute("DESCRIBE public.transactions").fetchall()
    }
    assert "manager" not in columns
    assert "franchise_id" in transaction_columns
    folded_franchise = snapshot.execute(
        "SELECT franchise_id FROM public.transactions"
    ).fetchone()[0]
    assert folded_franchise.startswith("anon-")
    assert folded_franchise != "private-franchise"
    assert snapshot.execute("SELECT COUNT(*) FROM _sources").fetchone()[0] == 1
    snapshot.close()
