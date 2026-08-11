from __future__ import annotations

import sys
from pathlib import Path

import duckdb

sys.path.insert(0, str(Path(__file__).parent))

from canonical_player_schema import EXPECTED_COHORT_COLUMNS
from materialize_materialized_cohort_fanout import apply
from test_audit_materialized_cohort_fanout import _create_fixture


def test_apply_replaces_stale_pass_td_across_base_and_all_group_copies(tmp_path: Path) -> None:
    base = tmp_path / "corpus.duckdb"
    delta = tmp_path / "settings.parquet"
    ops = tmp_path / "ops.duckdb"
    _create_fixture(base, delta)
    ops.write_bytes(b"unchanged ops cache")

    result = apply(base, ops, delta)

    assert result["schema_unchanged"] is True
    assert result["rows_unchanged"] is True
    assert result["updates"]["pass_td"] == 1
    con = duckdb.connect(str(base), read_only=True)
    values = con.execute("""
      SELECT cohort_pass_td, cohort_pass_td_1, cohort_pass_td_12
      FROM public.player_fantasy
    """).fetchone()
    con.close()
    assert values == ("6pt", "4pt", "kept")


def test_apply_refreshes_roster_and_position_eligibility_from_position_not_lineup_slot(tmp_path: Path) -> None:
    base = tmp_path / "corpus.duckdb"
    delta = tmp_path / "settings.parquet"
    ops = tmp_path / "ops.duckdb"
    _create_fixture(base, delta)
    ops.write_bytes(b"unchanged ops cache")
    con = duckdb.connect(str(base))
    con.execute("UPDATE public.league_settings SET roster_IDP=1")
    eligible_columns = sorted(c for c in EXPECTED_COHORT_COLUMNS if c.startswith("cohort_position_eligible"))
    con.execute(
        "UPDATE public.player_fantasy SET position='LB', fantasy_position='BN', "
        + ", ".join(f'"{column}"=\'0\'' for column in eligible_columns)
    )
    con.execute("""
      COPY (SELECT 'lg'::VARCHAR AS db_name, 2025::INTEGER AS year,
                   NULL::DOUBLE AS source_scoring_pass_td,
                   NULL::INTEGER AS source_playoff_teams,
                   NULL::INTEGER AS source_roster_FLX,
                   NULL::INTEGER AS source_roster_SUPER_FLEX,
                   1::INTEGER AS source_roster_IDP,
                   NULL::BOOLEAN AS source_sleeper_best_ball)
      TO ? (FORMAT PARQUET, OVERWRITE_OR_IGNORE TRUE)
    """, [str(delta)])
    con.close()

    apply(base, ops, delta)

    con = duckdb.connect(str(base), read_only=True)
    values = con.execute("""
      SELECT cohort_roster, cohort_roster_7,
             cohort_position_eligible, cohort_position_eligible_7
      FROM public.player_fantasy
    """).fetchone()
    con.close()
    assert values == ("idp", "kept", "1", "0")
