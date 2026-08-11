from __future__ import annotations

import sys
from pathlib import Path

import duckdb

sys.path.insert(0, str(Path(__file__).parent))

from canonical_player_schema import BASE_PLAYER_COLUMNS, EXPECTED_COHORT_COLUMNS
from audit_materialized_cohort_fanout import audit


def _create_fixture(path: Path, delta: Path) -> None:
    con = duckdb.connect(str(path))
    con.execute("CREATE SCHEMA public")
    con.execute("""
        CREATE TABLE public.league_settings (
          db_name VARCHAR, year INTEGER, scoring_pass_td DOUBLE,
          playoff_teams INTEGER, roster_FLX INTEGER, roster_SUPER_FLEX INTEGER,
          roster_IDP INTEGER, roster_DL INTEGER, roster_LB INTEGER,
          roster_DB INTEGER, roster_DB_LB INTEGER, roster_DL_LB INTEGER,
          roster_K INTEGER, roster_DEF INTEGER,
          sleeper_best_ball BOOLEAN
        )
    """)
    con.execute("""
        INSERT INTO public.league_settings VALUES
        ('lg', 2025, 6.0, 6, 1, 0, 0, 0, 0, 0, 0, 0, 1, 1, false)
    """)
    definitions = []
    for name in BASE_PLAYER_COLUMNS:
        if name in {"year", "week", "is_started", "is_rostered", "win", "champion", "final_playoff_seed", "is_playoffs", "has_po_signal", "made_playoffs"}:
            dtype = "INTEGER"
        elif name in {"fantasy_points", "clutch_equity", "manager_lamar", "team_points"}:
            dtype = "DOUBLE"
        else:
            dtype = "VARCHAR"
        definitions.append(f'"{name}" {dtype}')
    definitions.extend(f'"{name}" VARCHAR' for name in sorted(EXPECTED_COHORT_COLUMNS))
    con.execute(f"CREATE TABLE public.player_fantasy ({', '.join(definitions)})")
    row = {name: None for name in BASE_PLAYER_COLUMNS}
    row.update({
        "db_name": "lg", "year": 2025, "week": 1, "NFL_player_id": "p1",
        "position": "RB", "is_started": 1, "is_rostered": 1,
    })
    row.update({name: "kept" for name in EXPECTED_COHORT_COLUMNS})
    row["cohort_pass_td"] = "4pt"
    row["cohort_pass_td_1"] = "4pt"
    names = BASE_PLAYER_COLUMNS + sorted(EXPECTED_COHORT_COLUMNS)
    placeholders = ", ".join("?" for _ in names)
    con.execute(
        f"INSERT INTO public.player_fantasy ({', '.join('"' + n + '"' for n in names)}) VALUES ({placeholders})",
        [row[n] for n in names],
    )
    con.execute("""
      COPY (SELECT 'lg'::VARCHAR AS db_name, 2025::INTEGER AS year,
                   6.0::DOUBLE AS source_scoring_pass_td,
                   NULL::INTEGER AS source_playoff_teams,
                   NULL::INTEGER AS source_roster_FLX,
                   NULL::INTEGER AS source_roster_SUPER_FLEX,
                   NULL::INTEGER AS source_roster_IDP,
                   NULL::BOOLEAN AS source_sleeper_best_ball)
      TO ? (FORMAT PARQUET)
    """, [str(delta)])
    con.close()


def test_audit_flags_stale_base_cohort_value_and_describes_group_copy(tmp_path: Path) -> None:
    base = tmp_path / "corpus.duckdb"
    delta = tmp_path / "settings.parquet"
    _create_fixture(base, delta)

    result = audit(base, delta)

    assert result["affected_setting_keys"] == 1
    assert result["dimensions"]["pass_td"]["player_rows"] == 1
    assert result["dimensions"]["pass_td"]["base_mismatches"] == 1
    assert result["dimensions"]["pass_td"]["expected_label_counts"] == {"6pt": 1}
    assert result["dimensions"]["pass_td"]["groups"]["cohort_pass_td_1"]["same_as_base"] == 1


def test_base_only_audit_avoids_legacy_group_diagnostics(tmp_path: Path) -> None:
    base = tmp_path / "corpus.duckdb"
    delta = tmp_path / "settings.parquet"
    _create_fixture(base, delta)

    result = audit(base, delta, include_legacy_groups=False)

    assert result["dimensions"]["pass_td"]["base_mismatches"] == 1
    assert "groups" not in result["dimensions"]["pass_td"]


def test_audit_treats_position_eligibility_as_a_roster_dependent_fanout(tmp_path: Path) -> None:
    base = tmp_path / "corpus.duckdb"
    delta = tmp_path / "settings.parquet"
    _create_fixture(base, delta)
    con = duckdb.connect(str(base))
    con.execute("UPDATE public.league_settings SET roster_IDP=1")
    eligible_columns = sorted(c for c in EXPECTED_COHORT_COLUMNS if c.startswith("cohort_position_eligible"))
    con.execute(
        "UPDATE public.player_fantasy SET position='LB', "
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

    result = audit(base, delta)

    eligible = result["dimensions"]["roster"]["dependent_columns"]["cohort_position_eligible"]
    assert eligible["base_mismatches"] == 1
    assert eligible["groups"]["cohort_position_eligible_1"]["mismatches_expected"] == 1
    assert eligible["groups"]["cohort_position_eligible_1"]["position_value_counts"] == {"LB": {"0": 1}}
