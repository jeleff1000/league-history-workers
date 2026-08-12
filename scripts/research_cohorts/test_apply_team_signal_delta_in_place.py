import json
import subprocess
import sys

import duckdb

from canonical_player_schema import BASE_PLAYER_COLUMNS, EXPECTED_COHORT_COLUMNS


def _base(path, *, null_team_identity: bool = False, null_player_id: bool = False):
    columns = BASE_PLAYER_COLUMNS + sorted(EXPECTED_COHORT_COLUMNS) + ["loss", "tie"]
    con = duckdb.connect(str(path))
    con.execute("CREATE SCHEMA public")
    con.execute(
        "CREATE TABLE public.player_fantasy (" + ", ".join(
            f'"{column}" VARCHAR' for column in columns
        ) + ")"
    )
    values = {column: None for column in columns}
    values.update({
        "db_name": "league-a", "year": "2020", "week": "1", "NFL_player_id": "nfl-a",
        "manager": "manager-a", "team_key": "team-a", "team_name": "Team A", "platform": "mfl",
    })
    if null_team_identity:
        values["team_key"] = None
        values["team_name"] = None
    if null_player_id:
        values["NFL_player_id"] = None
    names = ", ".join(f'"{column}"' for column in columns)
    placeholders = ", ".join("?" for _ in columns)
    con.execute(f"INSERT INTO public.player_fantasy ({names}) VALUES ({placeholders})", list(values.values()))
    con.close()


def _delta(path, *, conflicting_loss: bool = False, null_player_id: bool = False):
    con = duckdb.connect()
    con.execute("""
        CREATE TABLE d AS
        SELECT 'league-a'::VARCHAR AS db_name, 2020::INTEGER AS year, 1::INTEGER AS week,
               'nfl-a'::VARCHAR AS NFL_player_id, 'manager-a'::VARCHAR AS manager,
               'team-a'::VARCHAR AS team_key, 'Team A'::VARCHAR AS team_name,
               'mfl'::VARCHAR AS platform, NULL::VARCHAR AS source_win,
               '1'::VARCHAR AS source_loss, '0'::VARCHAR AS source_tie,
               NULL::VARCHAR AS source_team_points, NULL::VARCHAR AS source_playoffs,
               NULL::VARCHAR AS source_champion, NULL::VARCHAR AS source_final_playoff_seed
    """)
    if null_player_id:
        con.execute("UPDATE d SET NFL_player_id=NULL")
    con.execute("INSERT INTO d SELECT * FROM d")
    if conflicting_loss:
        con.execute("UPDATE d SET source_loss='0' WHERE rowid=1")
    con.execute("COPY d TO ? (FORMAT PARQUET)", [str(path)])
    con.close()


def test_apply_collapses_identical_duplicate_delta_keys(tmp_path):
    base = tmp_path / "base.duckdb"
    delta = tmp_path / "delta.parquet"
    report = tmp_path / "report.json"
    _base(base)
    _delta(delta)

    result = subprocess.run([
        sys.executable,
        "scripts/research_cohorts/apply_team_signal_delta_in_place.py",
        "--base", str(base), "--delta", str(delta), "--report", str(report),
    ], capture_output=True, text=True)

    assert result.returncode == 0, result.stderr + result.stdout
    payload = json.loads(report.read_text())
    assert payload["delta_rows"] == 1
    assert payload["improvements_by_field"]["loss"] == 1
    assert payload["improvements_by_field"]["tie"] == 1
    con = duckdb.connect(str(base), read_only=True)
    assert con.execute("SELECT loss, tie FROM public.player_fantasy").fetchone() == ("1", "0")
    con.close()


def test_apply_rejects_conflicting_duplicate_delta_keys(tmp_path):
    base = tmp_path / "base.duckdb"
    delta = tmp_path / "delta.parquet"
    report = tmp_path / "report.json"
    _base(base)
    _delta(delta, conflicting_loss=True)

    result = subprocess.run([
        sys.executable,
        "scripts/research_cohorts/apply_team_signal_delta_in_place.py",
        "--base", str(base), "--delta", str(delta), "--report", str(report),
    ], capture_output=True, text=True)

    assert result.returncode != 0
    assert "conflicting duplicate delta keys: 1" in result.stderr
    con = duckdb.connect(str(base), read_only=True)
    assert con.execute("SELECT loss, tie FROM public.player_fantasy").fetchone() == (None, None)
    con.close()


def test_apply_allows_known_source_team_identity_when_cache_identity_is_null(tmp_path):
    base = tmp_path / "base.duckdb"
    delta = tmp_path / "delta.parquet"
    report = tmp_path / "report.json"
    _base(base, null_team_identity=True)
    _delta(delta)

    result = subprocess.run([
        sys.executable,
        "scripts/research_cohorts/apply_team_signal_delta_in_place.py",
        "--base", str(base), "--delta", str(delta), "--report", str(report),
    ], capture_output=True, text=True)

    assert result.returncode == 0, result.stderr + result.stdout
    con = duckdb.connect(str(base), read_only=True)
    assert con.execute("SELECT loss, tie FROM public.player_fantasy").fetchone() == ("1", "0")
    con.close()


def test_apply_allows_exact_null_player_id_without_fanout(tmp_path):
    base = tmp_path / "base.duckdb"
    delta = tmp_path / "delta.parquet"
    report = tmp_path / "report.json"
    _base(base, null_player_id=True)
    _delta(delta, null_player_id=True)

    result = subprocess.run([
        sys.executable,
        "scripts/research_cohorts/apply_team_signal_delta_in_place.py",
        "--base", str(base), "--delta", str(delta), "--report", str(report),
    ], capture_output=True, text=True)

    assert result.returncode == 0, result.stderr + result.stdout
    con = duckdb.connect(str(base), read_only=True)
    assert con.execute("SELECT loss, tie FROM public.player_fantasy").fetchone() == ("1", "0")
    con.close()
