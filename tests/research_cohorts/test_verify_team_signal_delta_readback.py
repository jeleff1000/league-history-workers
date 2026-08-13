from pathlib import Path
import json
import subprocess
import sys

import duckdb


SCRIPT = (
    Path(__file__).resolve().parents[2]
    / "scripts"
    / "research_cohorts"
    / "verify_team_signal_delta_readback.py"
)


def test_verifies_only_authorized_null_fills_and_mfl_win_replacements(tmp_path: Path) -> None:
    base = tmp_path / "base.duckdb"
    con = duckdb.connect(str(base))
    con.execute("CREATE SCHEMA public")
    con.execute(
        """
        CREATE TABLE public.player_fantasy AS SELECT
          'league'::VARCHAR db_name, 2020::INTEGER AS "year", 7::INTEGER AS "week",
          'nfl-1'::VARCHAR NFL_player_id, 'manager'::VARCHAR manager,
          'team'::VARCHAR team_key, 'team'::VARCHAR team_name, 'mfl'::VARCHAR platform,
          1::INTEGER win, NULL::INTEGER loss, NULL::INTEGER tie,
          10.0::DOUBLE team_points, 1::INTEGER is_playoffs, NULL::INTEGER champion,
          NULL::INTEGER final_playoff_seed, NULL::INTEGER made_playoffs
        """
    )
    schema = con.execute("DESCRIBE public.player_fantasy").fetchall()
    con.close()
    before = tmp_path / "before.json"
    before.write_text(json.dumps({"schema": schema, "player_rows": 1}), encoding="utf-8")
    ops = tmp_path / "ops.duckdb"
    ops.write_bytes(b"ops")
    import hashlib
    ops_before = tmp_path / "ops.sha256"
    ops_before.write_text(hashlib.sha256(b"ops").hexdigest() + "  ops.duckdb\n", encoding="utf-8")
    delta = tmp_path / "delta.parquet"
    con = duckdb.connect()
    con.execute(
        """
        CREATE TABLE d AS SELECT
          'league'::VARCHAR db_name, 2020::INTEGER AS "year", 7::INTEGER AS "week",
          'nfl-1'::VARCHAR NFL_player_id, 'manager'::VARCHAR manager,
          'team'::VARCHAR team_key, 'team'::VARCHAR team_name, 'mfl'::VARCHAR platform,
          0::INTEGER canonical_win, 1::INTEGER source_win,
          NULL::INTEGER canonical_loss, NULL::INTEGER source_loss,
          NULL::INTEGER canonical_tie, NULL::INTEGER source_tie,
          10.0::DOUBLE canonical_team_points, 12.0::DOUBLE source_team_points,
          NULL::INTEGER canonical_is_playoffs, 1::INTEGER source_playoffs,
          NULL::INTEGER canonical_champion, NULL::INTEGER source_champion,
          NULL::INTEGER canonical_final_playoff_seed, NULL::INTEGER source_final_playoff_seed,
          NULL::INTEGER canonical_made_playoffs, NULL::INTEGER source_made_playoffs
        """
    )
    con.execute("COPY d TO ? (FORMAT PARQUET)", [str(delta)])
    con.close()
    output = tmp_path / "result.json"

    result = subprocess.run(
        [sys.executable, str(SCRIPT), "--base", str(base), "--delta", str(delta), "--before", str(before), "--ops", str(ops), "--ops-before", str(ops_before), "--out", str(output)],
        text=True, capture_output=True, check=False,
    )

    assert result.returncode == 0, result.stderr
    report = json.loads(output.read_text())
    assert report["authorized_cells_by_field"] == {"win": 1, "is_playoffs": 1}
    assert not any(report["source_value_disagreements"].values())
