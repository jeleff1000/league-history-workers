from pathlib import Path
import subprocess
import sys

import duckdb


SCRIPT = (
    Path(__file__).resolve().parents[2]
    / "scripts"
    / "research_cohorts"
    / "merge_team_signal_deltas.py"
)


KEY = "db_name, year, week, NFL_player_id, manager, team_key, team_name, platform"


def _write(path: Path, rows: list[tuple[object, ...]]) -> None:
    con = duckdb.connect()
    con.execute(
        """
        CREATE TABLE d(
          db_name VARCHAR, year INTEGER, week INTEGER, NFL_player_id VARCHAR,
          manager VARCHAR, team_key VARCHAR, team_name VARCHAR, platform VARCHAR,
          source_win INTEGER, source_team_points DOUBLE, source_playoffs INTEGER,
          source_champion INTEGER, source_final_playoff_seed INTEGER,
          source_loss INTEGER, source_tie INTEGER, source_made_playoffs INTEGER,
          source_files VARCHAR
        )
        """
    )
    con.executemany("INSERT INTO d VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", rows)
    con.execute("COPY d TO ? (FORMAT PARQUET)", [str(path)])
    con.close()


def _row(*, win: int | None, points: float | None, source_files: str) -> tuple[object, ...]:
    return (
        "league", 2020, 7, "nfl-1", "manager", "team", "team", "mfl",
        win, points, None, None, None, None, None, None, source_files,
    )


def test_merges_overlap_when_source_values_agree(tmp_path: Path) -> None:
    normal = tmp_path / "normal.parquet"
    replacement = tmp_path / "replacement.parquet"
    out = tmp_path / "combined.parquet"
    report = tmp_path / "report.json"
    _write(normal, [_row(win=1, points=123.0, source_files="normal")])
    _write(replacement, [_row(win=1, points=123.0, source_files="replacement")])

    result = subprocess.run(
        [sys.executable, str(SCRIPT), "--normal", str(normal), "--replacement", str(replacement), "--out", str(out), "--report", str(report)],
        text=True, capture_output=True, check=False,
    )

    assert result.returncode == 0, result.stderr
    con = duckdb.connect()
    assert con.execute(f"SELECT COUNT(*) FROM read_parquet('{out.as_posix()}')").fetchone()[0] == 1
    assert con.execute(f"SELECT source_win, source_team_points FROM read_parquet('{out.as_posix()}')").fetchone() == (1, 123.0)
    assert con.execute(f"SELECT source_files FROM read_parquet('{out.as_posix()}')").fetchone()[0] == "normal|replacement"
    con.close()


def test_rejects_overlap_when_source_values_disagree(tmp_path: Path) -> None:
    normal = tmp_path / "normal.parquet"
    replacement = tmp_path / "replacement.parquet"
    _write(normal, [_row(win=0, points=123.0, source_files="normal")])
    _write(replacement, [_row(win=1, points=123.0, source_files="replacement")])

    result = subprocess.run(
        [sys.executable, str(SCRIPT), "--normal", str(normal), "--replacement", str(replacement), "--out", str(tmp_path / "out.parquet"), "--report", str(tmp_path / "report.json")],
        text=True, capture_output=True, check=False,
    )

    assert result.returncode != 0
    assert "conflicting source values" in result.stderr
