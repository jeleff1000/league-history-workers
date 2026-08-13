import duckdb

from combine_null_team_week_loss_tie_candidates import combine
from test_combine_exact_team_null_candidates import _candidate


def test_combine_groups_null_player_rows_by_verified_team_week(tmp_path):
    root = _candidate(tmp_path, "null-team")
    path = root / "team_promotable_player_delta.parquet"
    con = duckdb.connect()
    con.execute("CREATE TABLE d AS SELECT * FROM read_parquet(?)", [str(path)])
    con.execute("UPDATE d SET NFL_player_id=NULL")
    con.execute("INSERT INTO d SELECT * FROM d")
    con.execute("COPY d TO ? (FORMAT PARQUET)", [str(path)])
    con.close()

    out = tmp_path / "team-week.parquet"
    receipt = combine([root], out, tmp_path / "receipt.json")

    assert receipt["raw_rows"] == 2
    assert receipt["unique_team_weeks"] == 1
    assert receipt["conflicting_team_weeks"] == 0
    con = duckdb.connect()
    assert con.execute(
        "SELECT NFL_player_id, source_loss, source_tie FROM read_parquet(?)", [str(out)]
    ).fetchone() == (None, 0, 0)
    con.close()
