from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from scripts.research_cohorts.build_exact_player_delta import build_delta


def test_exact_delta_preserves_loss_and_tie_and_only_emits_null_fills(tmp_path: Path) -> None:
    base = tmp_path / "base.duckdb"
    source = tmp_path / "source.parquet"
    out = tmp_path / "delta.parquet"
    con = duckdb.connect(str(base))
    con.execute("CREATE SCHEMA public")
    columns = [
        "db_name VARCHAR", "year INTEGER", "week INTEGER", "NFL_player_id VARCHAR",
        "is_started INTEGER", "is_rostered INTEGER", "fantasy_points DOUBLE",
        "win INTEGER", "loss INTEGER", "tie INTEGER", "champion INTEGER",
        "clutch_equity DOUBLE", "manager_lamar DOUBLE", "manager VARCHAR",
        "team_points DOUBLE", "final_playoff_seed INTEGER", "is_playoffs INTEGER",
        "has_po_signal INTEGER", "player VARCHAR", "position VARCHAR",
        "fantasy_position VARCHAR", "platform VARCHAR", "team_key VARCHAR",
        "team_name VARCHAR", "nfl_team_api VARCHAR", "yahoo_player_id VARCHAR",
        "sleeper_player_id VARCHAR", "espn_player_id VARCHAR",
        "fleaflicker_player_id VARCHAR", "mfl_player_id VARCHAR",
        "made_playoffs INTEGER", "cohort_a VARCHAR", "cohort_b VARCHAR",
        "cohort_c VARCHAR", "cohort_d VARCHAR", "cohort_e VARCHAR",
        "cohort_f VARCHAR", "cohort_g VARCHAR",
    ]
    con.execute(f"CREATE TABLE public.player_fantasy ({', '.join(columns)})")
    con.execute("""
        INSERT INTO public.player_fantasy
        VALUES ('db', 2024, 1, 'p1', 1, 1, 10, NULL, NULL, NULL, NULL,
                NULL, NULL, 'mgr', NULL, NULL, NULL, NULL, 'Player', 'RB', 'RB',
                'sleeper', 'tk', 'Team', NULL, NULL, 'sp1', NULL, NULL, NULL,
                NULL, 'a', 'b', 'c', 'd', 'e', 'f', 'g')
    """)
    con.close()
    table = pa.table({
        "db_name": ["db"], "year": [2024], "week": [1],
        "NFL_player_id": ["p1"], "manager": ["mgr"], "team_key": ["tk"],
        "team_name": ["Team"], "platform": ["sleeper"], "win": [0],
        "loss": [1], "tie": [0], "team_points": [101.5],
        "is_playoffs": [1], "champion": [0], "final_playoff_seed": [2],
        "made_playoffs": [1], "clutch_equity": [2.5],
    })
    pq.write_table(table, source)

    report = build_delta(base, [source], out)

    assert report["matched_rows"] == 1
    assert report["improvements_by_field"]["win"] == 1
    assert report["improvements_by_field"]["loss"] == 1
    assert report["improvements_by_field"]["tie"] == 1
    assert report["improvements_by_field"]["clutch_equity"] == 1
    assert pq.read_table(out).num_rows == 1


def test_exact_delta_emits_full_schema_rows_missing_from_canonical(tmp_path: Path) -> None:
    base = tmp_path / "base.duckdb"
    source = tmp_path / "source.parquet"
    delta = tmp_path / "delta.parquet"
    inserts = tmp_path / "inserts.parquet"
    con = duckdb.connect(str(base))
    con.execute("CREATE SCHEMA public")
    columns = [
        "db_name VARCHAR", "year INTEGER", "week INTEGER", "NFL_player_id VARCHAR",
        "is_started INTEGER", "is_rostered INTEGER", "fantasy_points DOUBLE",
        "win INTEGER", "loss INTEGER", "tie INTEGER", "champion INTEGER",
        "clutch_equity DOUBLE", "manager_lamar DOUBLE", "manager VARCHAR",
        "team_points DOUBLE", "final_playoff_seed INTEGER", "is_playoffs INTEGER",
        "has_po_signal INTEGER", "player VARCHAR", "position VARCHAR",
        "fantasy_position VARCHAR", "platform VARCHAR", "team_key VARCHAR",
        "team_name VARCHAR", "nfl_team_api VARCHAR", "yahoo_player_id VARCHAR",
        "sleeper_player_id VARCHAR", "espn_player_id VARCHAR",
        "fleaflicker_player_id VARCHAR", "mfl_player_id VARCHAR",
        "made_playoffs INTEGER", "cohort_a VARCHAR", "cohort_b VARCHAR",
        "cohort_c VARCHAR", "cohort_d VARCHAR", "cohort_e VARCHAR",
        "cohort_f VARCHAR", "cohort_g VARCHAR",
    ]
    con.execute(f"CREATE TABLE public.player_fantasy ({', '.join(columns)})")
    con.close()
    row = {
        "db_name": ["db"], "year": [2024], "week": [1], "NFL_player_id": ["p2"],
        "is_started": [1], "is_rostered": [1], "fantasy_points": [12.0],
        "win": [1], "loss": [0], "tie": [0], "champion": [0],
        "clutch_equity": [1.0], "manager_lamar": [2.0], "manager": ["mgr"],
        "team_points": [100.0], "final_playoff_seed": [1], "is_playoffs": [1],
        "has_po_signal": [1], "player": ["Player Two"], "position": ["RB"],
        "fantasy_position": ["RB"], "platform": ["sleeper"], "team_key": ["tk2"],
        "team_name": ["Team Two"], "nfl_team_api": ["BUF"], "yahoo_player_id": [None],
        "sleeper_player_id": ["sp2"], "espn_player_id": [None],
        "fleaflicker_player_id": [None], "mfl_player_id": [None],
        "made_playoffs": [1], "cohort_a": ["a"], "cohort_b": ["b"],
        "cohort_c": ["c"], "cohort_d": ["d"], "cohort_e": ["e"],
        "cohort_f": ["f"], "cohort_g": ["g"],
    }
    pq.write_table(pa.table(row), source)
    report = build_delta(base, [source], delta, inserts)
    assert report["matched_rows"] == 0
    assert report["insert_candidate_rows"] == 1
    assert report["unresolved_unmatched_rows"] == 0
    assert pq.read_table(inserts).num_rows == 1
    assert pq.read_table(inserts).column_names[-7:] == ["cohort_a", "cohort_b", "cohort_c", "cohort_d", "cohort_e", "cohort_f", "cohort_g"]
