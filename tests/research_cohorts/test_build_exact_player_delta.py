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
