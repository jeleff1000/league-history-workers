import duckdb

from compare_artifact_candidate_cells import compare_candidate_cells


def _candidate(**overrides):
    row = {
        "artifact_id": "1", "artifact_file": "source.parquet", "file_instance": "1",
        "source_grain": "team_week_manager", "db_name": "league", "year": "2020", "week": "6",
        "NFL_player_id": None, "team_key": None, "manager": "A",
        "source_column": "win", "target_column": "win", "source_value_text": "1",
    }
    row.update(overrides)
    return row


def test_manager_week_source_fans_to_all_recipient_players_and_only_counts_nulls_as_safe_fills(tmp_path):
    base = tmp_path / "base.duckdb"
    candidate = tmp_path / "candidate.parquet"
    con = duckdb.connect(str(base))
    con.execute("CREATE SCHEMA public")
    con.execute("CREATE TABLE public.player_fantasy (db_name VARCHAR, year VARCHAR, week VARCHAR, NFL_player_id VARCHAR, team_key VARCHAR, manager VARCHAR, is_started INTEGER, win INTEGER, champion INTEGER)")
    con.execute("INSERT INTO public.player_fantasy VALUES ('league','2020','6','p1','t1','A',1,NULL,NULL), ('league','2020','6','p2','t1','A',1,1,NULL)")
    con.execute("CREATE TABLE candidate_cells AS SELECT * FROM (VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)) AS x(artifact_id,artifact_file,file_instance,source_grain,db_name,year,week,NFL_player_id,team_key,manager,source_column,target_column,source_value_text)", list(_candidate().values()))
    con.execute(f"COPY candidate_cells TO '{candidate}' (FORMAT PARQUET)")
    con.close()

    result = compare_candidate_cells(base, candidate)

    assert result["safe_null_fills_by_field"] == {"win": 1}
    assert result["same_value_by_field"] == {"win": 1}
    assert result["conflicts_by_field"] == {}
    assert result["unmatched_candidates"] == 0


def test_team_champion_candidate_is_quarantined_before_it_can_fan_out(tmp_path):
    base = tmp_path / "base.duckdb"
    candidate = tmp_path / "candidate.parquet"
    con = duckdb.connect(str(base))
    con.execute("CREATE SCHEMA public")
    con.execute("CREATE TABLE public.player_fantasy (db_name VARCHAR, year VARCHAR, week VARCHAR, NFL_player_id VARCHAR, team_key VARCHAR, manager VARCHAR, is_started INTEGER, win INTEGER, champion INTEGER)")
    con.execute("INSERT INTO public.player_fantasy VALUES ('league','2020','16','p1','t1','A',1,NULL,NULL)")
    team_champ = _candidate(source_grain="team_week", week="16", manager=None, team_key="t1", source_column="champion", target_column="champion")
    con.execute("CREATE TABLE candidate_cells AS SELECT * FROM (VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)) AS x(artifact_id,artifact_file,file_instance,source_grain,db_name,year,week,NFL_player_id,team_key,manager,source_column,target_column,source_value_text)", list(team_champ.values()))
    con.execute(f"COPY candidate_cells TO '{candidate}' (FORMAT PARQUET)")
    con.close()

    result = compare_candidate_cells(base, candidate)

    assert result["blocked_by_reason"] == {"team_signal_cannot_award_player_championship_credit": 1}
    assert result["safe_null_fills_by_field"] == {}
