import duckdb

from audit_candidate_canonical_cardinality import audit


def _db(path):
    con = duckdb.connect(str(path))
    con.execute("CREATE SCHEMA public")
    con.execute("""
      CREATE TABLE public.player_fantasy AS
      SELECT 'league'::VARCHAR db_name, 2024::INTEGER AS "year", 1::INTEGER AS "week",
             'nfl'::VARCHAR NFL_player_id, 'manager'::VARCHAR manager,
             NULL::VARCHAR team_key, NULL::VARCHAR team_name, 'sleeper'::VARCHAR platform,
             NULL::INTEGER loss, NULL::INTEGER tie
    """)
    con.close()


def _delta(path):
    con = duckdb.connect()
    con.execute("""
      CREATE TABLE d AS
      SELECT 'league'::VARCHAR db_name, 2024::INTEGER AS "year", 1::INTEGER AS "week",
             'nfl'::VARCHAR NFL_player_id, 'manager'::VARCHAR manager,
             NULL::VARCHAR team_key, NULL::VARCHAR team_name, 'sleeper'::VARCHAR platform,
             0::INTEGER source_loss, 0::INTEGER source_tie
    """)
    con.execute("COPY d TO ? (FORMAT PARQUET)", [str(path)])
    con.close()


def test_audit_reports_duplicate_canonical_recipient(tmp_path):
    base = tmp_path / "base.duckdb"; delta = tmp_path / "delta.parquet"
    _db(base); _delta(delta)
    con = duckdb.connect(str(base)); con.execute("INSERT INTO public.player_fantasy SELECT * FROM public.player_fantasy"); con.close()
    result = audit(base, delta, tmp_path / "audit.json", tmp_path / "examples.json")
    assert result["candidate_keys"] == 1
    assert result["matched_physical_rows"] == 2
    assert result["duplicate_recipient_keys"] == 1
