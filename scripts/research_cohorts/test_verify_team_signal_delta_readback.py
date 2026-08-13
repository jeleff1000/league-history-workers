import hashlib
import json

import duckdb

from verify_team_signal_delta_readback import verify


def test_readback_verifies_has_po_signal_null_fill(tmp_path):
    base = tmp_path / "base.duckdb"
    delta = tmp_path / "delta.parquet"
    before = tmp_path / "before.json"
    ops = tmp_path / "ops.duckdb"
    ops_before = tmp_path / "ops_before.sha256"

    con = duckdb.connect(str(base))
    con.execute("CREATE SCHEMA public")
    con.execute("""
        CREATE TABLE public.player_fantasy (
          db_name VARCHAR, year INTEGER, week INTEGER, NFL_player_id VARCHAR,
          manager VARCHAR, team_key VARCHAR, team_name VARCHAR, platform VARCHAR,
          win INTEGER, loss INTEGER, tie INTEGER, team_points DOUBLE,
          is_playoffs INTEGER, champion INTEGER, final_playoff_seed INTEGER,
          made_playoffs INTEGER, has_po_signal INTEGER
        )
    """)
    con.execute("""
        INSERT INTO public.player_fantasy VALUES
        ('league', 2017, 15, 'nfl', NULL, NULL, NULL, 'sleeper',
         NULL, NULL, NULL, NULL, 1, 1, NULL, NULL, 1)
    """)
    schema = con.execute("DESCRIBE public.player_fantasy").fetchall()
    con.execute("""
      COPY (
        SELECT 'league'::VARCHAR AS db_name, 2017::INTEGER AS year, 15::INTEGER AS week,
          'nfl'::VARCHAR AS NFL_player_id, NULL::VARCHAR AS manager, NULL::VARCHAR AS team_key,
          NULL::VARCHAR AS team_name, 'sleeper'::VARCHAR AS platform,
          NULL::INTEGER source_win, NULL::INTEGER source_loss, NULL::INTEGER source_tie,
          NULL::DOUBLE source_team_points, NULL::INTEGER source_playoffs,
          NULL::INTEGER source_champion, NULL::INTEGER source_final_playoff_seed,
          1::INTEGER source_has_po_signal, NULL::INTEGER source_made_playoffs,
          NULL::DOUBLE source_clutch_equity,
          NULL::INTEGER canonical_win, NULL::INTEGER canonical_loss,
          NULL::INTEGER canonical_tie, NULL::DOUBLE canonical_team_points,
          NULL::INTEGER canonical_is_playoffs, NULL::INTEGER canonical_champion,
          NULL::INTEGER canonical_final_playoff_seed, NULL::INTEGER canonical_made_playoffs,
          NULL::INTEGER canonical_has_po_signal
      ) TO ? (FORMAT PARQUET)
    """, [str(delta)])
    con.close()
    duckdb.connect(str(ops)).close()
    before.write_text(json.dumps({"schema": schema, "player_rows": 1}), encoding="utf-8")
    ops_before.write_text(hashlib.sha256(ops.read_bytes()).hexdigest() + "  ops.duckdb\n", encoding="utf-8")

    result = verify(base=base, delta=delta, before=before, ops=ops, ops_before=ops_before)

    assert result["authorized_cells_by_field"] == {"has_po_signal": 1}
    assert result["source_value_disagreements"]["has_po_signal"] == 0
