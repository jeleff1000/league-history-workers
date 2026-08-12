import json

import duckdb

from finalize_artifact_cache_receipts import build_receipts


def test_receipts_preserve_non_numeric_supplemental_cache_recovery_record(tmp_path):
    base = tmp_path / "base.duckdb"
    duckdb.connect(str(base)).close()
    ledger = tmp_path / "ledger.json"
    out = tmp_path / "receipts.json"
    ledger.write_text(json.dumps({"rows": [{
        "record_type": "cache_recovery_receipt",
        "artifact_id": "mfl-74-row-recovery-31624673691",
        "source_cells": "74",
        "cache_match_cells": "74",
        "cache_missing_cells": "0",
        "cache_conflict_cells": "0",
        "unmatched_cache_cells": "0",
        "blocked_schema_cells": "0",
        "final_status": "cache_verified",
    }]}), encoding="utf-8")

    result = build_receipts(
        ledger_path=ledger,
        base_path=base,
        team_delta=None,
        exact_delta=None,
        structured_delta=None,
        settings_delta=None,
        out_path=out,
    )

    assert result["summary"]["ledger_rows"] == 1
    assert result["summary"]["raw_artifact_rows"] == 0
    assert result["summary"]["supplemental_cache_recovery_receipt_rows"] == 1
    assert result["summary"]["cache_match_cells"] == 74
    assert json.loads(out.read_text(encoding="utf-8"))["rows"][0]["artifact_id"] == "mfl-74-row-recovery-31624673691"


def test_raw_team_receipt_recomputes_closed_gates_after_cache_readback(tmp_path):
    """A fresh raw-team readback must replace stale schema/bridge gate states."""
    base = tmp_path / "base.duckdb"
    con = duckdb.connect(str(base))
    con.execute("CREATE SCHEMA public")
    con.execute("""
        CREATE TABLE public.player_fantasy (
            db_name VARCHAR, year INTEGER, week INTEGER, NFL_player_id VARCHAR,
            platform VARCHAR, manager VARCHAR, team_key VARCHAR, team_name VARCHAR,
            win BIGINT, loss BIGINT, tie BIGINT, team_points DOUBLE,
            is_playoffs BIGINT, champion BIGINT, final_playoff_seed BIGINT,
            made_playoffs BIGINT
        )
    """)
    con.execute("""
        INSERT INTO public.player_fantasy VALUES
        ('smpl_mfl_2015_15345', 2013, 15, 'nfl-1', 'mfl', 'TRIP CHERRIES', NULL, NULL,
         1, 0, 0, 127.5, 1, 1, 1, 1)
    """)
    source = tmp_path / "team_signal.parquet"
    con.execute("""
        COPY (
          SELECT 'smpl_mfl_2015_15345'::VARCHAR AS db_name, 2013::INTEGER AS year,
                 15::INTEGER AS week, '0005'::VARCHAR AS team_key,
                 '0005'::VARCHAR AS manager_guid, NULL::VARCHAR AS franchise_id,
                 'mfl'::VARCHAR AS platform, 'Trip Cherries'::VARCHAR AS manager,
                 1::BIGINT AS win, 0::BIGINT AS loss, 0::BIGINT AS tie,
                 127.5::DOUBLE AS team_points, 1::BIGINT AS is_playoffs,
                 1::BIGINT AS champion, 1::BIGINT AS is_championship,
                 1::BIGINT AS final_playoff_seed
        ) TO ? (FORMAT PARQUET)
    """, [str(source)])
    con.close()

    ledger = tmp_path / "ledger.json"
    out = tmp_path / "receipts.json"
    manifest = tmp_path / "manifest.json"
    ledger.write_text(json.dumps({"rows": [{
        "record_type": "artifact_inventory",
        "artifact_id": "1",
        "final_status": "partial_schema_blocked_unmatched",
        "loss_tie_gate": "open_schema_required",
        "player_team_bridge_gate": "open_raw_roster_bridge_required",
    }]}), encoding="utf-8")
    manifest.write_text(json.dumps([{"artifact_id": 1, "path": str(source)}]), encoding="utf-8")

    result = build_receipts(
        ledger_path=ledger,
        base_path=base,
        team_delta=None,
        exact_delta=None,
        structured_delta=None,
        settings_delta=None,
        team_signal_manifest=manifest,
        out_path=out,
    )

    row = result["rows"][0]
    assert row["final_status"] == "cache_verified"
    assert row["cache_missing_cells"] == 0
    assert row["cache_conflict_cells"] == 0
    assert row["unmatched_cache_cells"] == 0
    assert row["blocked_schema_cells"] == 0
    assert row["loss_tie_gate"] == "not_required"
    assert row["player_team_bridge_gate"] == "not_required"
    assert row["source_precedence_gate"] == "not_required"
    assert row["cache_admission_state"] == "closed_cache_verified"
