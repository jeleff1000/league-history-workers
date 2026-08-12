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
    assert result["summary"]["cache_match_cells"] == 74
    assert json.loads(out.read_text(encoding="utf-8"))["rows"][0]["artifact_id"] == "mfl-74-row-recovery-31624673691"
