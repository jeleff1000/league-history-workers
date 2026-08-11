import csv
import json
from pathlib import Path


def test_refresh_replaces_only_selected_artifact_receipt_fields(tmp_path: Path) -> None:
    from scripts.research_cohorts.refresh_artifact_cache_ledger import refresh

    fields = [
        "receipt_run_id", "canonical_cache_key", "receipt_json_sha256", "artifact_id",
        "artifact", "workflow_run_id", "dispositions", "candidate_delta_rows",
        "candidate_fields", "candidate_file_count", "candidate_files", "source_cells",
        "cache_match_cells", "cache_missing_cells", "cache_conflict_cells",
        "unmatched_cache_cells", "blocked_schema_cells", "final_status", "final_reason",
        "next_action",
    ]
    ledger = tmp_path / "ledger.csv"
    rows = [
        dict.fromkeys(fields, ""),
        dict.fromkeys(fields, ""),
    ]
    rows[0].update(artifact_id="1", final_status="old", source_cells="0")
    rows[1].update(artifact_id="2", final_status="untouched", source_cells="9")
    with ledger.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)
    receipt = tmp_path / "receipt.json"
    receipt.write_text(json.dumps({"rows": [{
        "artifact_id": 1, "source_cells": 5, "cache_match_cells": 2,
        "cache_missing_cells": 1, "cache_conflict_cells": 0,
        "unmatched_cache_cells": 0, "blocked_schema_cells": 2,
        "final_status": "partial_schema_blocked_cache_updates",
        "final_reason": "safe updates plus blocked fields",
    }]}))
    selected = tmp_path / "selected.json"
    selected.write_text(json.dumps([{ "artifact_id": 1 }]))
    out = tmp_path / "out.csv"

    assert refresh(
        ledger_path=ledger, receipt_path=receipt, selected_path=selected,
        receipt_run_id="99", out_path=out,
    ) == {"ledger_rows": 2, "updated_rows": 1}

    result = list(csv.DictReader(out.open(newline="", encoding="utf-8")))
    assert result[0]["receipt_run_id"] == "99"
    assert result[0]["cache_missing_cells"] == "1"
    assert result[0]["next_action"] == "apply_supported_null_cell_updates; separately_adjudicate_blocked_schema_fields"
    assert result[1]["final_status"] == "untouched"
    assert result[1]["source_cells"] == "9"
