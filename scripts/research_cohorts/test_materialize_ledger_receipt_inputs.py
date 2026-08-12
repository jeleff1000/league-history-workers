import csv
import json

from materialize_ledger_receipt_inputs import materialize


def test_receipt_inputs_exclude_supplemental_cache_recovery_records(tmp_path):
    ledger = tmp_path / "ledger.csv"
    fields = ["record_type", "artifact_id", "source_cells", "cache_match_cells", "cache_missing_cells", "cache_conflict_cells", "unmatched_cache_cells", "blocked_schema_cells"]
    with ledger.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows([
            {"record_type": "artifact_inventory", "artifact_id": "1", "source_cells": "4", "cache_match_cells": "4", "cache_missing_cells": "0", "cache_conflict_cells": "0", "unmatched_cache_cells": "0", "blocked_schema_cells": "0"},
            {"record_type": "cache_recovery_receipt", "artifact_id": "mfl-74-row-recovery-31624673691", "source_cells": "74", "cache_match_cells": "74", "cache_missing_cells": "0", "cache_conflict_cells": "0", "unmatched_cache_cells": "0", "blocked_schema_cells": "0"},
        ])
    ledger_json = tmp_path / "ledger.json"
    prior_receipts = tmp_path / "prior.json"

    assert materialize(ledger, ledger_json, prior_receipts) == {
        "ledger_rows": 2,
        "raw_artifact_rows": 1,
        "prior_receipt_rows": 1,
    }
    assert len(json.loads(ledger_json.read_text())["rows"]) == 2
    assert json.loads(prior_receipts.read_text())["rows"] == [{
        "artifact_id": "1",
        "source_cells": 4,
        "cache_match_cells": 4,
        "cache_missing_cells": 0,
        "cache_conflict_cells": 0,
        "unmatched_cache_cells": 0,
        "blocked_schema_cells": 0,
    }]
