from __future__ import annotations

import csv
import json
from pathlib import Path


def test_materialize_preserves_all_ledger_rows_and_existing_receipt_counts(tmp_path: Path) -> None:
    from scripts.research_cohorts.materialize_ledger_receipt_inputs import materialize

    source = tmp_path / "ledger.csv"
    fields = [
        "artifact_id", "artifact", "source_cells", "cache_match_cells",
        "cache_missing_cells", "cache_conflict_cells", "unmatched_cache_cells",
        "blocked_schema_cells", "final_status",
    ]
    with source.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerow({
            "artifact_id": "10", "artifact": "source-a", "source_cells": "9",
            "cache_match_cells": "4", "cache_missing_cells": "3",
            "cache_conflict_cells": "1", "unmatched_cache_cells": "1",
            "blocked_schema_cells": "0", "final_status": "still_missing_cache_cells",
        })
        writer.writerow({
            "artifact_id": "11", "artifact": "source-b", "source_cells": "0",
            "cache_match_cells": "0", "cache_missing_cells": "0",
            "cache_conflict_cells": "0", "unmatched_cache_cells": "0",
            "blocked_schema_cells": "0", "final_status": "cache_verified",
        })

    ledger_json = tmp_path / "ledger.json"
    prior_json = tmp_path / "prior.json"
    result = materialize(source, ledger_json, prior_json)

    assert result == {"ledger_rows": 2, "prior_receipt_rows": 2}
    ledger = json.loads(ledger_json.read_text(encoding="utf-8"))
    prior = json.loads(prior_json.read_text(encoding="utf-8"))
    assert [row["artifact_id"] for row in ledger["rows"]] == ["10", "11"]
    assert prior["rows"][0] == {
        "artifact_id": "10", "source_cells": 9, "cache_match_cells": 4,
        "cache_missing_cells": 3, "cache_conflict_cells": 1,
        "unmatched_cache_cells": 1, "blocked_schema_cells": 0,
    }
