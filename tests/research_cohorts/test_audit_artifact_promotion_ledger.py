import json
from pathlib import Path

from scripts.research_cohorts.audit_artifact_promotion_ledger import main


def test_ledger_has_a_terminal_reason_for_every_manifest_file(tmp_path: Path, monkeypatch) -> None:
    manifest = tmp_path / "manifest.json"
    manifest.write_text(json.dumps({"files": [
        {"artifact_id": 1, "file": "a.json", "candidate": False,
         "disposition": "metadata_or_validation", "reason": "metadata"},
        {"artifact_id": 1, "file": "b.parquet", "candidate": True,
         "disposition": "team_week_signal_candidate", "reason": "team"},
    ]}))
    ledger = tmp_path / "extract.jsonl"
    ledger.write_text(json.dumps({"artifact_id": 1, "file": "b.parquet",
                                  "status": "candidate_extracted",
                                  "extracted_file": "1__b.parquet"}) + "\n")
    comparison = tmp_path / "comparison"
    comparison.mkdir()
    out = tmp_path / "out.json"
    monkeypatch.setattr("sys.argv", ["audit", "--manifest", str(manifest),
                                      "--extract-ledger", str(ledger),
                                      "--comparison", str(comparison),
                                      "--out", str(out)])
    main()
    result = json.loads(out.read_text())
    assert len(result["files"]) == 2
    statuses = {r["file"]: r["promotion_status"] for r in result["files"]}
    assert statuses["a.json"] == "not_applicable"
    assert statuses["b.parquet"] == "comparison_complete_pending_apply_audit"
