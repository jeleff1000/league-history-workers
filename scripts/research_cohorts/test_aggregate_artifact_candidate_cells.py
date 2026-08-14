import json

from aggregate_artifact_candidate_cells import aggregate_candidate_cells


def test_aggregate_records_missing_receipt_ids_without_silently_accepting_partial_coverage(tmp_path):
    shard = tmp_path / "shards" / "candidate-0" / "receipts"
    shard.mkdir(parents=True)
    (shard / "101.json").write_text(json.dumps({"artifact_id": "101"}))

    result = aggregate_candidate_cells(
        shard_root=tmp_path / "shards",
        expected_artifact_ids={"101", "102"},
        out_parquet=tmp_path / "cells.parquet",
    )

    assert result["receipt_coverage_complete"] is False
    assert result["missing_artifact_ids"] == ["102"]
    assert result["cache_mutated"] is False
    assert result["new_lineage"] is False
