from __future__ import annotations

from pathlib import Path

from audit_local_mfl_campaigns import audit_campaigns


def test_audit_separates_index_advertised_coverage_from_validated_recovery(tmp_path: Path) -> None:
    campaigns = tmp_path / "campaigns"
    first = campaigns / "100"
    second = campaigns / "200"
    missing_chunk = campaigns / "300"
    for directory in (first, second, missing_chunk):
        directory.mkdir(parents=True)
        (directory / "mfl_register_all_runs.json").write_text("{}", encoding="utf-8")
    (first / "mfl_register_chunk.duckdb").write_bytes(b"chunk")
    (second / "mfl_register_chunk.duckdb").write_bytes(b"chunk")

    identities = {
        first / "mfl_register_all_runs.json": {(2004, "1"), (2004, "2")},
        second / "mfl_register_all_runs.json": {(2004, "2"), (2005, "3")},
        missing_chunk / "mfl_register_all_runs.json": {(2005, "4")},
    }

    report = audit_campaigns(
        campaigns,
        expected={(2004, "1"), (2004, "2"), (2005, "3"), (2005, "4")},
        scan_identities=lambda path: identities[path],
    )

    assert report["campaign_index_count"] == 3
    assert report["chunk_backed_index_count"] == 2
    assert report["index_advertised_expected_count"] == 3
    assert report["missing_from_chunk_backed_indexes"] == [[2005, "4"]]
    assert report["duplicate_chunk_backed_identities"] == {
        "2004|2": [str(first), str(second)]
    }
    assert report["missing_chunk_indexes"] == [str(missing_chunk)]
    assert report["validated_full_schema_identity_count"] == 0
