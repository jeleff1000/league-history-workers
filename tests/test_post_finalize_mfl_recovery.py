from __future__ import annotations

import hashlib
import json
from pathlib import Path


def test_validate_finalization_receipt_requires_full_proven_population() -> None:
    from scripts.extraplatform_corpus.post_finalize_mfl_recovery import (
        EXPECTED_MFL_LEAGUE_YEARS,
        validate_finalization_receipt,
    )

    receipt = {
        "ok": True,
        "identity_count": EXPECTED_MFL_LEAGUE_YEARS,
        "candidate_path": "/remote/candidate.duckdb",
        "index_path": "/remote/index.json",
        "proof_path": "/remote/proof.json",
        "candidate_sha256": "a" * 64,
    }

    assert validate_finalization_receipt(receipt)["identity_count"] == 39_368

    receipt["identity_count"] = 39_367
    try:
        validate_finalization_receipt(receipt)
    except RuntimeError as error:
        assert "identity count" in str(error)
    else:
        raise AssertionError("partial finalization receipt must fail closed")


def test_handoff_verifies_transfer_before_merge_and_validation(tmp_path: Path) -> None:
    from scripts.extraplatform_corpus.post_finalize_mfl_recovery import (
        EXPECTED_LEDGER_JSON,
        handoff_finalized_recovery,
    )

    payloads = {
        "/remote/candidate.duckdb": b"candidate",
        "/remote/index.json": b'{"league_count": 39368}',
        "/remote/proof.json": b'{"ok": true, "identity_count": 39368}',
        "/remote/receipt.json": b'{"ok": true}',
    }
    receipt = {
        "ok": True,
        "identity_count": 39_368,
        "candidate_path": "/remote/candidate.duckdb",
        "index_path": "/remote/index.json",
        "proof_path": "/remote/proof.json",
        "candidate_sha256": hashlib.sha256(payloads["/remote/candidate.duckdb"]).hexdigest(),
    }
    calls: list[list[str]] = []

    def remote_sha(path: str) -> str:
        return hashlib.sha256(payloads[path]).hexdigest()

    def copy_remote(source: str, destination: Path) -> None:
        destination.write_bytes(payloads[source])

    def run_command(command: list[str]) -> None:
        calls.append(command)

    result = handoff_finalized_recovery(
        receipt=receipt,
        finalization_receipt_path="/remote/receipt.json",
        output_root=tmp_path / "fresh-output",
        remote_sha256=remote_sha,
        copy_remote=copy_remote,
        run_command=run_command,
        python_executable="python",
        repo_root=tmp_path / "repo",
        base_path=tmp_path / "base.duckdb",
        ops_path=tmp_path / "ops.duckdb",
    )

    assert result["ok"] is True
    assert json.loads((tmp_path / "fresh-output" / "HANDOFF_OK.json").read_text(encoding="utf-8"))["identity_count"] == 39_368
    assert len(calls) == 2
    assert calls[0][1].endswith("merge_research_with_mfl.py")
    assert "--expected-ledger" in calls[0]
    assert EXPECTED_LEDGER_JSON in calls[0]
    assert calls[1][1].endswith("validate_merged_mfl_candidate.py")


def test_handoff_refuses_existing_or_corrupt_destination(tmp_path: Path) -> None:
    from scripts.extraplatform_corpus.post_finalize_mfl_recovery import handoff_finalized_recovery

    existing = tmp_path / "existing"
    existing.mkdir()
    receipt = {
        "ok": True,
        "identity_count": 39_368,
        "candidate_path": "/remote/candidate.duckdb",
        "index_path": "/remote/index.json",
        "proof_path": "/remote/proof.json",
        "candidate_sha256": "a" * 64,
    }
    try:
        handoff_finalized_recovery(
            receipt=receipt,
            finalization_receipt_path="/remote/receipt.json",
            output_root=existing,
            remote_sha256=lambda _: "a" * 64,
            copy_remote=lambda *_: None,
            run_command=lambda _: None,
            python_executable="python",
            repo_root=tmp_path,
            base_path=tmp_path / "base.duckdb",
            ops_path=tmp_path / "ops.duckdb",
        )
    except FileExistsError as error:
        assert "fresh" in str(error)
    else:
        raise AssertionError("existing output must never be reused")
