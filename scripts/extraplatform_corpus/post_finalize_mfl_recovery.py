"""Fail-closed, unattended handoff from remote MFL finalization to local proof.

The recovery server does the I/O-heavy 15-lane reconstruction.  This local
controller is deliberately boring: wait for the one final receipt, copy only
the three finalized artifacts into a *new* D: staging directory, verify every
copy by SHA-256, then invoke the existing local merge and proof validators.
No source cache, artifact, or prior candidate is overwritten or cleaned up.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import shlex
import subprocess
import sys
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any


EXPECTED_MFL_LEAGUE_YEARS = 39_368
EXPECTED_LEDGER_JSON = json.dumps(
    {
        "2004": 4780,
        "2005": 935,
        "2006": 7719,
        "2007": 2136,
        "2008": 2195,
        "2009": 2133,
        "2010": 2155,
        "2011": 2170,
        "2012": 1801,
        "2013": 1288,
        "2014": 865,
        "2015": 836,
        "2016": 9114,
        "2017": 468,
        "2018": 773,
    },
    sort_keys=True,
)


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(8 * 1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def validate_finalization_receipt(receipt: dict[str, Any]) -> dict[str, Any]:
    """Accept only a final receipt proving the complete 39,368 identity set."""
    if receipt.get("ok") is not True:
        raise RuntimeError("finalization receipt is not marked ok")
    if int(receipt.get("identity_count", -1)) != EXPECTED_MFL_LEAGUE_YEARS:
        raise RuntimeError("finalization receipt has the wrong identity count")
    for key in ("candidate_path", "index_path", "proof_path"):
        value = receipt.get(key)
        if not isinstance(value, str) or not value.startswith("/"):
            raise RuntimeError(f"finalization receipt has no safe remote {key}")
    candidate_sha256 = str(receipt.get("candidate_sha256", ""))
    if len(candidate_sha256) != 64 or any(ch not in "0123456789abcdef" for ch in candidate_sha256.lower()):
        raise RuntimeError("finalization receipt has an invalid candidate SHA-256")
    return receipt


def _fresh_output_root(output_root: Path) -> None:
    if output_root.exists():
        raise FileExistsError(f"handoff output must be fresh; refusing to reuse {output_root}")
    output_root.mkdir(parents=True)


def _write_new_json(path: Path, payload: dict[str, Any]) -> None:
    with path.open("x", encoding="utf-8") as handle:
        json.dump(payload, handle, sort_keys=True, indent=2)


def handoff_finalized_recovery(
    *,
    receipt: dict[str, Any],
    finalization_receipt_path: str,
    output_root: Path,
    remote_sha256: Callable[[str], str],
    copy_remote: Callable[[str, Path], None],
    run_command: Callable[[list[str]], None],
    python_executable: str,
    repo_root: Path,
    base_path: Path,
    ops_path: Path,
) -> dict[str, Any]:
    """Copy a final remote set, verify it, then run the existing local gates."""
    receipt = validate_finalization_receipt(receipt)
    output_root = Path(output_root)
    _fresh_output_root(output_root)
    recovered_dir = output_root / "recovered"
    merged_dir = output_root / "merged"
    proof_dir = output_root / "proof"
    recovered_dir.mkdir()
    merged_dir.mkdir()

    remote_files = {
        "finalization_receipt.json": finalization_receipt_path,
        "mfl_reconstructed.duckdb": str(receipt["candidate_path"]),
        "mfl_register_all_runs.json": str(receipt["index_path"]),
        "finalize_proof.json": str(receipt["proof_path"]),
    }
    transfer_manifest: dict[str, Any] = {"identity_count": EXPECTED_MFL_LEAGUE_YEARS, "files": {}}
    for local_name, remote_path in remote_files.items():
        expected = remote_sha256(remote_path).strip().lower()
        if len(expected) != 64:
            raise RuntimeError(f"remote SHA-256 is malformed for {remote_path}")
        destination = recovered_dir / local_name
        copy_remote(remote_path, destination)
        actual = sha256(destination)
        if actual != expected:
            raise RuntimeError(f"copied artifact SHA-256 mismatch for {local_name}")
        transfer_manifest["files"][local_name] = {
            "remote_path": remote_path,
            "sha256": actual,
            "bytes": destination.stat().st_size,
        }
    if transfer_manifest["files"]["mfl_reconstructed.duckdb"]["sha256"] != receipt["candidate_sha256"]:
        raise RuntimeError("candidate hash does not match the finalization receipt")
    _write_new_json(output_root / "transfer_manifest.json", transfer_manifest)

    merged_candidate = merged_dir / "corpus_snapshot_merged_mfl.duckdb"
    merge_script = repo_root / "scripts" / "research_cohorts" / "merge_research_with_mfl.py"
    validate_script = repo_root / "scripts" / "research_cohorts" / "validate_merged_mfl_candidate.py"
    run_command([
        python_executable,
        str(merge_script),
        "--base", str(base_path),
        "--mfl", str(recovered_dir / "mfl_reconstructed.duckdb"),
        "--mfl-index", str(recovered_dir / "mfl_register_all_runs.json"),
        "--out", str(merged_candidate),
        "--expected-ledger", EXPECTED_LEDGER_JSON,
    ])
    run_command([
        python_executable,
        str(validate_script),
        "--base", str(base_path),
        "--recovered", str(recovered_dir / "mfl_reconstructed.duckdb"),
        "--recovered-index", str(recovered_dir / "mfl_register_all_runs.json"),
        "--candidate", str(merged_candidate),
        "--ops", str(ops_path),
        "--recovery-proof", str(recovered_dir / "finalize_proof.json"),
        "--proof-dir", str(proof_dir),
    ])
    report = {
        "ok": True,
        "identity_count": EXPECTED_MFL_LEAGUE_YEARS,
        "output_root": str(output_root),
        "merged_candidate": str(merged_candidate),
        "proof_dir": str(proof_dir),
        "candidate_sha256": receipt["candidate_sha256"],
    }
    _write_new_json(output_root / "HANDOFF_OK.json", report)
    return report


def _ssh_json(host: str, identity_file: Path, remote_path: str) -> dict[str, Any]:
    command = ["ssh", "-i", str(identity_file), "-o", "BatchMode=yes", host, f"cat -- {shlex.quote(remote_path)}"]
    result = subprocess.run(command, check=True, capture_output=True, text=True)
    return json.loads(result.stdout)


def _remote_exists(host: str, identity_file: Path, remote_path: str) -> bool:
    command = ["ssh", "-i", str(identity_file), "-o", "BatchMode=yes", host, f"test -f {shlex.quote(remote_path)}"]
    return subprocess.run(command, check=False).returncode == 0


def _remote_sha256(host: str, identity_file: Path, remote_path: str) -> str:
    command = ["ssh", "-i", str(identity_file), "-o", "BatchMode=yes", host, f"sha256sum -- {shlex.quote(remote_path)}"]
    result = subprocess.run(command, check=True, capture_output=True, text=True)
    return result.stdout.split(maxsplit=1)[0]


def _copy_remote(host: str, identity_file: Path, source: str, destination: Path) -> None:
    subprocess.run(
        ["scp", "-i", str(identity_file), "-o", "BatchMode=yes", f"{host}:{source}", str(destination)],
        check=True,
    )


def wait_for_finalization(
    *, host: str, identity_file: Path, run_root: str, poll_seconds: float
) -> tuple[dict[str, Any], str]:
    """Poll only immutable terminal receipts; a failed run is terminal failure."""
    if poll_seconds <= 0:
        raise ValueError("poll seconds must be positive")
    success = f"{run_root.rstrip('/')}/FINALIZATION_OK.json"
    failed = f"{run_root.rstrip('/')}/FINALIZATION_FAILED.json"
    while True:
        if _remote_exists(host, identity_file, failed):
            failed_receipt = _ssh_json(host, identity_file, failed)
            raise RuntimeError(f"remote finalization failed: {failed_receipt}")
        if _remote_exists(host, identity_file, success):
            return validate_finalization_receipt(_ssh_json(host, identity_file, success)), success
        time.sleep(poll_seconds)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", required=True)
    parser.add_argument("--identity-file", type=Path, required=True)
    parser.add_argument("--run-root", required=True)
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument("--repo-root", type=Path, required=True)
    parser.add_argument("--base", type=Path, required=True)
    parser.add_argument("--ops", type=Path, required=True)
    parser.add_argument("--poll-seconds", type=float, default=60.0)
    args = parser.parse_args()
    if args.output_root.drive.upper() != "D:":
        raise SystemExit("output-root must be on D:; no recovery output may land on C:")
    if args.output_root.exists():
        raise SystemExit(f"output-root must be new evidence, not an existing path: {args.output_root}")
    for required in (args.identity_file, args.base, args.ops):
        if not required.is_file():
            raise SystemExit(f"required source is missing: {required}")
    receipt, receipt_path = wait_for_finalization(
        host=args.host,
        identity_file=args.identity_file,
        run_root=args.run_root,
        poll_seconds=args.poll_seconds,
    )
    try:
        report = handoff_finalized_recovery(
            receipt=receipt,
            finalization_receipt_path=receipt_path,
            output_root=args.output_root,
            remote_sha256=lambda path: _remote_sha256(args.host, args.identity_file, path),
            copy_remote=lambda source, destination: _copy_remote(args.host, args.identity_file, source, destination),
            run_command=lambda command: subprocess.run(command, check=True),
            python_executable=sys.executable,
            repo_root=args.repo_root,
            base_path=args.base,
            ops_path=args.ops,
        )
    except Exception as error:
        if args.output_root.exists():
            failed = args.output_root / "HANDOFF_FAILED.json"
            if not failed.exists():
                _write_new_json(failed, {"ok": False, "error": f"{type(error).__name__}: {error}"})
        raise
    print(json.dumps(report, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
