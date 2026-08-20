"""Finalize a proven MFL lane run without manual intervention.

The lane executor and finalizer intentionally have separate responsibilities.
This controller bridges them only at the durable ``ALL_LANES_OK.json`` gate.
It never treats a running process, a partial candidate, or an absent marker as
success, and it never overwrites a previous terminal receipt.
"""
from __future__ import annotations

import argparse
import json
import time
from pathlib import Path
from typing import Any

from scripts.extraplatform_corpus.finalize_mfl_reconstruction_lanes import finalize_lanes


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _terminal_lane_status(run_root: Path) -> dict[str, Any] | None:
    """Return the only lane states that permit a terminal decision."""
    failed_path = run_root / "RUN_FAILED.json"
    success_path = run_root / "ALL_LANES_OK.json"
    if failed_path.is_file() and success_path.is_file():
        raise RuntimeError("lane run has contradictory success and failure markers")
    if failed_path.is_file():
        return {"state": "failed", "receipt": _read_json(failed_path)}
    if success_path.is_file():
        return {"state": "succeeded", "receipt": _read_json(success_path)}
    return None


def wait_for_lane_terminal(run_root: Path, *, poll_seconds: float) -> dict[str, Any]:
    """Wait until lane execution writes one unambiguous terminal receipt."""
    if poll_seconds <= 0:
        raise ValueError("poll_seconds must be positive")
    while True:
        status = _terminal_lane_status(run_root)
        if status is not None:
            return status
        time.sleep(poll_seconds)


def finalize_completed_lanes(
    *,
    run_root: Path,
    lane_plan_path: Path,
    candidate_path: Path,
    index_path: Path,
    proof_path: Path,
) -> dict[str, Any]:
    """Finalize exactly one successful lane run and write a durable receipt."""
    run_root = Path(run_root)
    terminal_ok = run_root / "FINALIZATION_OK.json"
    terminal_failed = run_root / "FINALIZATION_FAILED.json"
    if terminal_ok.is_file():
        receipt = _read_json(terminal_ok)
        if not receipt.get("ok"):
            raise RuntimeError(f"invalid finalization success receipt: {terminal_ok}")
        return receipt
    if terminal_failed.is_file():
        raise RuntimeError(f"previous finalization failed; inspect {terminal_failed}")

    status = _terminal_lane_status(run_root)
    if status is None:
        raise RuntimeError("lane run is not complete; ALL_LANES_OK.json is required")
    if status["state"] == "failed":
        raise RuntimeError("lane run failed; refusing finalization")
    lane_receipt = status["receipt"]
    if not lane_receipt.get("ok"):
        raise RuntimeError("lane success receipt is not marked ok")

    try:
        final = finalize_lanes(run_root, lane_plan_path, candidate_path, index_path, proof_path)
        if not final.get("ok"):
            raise RuntimeError("finalizer returned a non-success report")
        if int(final["identity_count"]) != int(lane_receipt["identity_count"]):
            raise RuntimeError(
                "finalization identity count does not match the all-lanes success receipt"
            )
        receipt = {
            "ok": True,
            "run_root": str(run_root),
            "identity_count": int(final["identity_count"]),
            "candidate_path": str(candidate_path),
            "index_path": str(index_path),
            "proof_path": str(proof_path),
            "candidate_sha256": str(final["candidate_sha256"]),
        }
        with terminal_ok.open("x", encoding="utf-8") as handle:
            json.dump(receipt, handle, sort_keys=True, indent=2)
        return receipt
    except Exception as error:
        failure = {"ok": False, "error": f"{type(error).__name__}: {error}"}
        with terminal_failed.open("x", encoding="utf-8") as handle:
            json.dump(failure, handle, sort_keys=True, indent=2)
        raise


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-root", type=Path, required=True)
    parser.add_argument("--lane-plan", type=Path, required=True)
    parser.add_argument("--candidate", type=Path, required=True)
    parser.add_argument("--index", type=Path, required=True)
    parser.add_argument("--proof", type=Path, required=True)
    parser.add_argument("--poll-seconds", type=float, default=60.0)
    args = parser.parse_args()
    status = wait_for_lane_terminal(args.run_root, poll_seconds=args.poll_seconds)
    if status["state"] == "failed":
        raise RuntimeError("lane run failed; refusing finalization")
    report = finalize_completed_lanes(
        run_root=args.run_root,
        lane_plan_path=args.lane_plan,
        candidate_path=args.candidate,
        index_path=args.index,
        proof_path=args.proof,
    )
    print(json.dumps(report, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
