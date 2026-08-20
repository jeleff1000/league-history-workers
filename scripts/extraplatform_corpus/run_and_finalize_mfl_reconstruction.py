"""One-command, fail-closed MFL reconstruction and finalization."""
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from scripts.extraplatform_corpus.finalize_completed_mfl_reconstruction import (
    finalize_completed_lanes,
)
from scripts.extraplatform_corpus.run_mfl_reconstruction_lanes import run_all_lanes


def run_and_finalize(
    *,
    lane_plan_path: Path,
    run_root: Path,
    workers: int,
    candidate_path: Path,
    index_path: Path,
    proof_path: Path,
) -> dict[str, Any]:
    """Run a fresh lane plan then finalize its proven terminal output.

    Any lane exception returns immediately and therefore cannot invoke the
    finalizer.  Both child operations independently reject reused output paths.
    """
    lanes = run_all_lanes(lane_plan_path, run_root=run_root, workers=workers)
    if not lanes.get("ok"):
        raise RuntimeError("lane executor returned a non-success report")
    final = finalize_completed_lanes(
        run_root=run_root,
        lane_plan_path=lane_plan_path,
        candidate_path=candidate_path,
        index_path=index_path,
        proof_path=proof_path,
    )
    if not final.get("ok"):
        raise RuntimeError("finalizer returned a non-success report")
    if int(final["identity_count"]) != int(lanes["identity_count"]):
        raise RuntimeError("final identity count does not match the lane success receipt")
    return {
        "ok": True,
        "run_root": str(run_root),
        "identity_count": int(final["identity_count"]),
        "candidate_path": str(candidate_path),
        "index_path": str(index_path),
        "proof_path": str(proof_path),
        "candidate_sha256": str(final["candidate_sha256"]),
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--lane-plan", type=Path, required=True)
    parser.add_argument("--run-root", type=Path, required=True)
    parser.add_argument("--workers", type=int, default=15)
    parser.add_argument("--candidate", type=Path, required=True)
    parser.add_argument("--index", type=Path, required=True)
    parser.add_argument("--proof", type=Path, required=True)
    args = parser.parse_args()
    report = run_and_finalize(
        lane_plan_path=args.lane_plan,
        run_root=args.run_root,
        workers=args.workers,
        candidate_path=args.candidate,
        index_path=args.index,
        proof_path=args.proof,
    )
    print(json.dumps(report, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
