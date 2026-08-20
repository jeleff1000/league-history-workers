"""Run immutable MFL reconstruction lanes concurrently and fail closed."""
from __future__ import annotations

import argparse
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any

from scripts.extraplatform_corpus.reconstruct_mfl_recovery_candidate import run_lane


def _identity(entry: dict[str, Any]) -> tuple[int, str]:
    return int(entry["season"]), str(entry["league_id"])


def _validate_plan(plan: dict[str, Any]) -> list[dict[str, Any]]:
    lanes = list(plan.get("lanes", []))
    expected_count = int(plan.get("expected_identity_count", 0))
    if not lanes or expected_count <= 0:
        raise ValueError("lane plan lacks lanes or its immutable expected identity count")
    identities: set[tuple[int, str]] = set()
    for lane in lanes:
        entries = [entry for item in lane.get("items", []) for entry in item.get("entries", [])]
        if int(lane.get("identity_count", -1)) != len(entries):
            raise ValueError(f"lane {lane.get('lane')} identity count does not match its items")
        for entry in entries:
            identity = _identity(entry)
            if identity in identities:
                raise ValueError(f"duplicate immutable identity across lanes: {identity}")
            identities.add(identity)
    if len(identities) != expected_count:
        raise ValueError(
            f"lane plan identity count mismatch expected={expected_count} actual={len(identities)}"
        )
    return lanes


def _lane_output_paths(run_root: Path, ordinal: int) -> tuple[Path, Path, Path]:
    label = f"lane_{ordinal:02d}"
    return (
        run_root / "candidates" / f"{label}.duckdb",
        run_root / "work" / label,
        run_root / "proof" / f"{label}.json",
    )


def run_all_lanes(plan_path: Path, *, run_root: Path, workers: int = 15) -> dict[str, Any]:
    """Create one fresh run root and validate all lane candidates before success."""
    plan_path = Path(plan_path)
    run_root = Path(run_root)
    if run_root.exists():
        raise FileExistsError(f"refusing to reuse an existing reconstruction run root: {run_root}")
    if workers < 1:
        raise ValueError("workers must be positive")
    plan = json.loads(plan_path.read_text(encoding="utf-8"))
    lanes = _validate_plan(plan)
    run_root.mkdir(parents=True)
    started_path = run_root / "RUN_STARTED.json"
    with started_path.open("x", encoding="utf-8") as handle:
        json.dump(
            {
                "plan_path": str(plan_path),
                "expected_identity_count": int(plan["expected_identity_count"]),
                "lane_count": len(lanes),
                "workers": min(workers, len(lanes)),
            },
            handle,
            sort_keys=True,
            indent=2,
        )

    results: list[dict[str, Any]] = []
    failures: list[dict[str, str]] = []
    with ThreadPoolExecutor(max_workers=min(workers, len(lanes))) as executor:
        futures = {}
        for ordinal, lane in enumerate(lanes):
            candidate_path, work_dir, proof_path = _lane_output_paths(run_root, ordinal)
            futures[executor.submit(run_lane, lane, candidate_path=candidate_path, work_dir=work_dir, proof_path=proof_path)] = ordinal
        for future in as_completed(futures):
            ordinal = futures[future]
            try:
                results.append(future.result())
            except Exception as error:  # retain all independent lane diagnostics
                failures.append({"lane_ordinal": str(ordinal), "error": f"{type(error).__name__}: {error}"})

    if failures:
        failed_path = run_root / "RUN_FAILED.json"
        with failed_path.open("x", encoding="utf-8") as handle:
            json.dump({"ok": False, "failures": sorted(failures, key=lambda item: item["lane_ordinal"])}, handle, sort_keys=True, indent=2)
        raise RuntimeError(f"MFL reconstruction failed in {len(failures)} lane(s); see {failed_path}")

    identity_count = sum(int(result["expected_identity_count"]) for result in results)
    if identity_count != int(plan["expected_identity_count"]):
        raise RuntimeError(
            f"completed lane identity count mismatch expected={plan['expected_identity_count']} actual={identity_count}"
        )
    report = {
        "ok": True,
        "plan_path": str(plan_path),
        "expected_identity_count": int(plan["expected_identity_count"]),
        "identity_count": identity_count,
        "lane_count": len(results),
        "lane_candidates": sorted(
            [{"lane": result["lane"], "path": result["candidate_path"], "sha256": result["candidate_sha256"]} for result in results],
            key=lambda item: str(item["lane"]),
        ),
    }
    success_path = run_root / "ALL_LANES_OK.json"
    with success_path.open("x", encoding="utf-8") as handle:
        json.dump(report, handle, sort_keys=True, indent=2)
    return report


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--lane-plan", type=Path, required=True)
    parser.add_argument("--run-root", type=Path, required=True)
    parser.add_argument("--workers", type=int, default=15)
    args = parser.parse_args()
    report = run_all_lanes(args.lane_plan, run_root=args.run_root, workers=args.workers)
    print(json.dumps(report, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
