"""Plan deterministic, byte-balanced recovery lanes for original MFL artifacts."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Iterable


def _normalise_artifact(raw: dict[str, object], run_id: int) -> dict[str, object]:
    try:
        artifact_id = int(raw["artifact_id"])
        size = int(raw["size_in_bytes"])
    except (KeyError, TypeError, ValueError) as exc:
        raise ValueError(f"artifact on run {run_id} lacks a numeric id or size") from exc
    if artifact_id <= 0 or size < 0:
        raise ValueError(f"artifact on run {run_id} has an invalid id or size")
    if raw.get("expired") is True:
        raise ValueError(f"expired source artifact cannot enter a recovery lane: {artifact_id}")
    name = str(raw.get("name", ""))
    digest = str(raw.get("digest", ""))
    if not name or not digest.startswith("sha256:") or len(digest) != 71:
        raise ValueError(f"artifact {artifact_id} lacks required provenance")
    return {
        "artifact_id": artifact_id,
        "run_id": run_id,
        "name": name,
        "size_in_bytes": size,
        "digest": digest,
    }


def build_lane_plan(
    inventory: dict[str, Any], *, lane_count: int, download_wave_size: int = 5
) -> dict[str, object]:
    """Assign every available source artifact to one deterministic byte-balanced lane."""

    if lane_count <= 0:
        raise ValueError("lane_count must be positive")
    if download_wave_size <= 0:
        raise ValueError("download_wave_size must be positive")
    if int(inventory.get("run_count", 0)) != 177:
        raise ValueError(f"expected 177 original campaign runs, found {inventory.get('run_count')!r}")
    raw_runs = inventory.get("runs")
    if not isinstance(raw_runs, list):
        raise ValueError("inventory has no run list")

    artifacts: list[dict[str, object]] = []
    seen_artifact_ids: set[int] = set()
    for raw_run in raw_runs:
        if not isinstance(raw_run, dict):
            raise ValueError("inventory contains a malformed run")
        try:
            run_id = int(raw_run["run_id"])
        except (KeyError, TypeError, ValueError) as exc:
            raise ValueError("inventory run has no numeric run id") from exc
        raw_artifacts = raw_run.get("artifacts")
        if not isinstance(raw_artifacts, list):
            raise ValueError(f"run {run_id} has no artifact list")
        for raw_artifact in raw_artifacts:
            if not isinstance(raw_artifact, dict):
                raise ValueError(f"run {run_id} contains a malformed artifact")
            artifact = _normalise_artifact(raw_artifact, run_id)
            artifact_id = int(artifact["artifact_id"])
            if artifact_id in seen_artifact_ids:
                raise ValueError(f"duplicate source artifact id: {artifact_id}")
            seen_artifact_ids.add(artifact_id)
            artifacts.append(artifact)

    if not artifacts:
        raise ValueError("inventory contains no source artifacts")
    reported_count = inventory.get("artifact_count")
    if reported_count is not None and int(reported_count) != len(artifacts):
        raise ValueError(
            f"inventory artifact count {reported_count!r} does not match its run entries ({len(artifacts)})"
        )
    lanes = [
        {
            "lane": lane,
            "download_wave": lane // download_wave_size,
            "bytes": 0,
            "artifacts": [],
        }
        for lane in range(lane_count)
    ]
    for artifact in sorted(artifacts, key=lambda row: (-int(row["size_in_bytes"]), int(row["run_id"]), int(row["artifact_id"]))):
        lane = min(lanes, key=lambda row: (int(row["bytes"]), int(row["lane"])))
        lane["artifacts"].append(artifact)
        lane["bytes"] += int(artifact["size_in_bytes"])
    return {
        "lane_count": lane_count,
        "artifact_count": len(artifacts),
        "total_bytes": sum(int(artifact["size_in_bytes"]) for artifact in artifacts),
        "lanes": lanes,
    }


def main(argv: Iterable[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Plan byte-balanced immutable MFL recovery artifact lanes")
    parser.add_argument("--inventory", type=Path, required=True)
    parser.add_argument("--lanes", type=int, default=15)
    parser.add_argument("--download-wave-size", type=int, default=5)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(list(argv) if argv is not None else None)
    inventory = json.loads(args.inventory.read_text(encoding="utf-8"))
    plan = build_lane_plan(
        inventory,
        lane_count=args.lanes,
        download_wave_size=args.download_wave_size,
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(plan, indent=2, sort_keys=True), encoding="utf-8")
    print(json.dumps({key: plan[key] for key in ("lane_count", "artifact_count", "total_bytes")}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
