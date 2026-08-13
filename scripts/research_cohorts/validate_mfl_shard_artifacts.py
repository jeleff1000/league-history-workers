"""Fail closed unless selected MFL rescue artifacts exactly match selected shards."""
from __future__ import annotations

import argparse
import json
import re
from pathlib import Path


JOB_RE = re.compile(r"^rescue \((\d+), \d+\)$")
ARTIFACT_RE = re.compile(r"^research-mfl-classification-roster-bridge-shard-(\d+)-\d+$")
TERMINAL = {"success", "failure"}


def _excluded(raw: str) -> set[int]:
    values: set[int] = set()
    for value in (item.strip() for item in raw.split(",")):
        if not value:
            continue
        try:
            index = int(value)
        except ValueError as exc:
            raise ValueError(f"invalid excluded shard: {value!r}") from exc
        if index < 0:
            raise ValueError(f"invalid excluded shard: {value!r}")
        values.add(index)
    return values


def _jobs(path: Path) -> dict[int, str]:
    rows: dict[int, str] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        try:
            name, conclusion = line.split("\t", 1)
        except ValueError as exc:
            raise ValueError(f"invalid rescue job row: {line!r}") from exc
        match = JOB_RE.fullmatch(name)
        if not match:
            raise ValueError(f"invalid rescue job name: {name!r}")
        index = int(match.group(1))
        if index in rows:
            raise ValueError(f"duplicate rescue job index: {index}")
        rows[index] = conclusion.strip()
    if not rows:
        raise ValueError("no rescue jobs")
    return rows


def _artifacts(path: Path) -> set[int]:
    rows: set[int] = set()
    for line in path.read_text(encoding="utf-8").splitlines():
        name = line.strip()
        if not name:
            continue
        match = ARTIFACT_RE.fullmatch(name)
        if not match:
            raise ValueError(f"invalid MFL rescue artifact name: {name!r}")
        index = int(match.group(1))
        if index in rows:
            raise ValueError(f"duplicate MFL rescue artifact index: {index}")
        rows.add(index)
    return rows


def validate(*, jobs_path: Path, artifacts_path: Path, excluded_shards: str) -> dict[str, object]:
    jobs = _jobs(jobs_path)
    excluded = _excluded(excluded_shards)
    unknown_exclusions = sorted(excluded - set(jobs))
    if unknown_exclusions:
        raise ValueError(f"excluded shards are not rescue jobs: {unknown_exclusions}")
    selected = set(jobs) - excluded
    nonterminal = sorted(index for index in selected if jobs[index] not in TERMINAL)
    if nonterminal:
        raise ValueError(f"nonterminal selected rescue shards: {nonterminal}")
    artifact_indices = _artifacts(artifacts_path)
    unexpected = sorted(artifact_indices - selected)
    if unexpected:
        raise ValueError(f"artifact indices include excluded or unknown shards: {unexpected}")
    if artifact_indices != selected:
        raise ValueError(
            "artifact indices do not equal selected rescue indices: "
            f"missing={sorted(selected-artifact_indices)}, extra={sorted(artifact_indices-selected)}"
        )
    return {
        "rescue_jobs": len(jobs),
        "excluded_shards": sorted(excluded),
        "selected_shards": len(selected),
        "artifact_shards": len(artifact_indices),
        "terminal_selected_shards": len(selected),
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--jobs", type=Path, required=True)
    parser.add_argument("--artifacts", type=Path, required=True)
    parser.add_argument("--excluded-shards", default="")
    parser.add_argument("--report", type=Path, required=True)
    args = parser.parse_args()
    report = validate(
        jobs_path=args.jobs,
        artifacts_path=args.artifacts,
        excluded_shards=args.excluded_shards,
    )
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
