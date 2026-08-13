"""Fail closed when the MFL batch admission manifest is incomplete or mislabeled."""
from __future__ import annotations

import argparse
import csv
from collections import Counter
from pathlib import Path


STATES = {
    "included_in_verified_cache_batch",
    "admitted_no_promotable_delta",
    "source_diagnostic_only",
    "explicitly_excluded_no_candidate",
}


def validate(
    manifest: Path,
    *,
    expected_total: int | None = None,
    expected_included: int | None = None,
    expected_zero_delta: int | None = None,
    expected_diagnostic: int | None = None,
    expected_excluded: int | None = None,
) -> dict[str, int]:
    with manifest.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    required = {"artifact", "artifact_id", "admission_state", "candidate_rows"}
    missing = required - set(rows[0] if rows else {})
    if missing:
        raise ValueError(f"manifest missing required columns: {sorted(missing)}")
    keys = [(row["artifact"], row["artifact_id"]) for row in rows]
    if len(keys) != len(set(keys)):
        raise ValueError("manifest contains duplicate artifact receipts")
    counts: Counter[str] = Counter()
    for row in rows:
        state = row["admission_state"]
        if state not in STATES:
            raise ValueError(f"invalid admission_state: {state!r}")
        try:
            candidate_rows = int(row["candidate_rows"])
        except ValueError as exc:
            raise ValueError(f"invalid candidate_rows: {row['candidate_rows']!r}") from exc
        if candidate_rows < 0:
            raise ValueError("candidate_rows must be non-negative")
        if state == "included_in_verified_cache_batch" and candidate_rows <= 0:
            raise ValueError(
                "included_in_verified_cache_batch must have candidate_rows > 0"
            )
        if state != "included_in_verified_cache_batch" and candidate_rows != 0:
            raise ValueError(f"{state} must have candidate_rows = 0")
        counts[state] += 1
    expected = {
        "total": expected_total,
        "included_in_verified_cache_batch": expected_included,
        "admitted_no_promotable_delta": expected_zero_delta,
        "source_diagnostic_only": expected_diagnostic,
        "explicitly_excluded_no_candidate": expected_excluded,
    }
    actual = {"total": len(rows), **counts}
    for name, value in expected.items():
        if value is not None and actual.get(name, 0) != value:
            raise ValueError(f"expected {name}={value}, got {actual.get(name, 0)}")
    return {name: actual.get(name, 0) for name in sorted({"total", *STATES})}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--expected-total", type=int)
    parser.add_argument("--expected-included", type=int)
    parser.add_argument("--expected-zero-delta", type=int)
    parser.add_argument("--expected-diagnostic", type=int)
    parser.add_argument("--expected-excluded", type=int)
    args = parser.parse_args()
    report = validate(
        args.manifest,
        expected_total=args.expected_total,
        expected_included=args.expected_included,
        expected_zero_delta=args.expected_zero_delta,
        expected_diagnostic=args.expected_diagnostic,
        expected_excluded=args.expected_excluded,
    )
    print(report)


if __name__ == "__main__":
    main()
