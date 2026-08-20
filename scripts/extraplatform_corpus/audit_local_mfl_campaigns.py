"""Bounded local inventory of retained MFL campaign artifacts.

The original Actions artifacts have expired, so this audits the campaign
folders already retained on D:.  It intentionally does *not* claim recovery:
an index can advertise identities without proving its DuckDB payload has the
required schema.  The report gives the next validator an exact, compact source
map and names every missing chunk, scan failure, duplicate, and legacy receipt.
"""

from __future__ import annotations

import argparse
from collections import defaultdict
import json
from pathlib import Path
import re
import subprocess
from typing import Callable, Iterable


EXPECTED_BY_YEAR = {
    2004: 4780,
    2005: 935,
    2006: 7719,
    2007: 2136,
    2008: 2195,
    2009: 2133,
    2010: 2155,
    2011: 2170,
    2012: 1801,
    2013: 1288,
    2014: 865,
    2015: 836,
    2016: 9114,
    2017: 468,
    2018: 773,
}
IDENTITY_PATTERN = re.compile(r"^mfl_(20(?:0[4-9]|1[0-8]))_([A-Za-z0-9_-]+)$")
RG_IDENTITY_PATTERN = r"mfl_(?:200[4-9]|201[0-8])_[A-Za-z0-9_-]+"


class CampaignAuditError(RuntimeError):
    """A required audit input is invalid or incomplete."""


def normalize_identity(raw: str) -> tuple[int, str]:
    match = IDENTITY_PATTERN.fullmatch(raw.strip())
    if not match:
        raise CampaignAuditError(f"invalid MFL database identity: {raw!r}")
    return int(match.group(1)), match.group(2)


def scan_index_identities(index_path: Path) -> set[tuple[int, str]]:
    """Extract index-advertised identities with rg, without loading JSON in RAM."""

    index_path = Path(index_path)
    if not index_path.is_file():
        raise FileNotFoundError(index_path)
    try:
        process = subprocess.Popen(
            ["rg", "-o", "--no-filename", "--no-line-number", RG_IDENTITY_PATTERN, str(index_path)],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            encoding="utf-8",
            errors="replace",
        )
    except FileNotFoundError as exc:
        raise CampaignAuditError("ripgrep (rg) is required for bounded campaign index scans") from exc
    assert process.stdout is not None
    identities = {normalize_identity(line) for line in process.stdout if line.strip()}
    stderr = process.stderr.read() if process.stderr is not None else ""
    exit_code = process.wait()
    if exit_code not in (0, 1):
        raise CampaignAuditError(f"rg failed for {index_path}: {stderr.strip()}")
    return identities


def _sort_identities(identities: Iterable[tuple[int, str]]) -> list[list[object]]:
    return [[season, league_id] for season, league_id in sorted(identities)]


def _receipt_state(campaign_dir: Path) -> str:
    receipt = campaign_dir / ".landed.json"
    if not receipt.is_file():
        return "missing"
    try:
        payload = json.loads(receipt.read_text(encoding="utf-8-sig"))
    except (OSError, json.JSONDecodeError):
        return "malformed"
    if payload.get("artifact_id") is not None and payload.get("archive_sha256"):
        return "digest_recorded"
    return "legacy"


def audit_campaigns(
    campaigns_root: Path,
    *,
    expected: set[tuple[int, str]],
    scan_identities: Callable[[Path], set[tuple[int, str]]] = scan_index_identities,
) -> dict[str, object]:
    """Inventory all local campaign indices, without opening their DuckDB files."""

    campaigns_root = Path(campaigns_root)
    if not campaigns_root.is_dir():
        raise FileNotFoundError(campaigns_root)
    campaign_dirs = sorted(path for path in campaigns_root.iterdir() if path.is_dir())
    advertised_by_identity: dict[tuple[int, str], list[str]] = defaultdict(list)
    missing_chunk_indexes: list[str] = []
    index_scan_failures: list[dict[str, str]] = []
    receipt_states: dict[str, int] = defaultdict(int)
    campaign_records: list[dict[str, object]] = []

    for campaign_dir in campaign_dirs:
        index_path = campaign_dir / "mfl_register_all_runs.json"
        if not index_path.is_file():
            continue
        receipt_state = _receipt_state(campaign_dir)
        receipt_states[receipt_state] += 1
        chunk_path = campaign_dir / "mfl_register_chunk.duckdb"
        if not chunk_path.is_file():
            missing_chunk_indexes.append(str(campaign_dir))
            campaign_records.append(
                {
                    "campaign": str(campaign_dir),
                    "index": str(index_path),
                    "chunk": None,
                    "receipt_state": receipt_state,
                    "identity_count": None,
                }
            )
            continue
        try:
            identities = scan_identities(index_path)
        except Exception as exc:  # report every bad source; do not hide it behind the first failure
            index_scan_failures.append({"campaign": str(campaign_dir), "error": f"{type(exc).__name__}: {exc}"})
            campaign_records.append(
                {
                    "campaign": str(campaign_dir),
                    "index": str(index_path),
                    "chunk": str(chunk_path),
                    "receipt_state": receipt_state,
                    "identity_count": None,
                }
            )
            continue
        for identity in identities:
            advertised_by_identity[identity].append(str(campaign_dir))
        campaign_records.append(
            {
                "campaign": str(campaign_dir),
                "index": str(index_path),
                "chunk": str(chunk_path),
                "receipt_state": receipt_state,
                "identity_count": len(identities),
            }
        )

    advertised = set(advertised_by_identity)
    duplicates = {
        f"{season}|{league_id}": sorted(paths)
        for (season, league_id), paths in sorted(advertised_by_identity.items())
        if len(paths) > 1
    }
    return {
        "scope": "index_advertised_only; DuckDB schema/payload validation remains required",
        "campaign_root": str(campaigns_root),
        "expected_identity_count": len(expected),
        "campaign_directory_count": len(campaign_dirs),
        "campaign_index_count": len(campaign_records),
        "chunk_backed_index_count": sum(record["chunk"] is not None for record in campaign_records),
        "receipt_states": dict(sorted(receipt_states.items())),
        "index_advertised_identity_count": len(advertised),
        "index_advertised_expected_count": len(advertised & expected),
        "missing_from_chunk_backed_indexes": _sort_identities(expected - advertised),
        "unexpected_chunk_backed_identities": _sort_identities(advertised - expected),
        "duplicate_chunk_backed_identities": duplicates,
        "missing_chunk_indexes": sorted(missing_chunk_indexes),
        "index_scan_failures": sorted(index_scan_failures, key=lambda row: row["campaign"]),
        "campaigns": campaign_records,
        "validated_full_schema_identity_count": 0,
    }


def build_expected_identities(index_path: Path) -> set[tuple[int, str]]:
    identities = scan_index_identities(index_path)
    by_year: dict[int, int] = defaultdict(int)
    for season, _ in identities:
        by_year[season] += 1
    if len(identities) != sum(EXPECTED_BY_YEAR.values()) or dict(sorted(by_year.items())) != EXPECTED_BY_YEAR:
        raise CampaignAuditError(
            f"immutable registry mismatch: identities={len(identities)} by_year={dict(sorted(by_year.items()))}"
        )
    return identities


def main() -> int:
    parser = argparse.ArgumentParser(description="Audit retained local MFL campaign index coverage")
    parser.add_argument("--campaigns-root", type=Path, required=True)
    parser.add_argument("--immutable-index", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.output.exists():
        raise SystemExit(f"output must be new evidence: {args.output}")
    expected = build_expected_identities(args.immutable_index)
    report = audit_campaigns(args.campaigns_root, expected=expected)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2, sort_keys=True), encoding="utf-8")
    print(
        json.dumps(
            {
                "expected_identity_count": report["expected_identity_count"],
                "index_advertised_expected_count": report["index_advertised_expected_count"],
                "missing_count": len(report["missing_from_chunk_backed_indexes"]),
                "scan_failures": len(report["index_scan_failures"]),
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
