"""Plan fail-closed reconstruction of the immutable original MFL artifacts.

This module only turns the frozen 39,368-entry source manifest into immutable,
archive-grouped work.  Assembly is intentionally a later gated operation: no
cache, artifact, or candidate database is mutated by this planner.
"""
from __future__ import annotations

import argparse
import hashlib
import json
from collections import defaultdict
from pathlib import Path, PureWindowsPath
from typing import Any
import zipfile


REQUIRED_CAMPAIGN_FIELDS = {
    "season",
    "league_id",
    "db_name",
    "run_id",
    "archive",
    "archive_sha256",
    "payload_member",
    "payload_sha256",
    "receipt_member",
}


def _identity(entry: dict[str, Any]) -> tuple[int, str]:
    return int(entry["season"]), str(entry["league_id"])


def _remote_archive_path(archive: str, remote_campaign_root: str) -> str:
    path = PureWindowsPath(archive)
    if len(path.parts) < 2:
        raise ValueError(f"manifest archive has no run/file path: {archive!r}")
    return f"{remote_campaign_root.rstrip('/')}/{path.parts[-2]}/{path.parts[-1]}"


def build_campaign_work(entries: list[dict[str, Any]], *, remote_campaign_root: str) -> list[dict[str, Any]]:
    """Group every campaign identity by one immutable remote archive.

    Each canonical identity may appear exactly once.  The caller receives only
    campaign entries; protected-source entries remain a separate explicit path.
    """
    seen: set[tuple[int, str]] = set()
    groups: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for raw_entry in entries:
        if raw_entry.get("source_type") != "campaign_artifact":
            continue
        missing = sorted(REQUIRED_CAMPAIGN_FIELDS - set(raw_entry))
        if missing:
            raise ValueError(f"campaign entry lacks required fields {missing}: {raw_entry}")
        entry = {str(key): value for key, value in raw_entry.items()}
        identity = _identity(entry)
        if identity in seen:
            raise ValueError(f"duplicate canonical identity in campaign manifest: {identity}")
        seen.add(identity)
        archive_sha256 = str(entry["archive_sha256"]).lower()
        if len(archive_sha256) != 64 or any(char not in "0123456789abcdef" for char in archive_sha256):
            raise ValueError(f"campaign archive has invalid SHA-256: {entry['archive']!r}")
        archive_path = _remote_archive_path(str(entry["archive"]), remote_campaign_root)
        groups[(archive_path, archive_sha256)].append(entry)

    result: list[dict[str, Any]] = []
    for (archive_path, archive_sha256), group_entries in sorted(groups.items()):
        result.append(
            {
                "archive_path": archive_path,
                "archive_sha256": archive_sha256,
                "entries": sorted(group_entries, key=lambda item: _identity(item)),
            }
        )
    return result


def read_validated_payload(archive_path: Path, entry: dict[str, Any]) -> tuple[bytes, dict[str, Any]]:
    """Read one manifest-selected payload only after its receipt and hash agree.

    No filesystem write occurs here.  Receipt ``season`` is validated when it
    exists because older artifacts encode the season only in ``db_name``.
    """
    archive_path = Path(archive_path)
    try:
        with zipfile.ZipFile(archive_path) as archive:
            payload = archive.read(str(entry["payload_member"]))
            receipt_raw = archive.read(str(entry["receipt_member"]))
    except (KeyError, OSError, zipfile.BadZipFile) as error:
        raise ValueError(f"cannot read manifest payload from {archive_path}: {error}") from error
    expected_hash = str(entry["payload_sha256"]).lower()
    actual_hash = hashlib.sha256(payload).hexdigest()
    if actual_hash != expected_hash:
        raise ValueError(
            f"payload SHA-256 mismatch for {entry.get('db_name')}: expected={expected_hash} actual={actual_hash}"
        )
    try:
        receipt = json.loads(receipt_raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError(f"receipt is not valid JSON for {entry.get('db_name')}") from error
    if not isinstance(receipt, dict):
        raise ValueError(f"receipt is not an object for {entry.get('db_name')}")
    expected = (str(entry["db_name"]), str(entry["league_id"]), int(entry["season"]))
    actual = (str(receipt.get("db_name", "")), str(receipt.get("league_id", "")), receipt.get("season"))
    if actual[0] != expected[0] or actual[1] != expected[1] or (
        actual[2] is not None and int(actual[2]) != expected[2]
    ):
        raise ValueError(f"receipt identity mismatch: expected={expected} actual={actual}")
    return payload, receipt


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--remote-campaign-root", required=True)
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()
    if args.out.exists():
        raise SystemExit(f"refusing to overwrite recovery work plan: {args.out}")
    manifest = json.loads(args.manifest.read_text(encoding="utf-8"))
    entries = manifest.get("entries")
    expected_count = int(manifest.get("expected_count", 0))
    if not isinstance(entries, list) or len(entries) != expected_count:
        raise SystemExit("manifest entry count does not match its immutable expected count")
    if int(manifest.get("missing_count", -1)) != 0:
        raise SystemExit("manifest reports missing original MFL identities")
    campaign_work = build_campaign_work(entries, remote_campaign_root=args.remote_campaign_root)
    protected = [entry for entry in entries if entry.get("source_type") == "protected_source_chunk"]
    report = {
        "expected_identity_count": expected_count,
        "campaign_identity_count": sum(len(group["entries"]) for group in campaign_work),
        "campaign_archive_count": len(campaign_work),
        "protected_identity_count": len(protected),
        "campaign_work": campaign_work,
        "protected_entries": sorted(protected, key=_identity),
    }
    if report["campaign_identity_count"] + report["protected_identity_count"] != expected_count:
        raise SystemExit("campaign and protected work do not cover the immutable manifest")
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps({key: report[key] for key in report if key.endswith("_count")}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
