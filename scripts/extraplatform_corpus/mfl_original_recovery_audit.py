"""Fail-closed helpers for the pre-2026-08-17 MFL recovery audit.

This module deliberately contains no cache, artifact, or database mutation.
It turns raw MFL identifiers into one canonical ``(season, league_id)`` key and
classifies evidence collected by Actions.  A key is recovered only when Actions
has seen at least one full-schema payload and no payload conflict for that key.
"""

from __future__ import annotations

import argparse
from collections.abc import Mapping, Set
from datetime import datetime, timezone
import json
from pathlib import Path
import re
import subprocess
from typing import Any


MFL_PREFIX = "mfl_"
_DB_NAME_PATTERN = re.compile(rb'mfl_([0-9]{4})_([^"\r\n]+)')
_DB_NAME_RG_PATTERN = r'"db_name"\s*:\s*"mfl_[0-9]{4}_[^"]+"'


def normalize_mfl_identity(season: int | str, raw_league_id: object) -> tuple[int, str]:
    """Return the canonical MFL ``(season, league_id)`` identity.

    Accepted source forms are a plain MFL league id (``26487``) and the
    pipeline database name (``mfl_2006_26487``).  Any mismatched database-name
    season is rejected rather than silently assigning the row to another year.
    """

    normalized_season = int(season)
    league_id = str(raw_league_id).strip()
    if not league_id:
        raise ValueError("MFL league id must not be blank")

    prefix = f"{MFL_PREFIX}{normalized_season}_"
    if league_id.lower().startswith(prefix):
        league_id = league_id[len(prefix) :]
    elif league_id.lower().startswith(MFL_PREFIX):
        raise ValueError(
            f"MFL database name season does not match supplied season: {raw_league_id!r}"
        )

    if not league_id:
        raise ValueError("MFL league id must not be blank after normalization")
    return normalized_season, league_id


def classify_expected_identities(
    expected: Set[tuple[int, str]],
    observed: Mapping[tuple[int, str], Mapping[str, Any]],
) -> dict[str, list[tuple[int, str]]]:
    """Classify expected identities using only full-schema, conflict-free proof.

    ``observed`` is an aggregate of artifact inspection evidence.  A
    ``conflicting_payloads`` value greater than zero always wins over a
    full-schema count; those keys require human/source resolution and may not
    be treated as recovered.
    """

    recovered: list[tuple[int, str]] = []
    missing_full_payload: list[tuple[int, str]] = []
    conflicting_payloads: list[tuple[int, str]] = []

    for identity in sorted(expected):
        evidence = observed.get(identity, {})
        full_payloads = int(evidence.get("full_schema_payloads", 0) or 0)
        conflicts = int(evidence.get("conflicting_payloads", 0) or 0)
        if conflicts > 0:
            conflicting_payloads.append(identity)
        elif full_payloads > 0:
            recovered.append(identity)
        else:
            missing_full_payload.append(identity)

    return {
        "recovered": recovered,
        "missing_full_payload": missing_full_payload,
        "conflicting_payloads": conflicting_payloads,
        "unexpected_observed": sorted(set(observed) - set(expected)),
    }


def select_newest_payloads(
    payloads_by_identity: Mapping[tuple[int, str], list[Mapping[str, Any]]],
) -> dict[tuple[int, str], Mapping[str, Any]]:
    """Select the user-approved newest artifact for each identity.

    ``created_at`` must be an ISO-8601 UTC timestamp.  Artifact id provides a
    stable deterministic tie-breaker when Actions records equal timestamps.
    Every candidate must carry both fields; absence is an audit error rather
    than an implicit fallback to iteration order.
    """

    selected: dict[tuple[int, str], Mapping[str, Any]] = {}
    for identity, candidates in payloads_by_identity.items():
        if not candidates:
            raise ValueError(f"no payload candidates for {identity}")
        for candidate in candidates:
            if not candidate.get("created_at") or candidate.get("artifact_id") is None:
                raise ValueError(f"payload candidate lacks precedence provenance for {identity}: {candidate}")
        selected[identity] = max(
            candidates,
            key=lambda candidate: (str(candidate["created_at"]), int(candidate["artifact_id"])),
        )
    return selected


def build_newest_source_manifest(
    *,
    expected: Set[tuple[int, str]],
    campaign_candidates: Mapping[tuple[int, str], list[Mapping[str, Any]]],
    protected_fallbacks: Set[tuple[int, str]],
    protected_source_run_id: int,
    protected_fallback_provenance: Mapping[tuple[int, str], Mapping[str, Any]] | None = None,
) -> dict[str, Any]:
    """Build a complete, provenance-preserving source manifest.

    Campaign duplicates use the user-approved newest-artifact precedence.  A
    protected-source fallback is valid only for an expected identity with no
    campaign candidate; it can never silently replace an available campaign
    payload.  When provenance is supplied, it must cover every protected
    fallback with the exact immutable Actions artifact that contains it.
    """

    unexpected = set(campaign_candidates) - set(expected)
    if unexpected:
        raise ValueError(f"campaign candidates outside immutable expected registry: {sorted(unexpected)[:5]}")
    selected = select_newest_payloads(campaign_candidates)
    campaign_identities = set(selected)
    missing_before_fallback = set(expected) - campaign_identities
    invalid_fallbacks = set(protected_fallbacks) - missing_before_fallback
    if invalid_fallbacks:
        raise ValueError(f"protected fallbacks must be missing campaign identities: {sorted(invalid_fallbacks)[:5]}")
    missing = missing_before_fallback - set(protected_fallbacks)

    if protected_fallback_provenance is not None:
        provenance_keys = set(protected_fallback_provenance)
        if provenance_keys != set(protected_fallbacks):
            raise ValueError(
                "protected fallback provenance must cover exactly the protected fallbacks: "
                f"missing={sorted(set(protected_fallbacks) - provenance_keys)[:5]} "
                f"unexpected={sorted(provenance_keys - set(protected_fallbacks))[:5]}"
            )
        required_provenance = {"artifact_id", "artifact_name", "created_at", "digest"}
        for identity, provenance in protected_fallback_provenance.items():
            absent = sorted(field for field in required_provenance if not provenance.get(field))
            if absent:
                raise ValueError(
                    f"protected fallback provenance is incomplete for {identity}: missing {absent}"
                )

    entries: list[dict[str, Any]] = []
    for identity in sorted(campaign_identities):
        chosen = selected[identity]
        entry = {
            "season": identity[0],
            "league_id": identity[1],
            "source_type": "campaign_artifact",
        }
        entry.update({str(key): value for key, value in chosen.items() if key not in {"season", "league_id"}})
        entries.append(entry)
    for season, league_id in sorted(protected_fallbacks):
        entry = {
            "season": season,
            "league_id": league_id,
            "source_type": "protected_source_chunk",
            "protected_source_run_id": protected_source_run_id,
        }
        if protected_fallback_provenance is not None:
            entry.update({
                str(key): value
                for key, value in protected_fallback_provenance[(season, league_id)].items()
                if key not in {"season", "league_id", "source_type", "protected_source_run_id"}
            })
        entries.append(entry)
    return {
        "policy": "newest_artifact_created_at_then_artifact_id",
        "expected_count": len(expected),
        "campaign_count": len(campaign_identities),
        "protected_fallback_count": len(protected_fallbacks),
        "missing_count": len(missing),
        "missing": sorted(missing),
        "entries": entries,
    }


def extract_mfl_identities_from_index(path: Path) -> set[tuple[int, str]]:
    """Extract MFL identities from a verbose reducer index using bounded memory.

    The retained reducer proof is hundreds of MB.  This deliberately scans its
    bytes with ripgrep's literal-oriented native scanner instead of
    deserializing the complete JSON document or making a Python-level scan.
    GitHub-hosted runners include ``rg``; absence is an explicit audit error.
    """

    scan_path = path.with_name(f".{path.name}.mfl_identity_scan.tmp")
    try:
        with scan_path.open("wb") as output:
            try:
                result = subprocess.run(
                    ["rg", "-o", "--no-filename", "--no-line-number", _DB_NAME_RG_PATTERN, str(path)],
                    check=False,
                    stdout=output,
                    stderr=subprocess.PIPE,
                )
            except FileNotFoundError as exc:
                raise RuntimeError("ripgrep (rg) is required for bounded MFL index extraction") from exc
        if result.returncode not in (0, 1):
            raise RuntimeError(f"rg failed while extracting MFL identities: {result.stderr.decode(errors='replace')}")

        identities: set[tuple[int, str]] = set()
        with scan_path.open("rb") as matches:
            for line in matches:
                match = _DB_NAME_PATTERN.search(line)
                if not match:
                    continue
                season = int(match.group(1))
                league_id = match.group(2).decode("utf-8")
                identities.add(normalize_mfl_identity(season, league_id))
        return identities
    finally:
        scan_path.unlink(missing_ok=True)


def extract_mfl_identities_from_duckdb(path: Path) -> set[tuple[int, str]]:
    """Read canonical MFL identities from one protected source chunk.

    The source chunk itself remains immutable: this opens DuckDB in read-only
    mode and selects only the identity columns from ``league_settings``.
    """

    try:
        import duckdb
    except ImportError as exc:
        raise RuntimeError("duckdb is required to inspect protected MFL source chunks") from exc

    connection = duckdb.connect(str(path), read_only=True)
    try:
        columns = {
            str(name).lower(): str(name)
            for _, name, *_ in connection.execute("PRAGMA table_info('league_settings')").fetchall()
        }
        db_name_column = columns.get("db_name")
        season_column = next(
            (columns[name] for name in ("season", "year", "league_year") if name in columns),
            None,
        )
        if not db_name_column or not season_column:
            raise ValueError(
                f"{path} league_settings lacks required identity columns; found {sorted(columns.values())}"
            )
        rows = connection.execute(
            f'SELECT DISTINCT "{season_column}", "{db_name_column}" '
            f'FROM league_settings WHERE "{db_name_column}" LIKE \'mfl_%\''
        ).fetchall()
        return {normalize_mfl_identity(season, db_name) for season, db_name in rows}
    finally:
        connection.close()


def validate_expected_registry(
    identities: Set[tuple[int, str]],
    *,
    expected_total: int,
    expected_by_year: Mapping[int, int],
) -> dict[int, int]:
    """Fail unless the reconstructed registry matches the immutable ledger."""

    if len(identities) != expected_total:
        raise ValueError(
            f"expected registry has {len(identities)} identities; required {expected_total}"
        )
    actual_by_year: dict[int, int] = {}
    for season, _ in identities:
        actual_by_year[season] = actual_by_year.get(season, 0) + 1
    normalized_expected = {int(year): int(count) for year, count in expected_by_year.items()}
    if actual_by_year != normalized_expected:
        raise ValueError(
            "expected registry per-year counts do not match immutable ledger: "
            f"actual={dict(sorted(actual_by_year.items()))} "
            f"required={dict(sorted(normalized_expected.items()))}"
        )
    return dict(sorted(actual_by_year.items()))


def build_expected_registry(
    *,
    proof_index: Path,
    source_chunks: list[Path],
    expected_total: int,
    expected_by_year: Mapping[int, int],
) -> dict[str, Any]:
    """Build the immutable expected ledger from fixed proof plus source chunks."""

    proof_identities = extract_mfl_identities_from_index(proof_index)
    source_identities: set[tuple[int, str]] = set()
    per_chunk: list[dict[str, Any]] = []
    for chunk in sorted(source_chunks):
        identities = extract_mfl_identities_from_duckdb(chunk)
        source_identities.update(identities)
        per_chunk.append({"path": str(chunk), "identity_count": len(identities)})
    identities = proof_identities | source_identities
    by_year = validate_expected_registry(
        identities, expected_total=expected_total, expected_by_year=expected_by_year
    )
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "proof_index": str(proof_index),
        "source_chunks": per_chunk,
        "proof_identity_count": len(proof_identities),
        "source_identity_count": len(source_identities),
        "overlap_identity_count": len(proof_identities & source_identities),
        "identity_count": len(identities),
        "by_year": by_year,
        "identities": sorted(identities),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Build the immutable original MFL identity ledger")
    parser.add_argument("--proof-index", type=Path, required=True)
    parser.add_argument("--source-chunk", type=Path, action="append", required=True)
    parser.add_argument("--expected-total", type=int, required=True)
    parser.add_argument("--expected-by-year", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    expected_by_year = json.loads(args.expected_by_year.read_text(encoding="utf-8"))
    report = build_expected_registry(
        proof_index=args.proof_index,
        source_chunks=args.source_chunk,
        expected_total=args.expected_total,
        expected_by_year=expected_by_year,
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2, sort_keys=True), encoding="utf-8")
    print(json.dumps({"identity_count": report["identity_count"], "by_year": report["by_year"]}, sort_keys=True))


if __name__ == "__main__":
    main()
