"""Fail-closed finalization of validated MFL reconstruction lane candidates."""
from __future__ import annotations

import argparse
import hashlib
import json
from collections import Counter
from pathlib import Path
from typing import Any

import duckdb

from scripts.extraplatform_corpus.reconstruct_mfl_recovery_candidate import (
    CANONICAL_MFL_TABLES,
    _qi,
    _sha256_file,
    canonical_schema,
    validate_candidate_identities,
)


def _lane_entries(lane: dict[str, Any]) -> list[dict[str, Any]]:
    return [entry for item in lane.get("items", []) for entry in item.get("entries", [])]


def _expected_entries(plan: dict[str, Any]) -> list[dict[str, Any]]:
    entries = [entry for lane in plan.get("lanes", []) for entry in _lane_entries(lane)]
    identities = {(int(entry["season"]), str(entry["league_id"])) for entry in entries}
    if len(entries) != len(identities) or len(entries) != int(plan.get("expected_identity_count", -1)):
        raise ValueError("immutable lane plan has duplicate or missing identities")
    return entries


def _schema_hash(schema: dict[str, list[tuple[str, str]]]) -> str:
    return hashlib.sha256(json.dumps(schema, sort_keys=True).encode("utf-8")).hexdigest()


def finalize_lanes(
    run_root: Path,
    lane_plan_path: Path,
    candidate_path: Path,
    index_path: Path,
    proof_path: Path,
) -> dict[str, Any]:
    """Build a new staging DB only from all proven lane candidates.

    Inputs and existing output paths are immutable.  The final proof is only
    created after lane hashes, lane identities, output identities, and output
    table population all agree with the lane plan.
    """
    run_root = Path(run_root)
    lane_plan_path = Path(lane_plan_path)
    candidate_path = Path(candidate_path)
    index_path = Path(index_path)
    proof_path = Path(proof_path)
    for path, label in ((candidate_path, "candidate"), (index_path, "index"), (proof_path, "proof")):
        if path.exists():
            raise FileExistsError(f"refusing to overwrite existing finalization {label}: {path}")

    plan = json.loads(lane_plan_path.read_text(encoding="utf-8"))
    lanes = list(plan.get("lanes", []))
    entries = _expected_entries(plan)
    success_path = run_root / "ALL_LANES_OK.json"
    if not success_path.is_file():
        raise FileNotFoundError(f"all-lanes success proof is required: {success_path}")
    success = json.loads(success_path.read_text(encoding="utf-8"))
    if not success.get("ok") or int(success.get("lane_count", -1)) != len(lanes):
        raise ValueError("all-lanes success proof does not cover the complete lane plan")
    if int(success.get("identity_count", -1)) != len(entries):
        raise ValueError("all-lanes success proof has the wrong identity count")
    ledger_candidates = {
        int(item["lane"]): item for item in success.get("lane_candidates", [])
    }
    if len(ledger_candidates) != len(lanes):
        raise ValueError("all-lanes success proof has duplicate or missing candidate records")

    lane_paths: list[Path] = []
    expected_schema: dict[str, list[tuple[str, str]]] | None = None
    for ordinal, lane in enumerate(lanes):
        lane_proof_path = run_root / "proof" / f"lane_{ordinal:02d}.json"
        if not lane_proof_path.is_file():
            raise FileNotFoundError(f"missing lane proof: {lane_proof_path}")
        lane_proof = json.loads(lane_proof_path.read_text(encoding="utf-8"))
        ledger = ledger_candidates.get(int(lane.get("lane", ordinal)))
        if ledger is None or not lane_proof.get("ok"):
            raise ValueError(f"lane {ordinal} lacks a matching successful proof")
        path = Path(str(lane_proof.get("candidate_path", "")))
        digest = _sha256_file(path) if path.is_file() else ""
        if (
            str(ledger.get("path")) != str(path)
            or str(ledger.get("sha256")) != digest
            or str(lane_proof.get("candidate_sha256")) != digest
        ):
            raise ValueError(f"lane {ordinal} candidate checksum proof mismatch")
        lane_entries = _lane_entries(lane)
        validate_candidate_identities(path, lane_entries)
        schema = canonical_schema(path)
        if expected_schema is None:
            expected_schema = schema
        elif schema != expected_schema:
            raise ValueError(f"lane {ordinal} canonical schema mismatch")
        lane_paths.append(path)
    if expected_schema is None:
        raise ValueError("no lane schema available")

    candidate_path.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(candidate_path))
    try:
        first = str(lane_paths[0].resolve()).replace("'", "''")
        con.execute(f"ATTACH '{first}' AS source_0 (READ_ONLY)")
        con.execute("CREATE SCHEMA public")
        for table in CANONICAL_MFL_TABLES:
            con.execute(
                f"CREATE TABLE public.{_qi(table)} AS "
                f"SELECT * FROM source_0.public.{_qi(table)} WHERE FALSE"
            )
        con.execute("DETACH source_0")
        for ordinal, path in enumerate(lane_paths):
            source = str(path.resolve()).replace("'", "''")
            alias = f"source_{ordinal}"
            con.execute(f"ATTACH '{source}' AS {alias} (READ_ONLY)")
            con.execute("BEGIN TRANSACTION")
            for table in CANONICAL_MFL_TABLES:
                con.execute(
                    f"INSERT INTO public.{_qi(table)} SELECT * FROM {alias}.public.{_qi(table)}"
                )
            con.execute("COMMIT")
            con.execute(f"DETACH {alias}")
    except Exception:
        try:
            con.execute("ROLLBACK")
        except duckdb.Error:
            pass
        raise
    finally:
        con.close()

    table_rows = validate_candidate_identities(candidate_path, entries)
    candidate_hash = _sha256_file(candidate_path)
    years = Counter(int(entry["season"]) for entry in entries)
    index = {
        "league_count": len(entries),
        "accepted_by_year": dict(sorted(years.items())),
        "leagues": [
            {"db_name": str(entry.get("db_name") or f"mfl_{int(entry['season'])}_{entry['league_id']}"), "season": int(entry["season"])}
            for entry in sorted(entries, key=lambda entry: (int(entry["season"]), str(entry["league_id"])))
        ],
    }
    report = {
        "ok": True,
        "run_root": str(run_root),
        "lane_plan_path": str(lane_plan_path),
        "identity_count": len(entries),
        "accepted_by_year": dict(sorted(years.items())),
        "table_rows": table_rows,
        "candidate_path": str(candidate_path),
        "candidate_sha256": candidate_hash,
        "schema_sha256": _schema_hash(expected_schema),
        "lane_candidates": [
            {"path": str(path), "sha256": _sha256_file(path)} for path in lane_paths
        ],
    }
    index_path.parent.mkdir(parents=True, exist_ok=True)
    proof_path.parent.mkdir(parents=True, exist_ok=True)
    with index_path.open("x", encoding="utf-8") as handle:
        json.dump(index, handle, sort_keys=True, indent=2)
    with proof_path.open("x", encoding="utf-8") as handle:
        json.dump(report, handle, sort_keys=True, indent=2)
    return report


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-root", type=Path, required=True)
    parser.add_argument("--lane-plan", type=Path, required=True)
    parser.add_argument("--candidate", type=Path, required=True)
    parser.add_argument("--index", type=Path, required=True)
    parser.add_argument("--proof", type=Path, required=True)
    args = parser.parse_args()
    report = finalize_lanes(args.run_root, args.lane_plan, args.candidate, args.index, args.proof)
    print(json.dumps(report, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
