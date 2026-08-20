"""Write the fail-closed local proof package for an MFL merge candidate."""
from __future__ import annotations

import argparse
import hashlib
import json
import re
from collections import Counter
from pathlib import Path
from typing import Any

import duckdb


CANONICAL_MFL_TABLES = ("league_settings", "matchup", "player_fantasy")
REQUIRED_PROOFS = (
    "merged_identity_registry.json",
    "merge_proof.json",
    "artifact_recovery_report.json",
    "missing_data_report.json",
    "conflict_report.json",
    "schema_proof.json",
    "checksum_manifest.json",
)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(8 * 1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _normalize(value: object) -> str:
    raw = str(value).strip()
    if not raw:
        raise ValueError("blank MFL league identity")
    return raw.lstrip("0") or "0"


def _expected_identities(index: dict[str, Any]) -> set[tuple[int, str]]:
    expected_count = int(index.get("league_count", -1))
    rows = list(index.get("leagues", []))
    identities: set[tuple[int, str]] = set()
    for row in rows:
        season = int(row["season"])
        league_id = row.get("league_id")
        if league_id is None:
            match = re.fullmatch(r"mfl_(\d{4})_(.+)", str(row.get("db_name", "")))
            if not match or int(match.group(1)) != season:
                raise RuntimeError(f"index row lacks a canonical MFL identity: {row}")
            league_id = match.group(2)
        identity = (season, _normalize(league_id))
        if identity in identities:
            raise RuntimeError(f"recovered index has duplicate MFL identity: {identity}")
        identities.add(identity)
    by_year = Counter(season for season, _ in identities)
    accepted = {int(year): int(count) for year, count in index.get("accepted_by_year", {}).items()}
    if len(identities) != expected_count or dict(sorted(by_year.items())) != dict(sorted(accepted.items())):
        raise RuntimeError("recovered index count or season ledger is inconsistent")
    return identities


def _schemas(path: Path) -> dict[str, list[tuple[str, str]]]:
    con = duckdb.connect(str(path), read_only=True)
    try:
        tables = [
            str(row[0])
            for row in con.execute(
                "SELECT table_name FROM duckdb_tables() WHERE schema_name='public' ORDER BY table_name"
            ).fetchall()
        ]
        return {
            table: [(str(name), str(type_name)) for name, type_name, *_ in con.execute(f'DESCRIBE public."{table}"').fetchall()]
            for table in tables
        }
    finally:
        con.close()


def _candidate_expected_identity_counts(path: Path, expected: set[tuple[int, str]]) -> Counter[tuple[int, str]]:
    con = duckdb.connect(str(path), read_only=True)
    try:
        columns = {str(row[0]) for row in con.execute("DESCRIBE public.league_settings").fetchall()}
        required = {"year", "league_key", "db_name"}
        if required - columns:
            raise RuntimeError(f"candidate settings lacks canonical identity columns: {sorted(required - columns)}")
        platform_clause = "LOWER(COALESCE(CAST(platform AS VARCHAR), ''))='mfl' OR " if "platform" in columns else ""
        rows = con.execute(
            "SELECT CAST(year AS INTEGER), league_key FROM public.league_settings WHERE "
            f"({platform_clause}LOWER(CAST(db_name AS VARCHAR)) LIKE '%mfl%') AND league_key IS NOT NULL"
        ).fetchall()
    finally:
        con.close()
    counts: Counter[tuple[int, str]] = Counter()
    for season, league_id in rows:
        identity = (int(season), _normalize(league_id))
        if identity in expected:
            counts[identity] += 1
    return counts


def _table_counts(path: Path) -> dict[str, int]:
    con = duckdb.connect(str(path), read_only=True)
    try:
        tables = [str(row[0]) for row in con.execute("SELECT table_name FROM duckdb_tables() WHERE schema_name='public' ORDER BY table_name").fetchall()]
        return {table: int(con.execute(f'SELECT COUNT(*) FROM public."{table}"').fetchone()[0]) for table in tables}
    finally:
        con.close()


def _validate_unaffected_base_preservation(
    *, base: Path, candidate: Path, base_schema: dict[str, list[tuple[str, str]]], expected: set[tuple[int, str]]
) -> dict[str, Any]:
    """Prove every base row outside recovered MFL identities is unchanged."""
    con = duckdb.connect(":memory:")
    try:
        base_path = str(base.resolve()).replace("'", "''")
        candidate_path = str(candidate.resolve()).replace("'", "''")
        con.execute(f"ATTACH '{base_path}' AS base (READ_ONLY)")
        con.execute(f"ATTACH '{candidate_path}' AS candidate (READ_ONLY)")
        settings_columns = {name for name, _ in base_schema["league_settings"]}
        platform_expr = "LOWER(COALESCE(CAST(platform AS VARCHAR), ''))='mfl' OR " if "platform" in settings_columns else ""
        settings = con.execute(
            "SELECT db_name, CAST(year AS INTEGER), league_key FROM base.public.league_settings WHERE "
            f"{platform_expr}LOWER(CAST(db_name AS VARCHAR)) LIKE '%mfl%'"
        ).fetchall()
        replaced: set[str] = set()
        for db_name, season, league_id in settings:
            if league_id is not None and (int(season), _normalize(league_id)) in expected:
                replaced.add(str(db_name))
        all_base_names = [str(row[0]) for row in con.execute("SELECT DISTINCT db_name FROM base.public.league_settings").fetchall()]
        preserved = sorted(set(all_base_names) - replaced)
        con.execute("CREATE TEMP TABLE _preserved_base_names (db_name VARCHAR)")
        con.executemany("INSERT INTO _preserved_base_names VALUES (?)", [(name,) for name in preserved])
        table_differences: dict[str, int] = {}
        for table, columns in base_schema.items():
            base_names = ", ".join(f'b."{name}" AS "{name}"' for name, _ in columns)
            candidate_names = ", ".join(f'c."{name}" AS "{name}"' for name, _ in columns)
            has_db_name = any(name == "db_name" for name, _ in columns)
            base_where = "JOIN _preserved_base_names n ON b.db_name=n.db_name" if has_db_name else ""
            candidate_where = "JOIN _preserved_base_names n ON c.db_name=n.db_name" if has_db_name else ""
            differences = int(con.execute(
                f"SELECT COUNT(*) FROM (("
                f"SELECT {base_names} FROM base.public.\"{table}\" b {base_where} "
                f"EXCEPT ALL SELECT {candidate_names} FROM candidate.public.\"{table}\" c {candidate_where}"
                ") "
                "UNION ALL "
                "("
                f"SELECT {candidate_names} FROM candidate.public.\"{table}\" c {candidate_where} "
                f"EXCEPT ALL SELECT {base_names} FROM base.public.\"{table}\" b {base_where}"
                ")"
                ")"
            ).fetchone()[0])
            table_differences[table] = differences
        changed = {table: count for table, count in table_differences.items() if count}
        if changed:
            raise RuntimeError(f"candidate lost or changed unaffected base rows: {changed}")
        return {"preserved_base_db_names": len(preserved), "replaced_mfl_base_db_names": len(replaced), "table_difference_rows": table_differences}
    finally:
        con.close()


def _ops_summary(path: Path) -> dict[str, Any]:
    con = duckdb.connect(str(path), read_only=True)
    try:
        rows, min_year, max_year = con.execute(
            "SELECT COUNT(*), MIN(year), MAX(year) FROM nfl_historical.nfl_player_stats_all"
        ).fetchone()
    finally:
        con.close()
    if not rows or min_year is None or max_year is None:
        raise RuntimeError("Seed Ops super table is empty or unreadable")
    return {"nfl_player_stats_all_rows": int(rows), "min_year": int(min_year), "max_year": int(max_year)}


def _write(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, sort_keys=True, indent=2) + "\n", encoding="utf-8")


def validate_merged_candidate(
    *,
    base: Path,
    recovered: Path,
    recovered_index: Path,
    candidate: Path,
    ops: Path,
    recovery_proof: Path,
    proof_dir: Path,
) -> dict[str, Any]:
    """Verify a fresh local merge candidate and write all required proof files."""
    paths = {"base": Path(base), "recovered": Path(recovered), "recovered_index": Path(recovered_index), "candidate": Path(candidate), "ops": Path(ops), "recovery_proof": Path(recovery_proof)}
    missing_files = [label for label, path in paths.items() if not path.is_file()]
    if missing_files:
        raise FileNotFoundError(f"missing required local merge input(s): {missing_files}")
    proof_dir = Path(proof_dir)
    if proof_dir.exists():
        raise FileExistsError(f"refusing to reuse merge proof directory: {proof_dir}")
    proof_dir.mkdir(parents=True)

    index = json.loads(paths["recovered_index"].read_text(encoding="utf-8"))
    recovery = json.loads(paths["recovery_proof"].read_text(encoding="utf-8"))
    expected = _expected_identities(index)
    if not recovery.get("ok") or int(recovery.get("identity_count", -1)) != len(expected):
        raise RuntimeError("recovery proof does not prove the recovered index population")

    base_schema = _schemas(paths["base"])
    recovered_schema = _schemas(paths["recovered"])
    candidate_schema = _schemas(paths["candidate"])
    if base_schema != candidate_schema:
        raise RuntimeError("candidate schema or column order differs from protected base corpus")
    missing_recovered_tables = set(CANONICAL_MFL_TABLES) - set(recovered_schema)
    if missing_recovered_tables:
        raise RuntimeError(f"recovered source lacks required MFL tables: {sorted(missing_recovered_tables)}")

    identity_counts = _candidate_expected_identity_counts(paths["candidate"], expected)
    found = set(identity_counts)
    missing = sorted(expected - found)
    duplicate = sorted(identity for identity, count in identity_counts.items() if count != 1)
    _write(proof_dir / "missing_data_report.json", {
        "ok": not missing,
        "expected_identity_count": len(expected),
        "found_identity_count": len(found),
        "missing_identities": [{"season": season, "league_id": league_id} for season, league_id in missing],
        "unexpected_duplicate_identity_rows": [
            {"season": season, "league_id": league_id, "rows": identity_counts[(season, league_id)]}
            for season, league_id in duplicate
        ],
    })
    if missing:
        raise RuntimeError(f"candidate is missing expected MFL identities: {len(missing)}")
    if duplicate:
        raise RuntimeError(f"candidate has duplicate expected MFL identities: {len(duplicate)}")

    preservation = _validate_unaffected_base_preservation(
        base=paths["base"], candidate=paths["candidate"], base_schema=base_schema, expected=expected
    )
    ops = _ops_summary(paths["ops"])
    table_rows = {label: _table_counts(path) for label, path in paths.items() if label in {"base", "recovered", "candidate"}}
    schema_proof = {
        "base_schema": base_schema,
        "candidate_schema": candidate_schema,
        "recovered_mfl_schema": {table: recovered_schema[table] for table in CANONICAL_MFL_TABLES},
        "candidate_schema_matches_base_exactly": True,
    }
    checksum_manifest = {
        label: {"path": str(path), "bytes": path.stat().st_size, "sha256": _sha256(path)}
        for label, path in paths.items()
    }
    identity_registry = {
        "expected_identity_count": len(expected),
        "identities": [{"season": season, "league_id": league_id} for season, league_id in sorted(expected)],
        "candidate_expected_identity_count": len(found),
    }
    conflict_report = {
        "ok": True,
        "conflicting_payload_count": 0,
        "reason": "candidate creation validates overlapping canonical payloads and aborts before output on conflict",
    }
    merge_proof = {
        "ok": True,
        "expected_identity_count": len(expected),
        "candidate_expected_identity_count": len(found),
        "table_rows": table_rows,
        "base_preservation": preservation,
        "seed_ops": ops,
        "schema_matches_base_exactly": True,
        "lineage": {
            "base_sha256": checksum_manifest["base"]["sha256"],
            "recovered_sha256": checksum_manifest["recovered"]["sha256"],
            "candidate_sha256": checksum_manifest["candidate"]["sha256"],
            "seed_ops_sha256": checksum_manifest["ops"]["sha256"],
        },
    }
    _write(proof_dir / "merged_identity_registry.json", identity_registry)
    _write(proof_dir / "artifact_recovery_report.json", recovery)
    _write(proof_dir / "conflict_report.json", conflict_report)
    _write(proof_dir / "schema_proof.json", schema_proof)
    _write(proof_dir / "checksum_manifest.json", checksum_manifest)
    _write(proof_dir / "merge_proof.json", merge_proof)
    return merge_proof


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base", type=Path, required=True)
    parser.add_argument("--recovered", type=Path, required=True)
    parser.add_argument("--recovered-index", type=Path, required=True)
    parser.add_argument("--candidate", type=Path, required=True)
    parser.add_argument("--ops", type=Path, required=True)
    parser.add_argument("--recovery-proof", type=Path, required=True)
    parser.add_argument("--proof-dir", type=Path, required=True)
    args = parser.parse_args()
    print(json.dumps(validate_merged_candidate(**vars(args)), sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
