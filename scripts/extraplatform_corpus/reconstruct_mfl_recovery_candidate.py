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

import duckdb


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
CANONICAL_MFL_TABLES = ("league_settings", "matchup", "player_fantasy")


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _qi(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def canonical_schema(payload_path: Path) -> dict[str, list[tuple[str, str]]]:
    """Return the exact three-table MFL recovery contract for one payload."""
    con = duckdb.connect(str(payload_path), read_only=True)
    try:
        result: dict[str, list[tuple[str, str]]] = {}
        for table in CANONICAL_MFL_TABLES:
            try:
                result[table] = [
                    (str(row[0]), str(row[1]))
                    for row in con.execute(f"DESCRIBE public.{_qi(table)}").fetchall()
                ]
            except duckdb.CatalogException as error:
                raise ValueError(f"payload lacks canonical table {table}: {payload_path}") from error
        return result
    finally:
        con.close()


def append_canonical_payload(
    candidate_path: Path,
    payload_path: Path,
    *,
    expected_schema: dict[str, list[tuple[str, str]]] | None = None,
) -> dict[str, list[tuple[str, str]]]:
    """Append a fully validated payload to one new or existing lane candidate.

    A source schema mismatch is detected before a new candidate file is
    created.  Existing candidates are never replaced.
    """
    candidate_path = Path(candidate_path)
    payload_path = Path(payload_path)
    source_schema = canonical_schema(payload_path)
    expected = expected_schema or source_schema
    if source_schema != expected:
        raise ValueError("canonical schema mismatch between payload and lane contract")

    candidate_exists = candidate_path.exists()
    candidate_path.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(candidate_path))
    try:
        payload_sql = str(payload_path.resolve()).replace("'", "''")
        con.execute(f"ATTACH '{payload_sql}' AS payload (READ_ONLY)")
        if not candidate_exists:
            con.execute("CREATE SCHEMA public")
            for table in CANONICAL_MFL_TABLES:
                con.execute(
                    f"CREATE TABLE public.{_qi(table)} AS "
                    f"SELECT * FROM payload.public.{_qi(table)} WHERE FALSE"
                )
        for table in CANONICAL_MFL_TABLES:
            candidate_table_schema = [
                (str(row[0]), str(row[1]))
                for row in con.execute(f"DESCRIBE public.{_qi(table)}").fetchall()
            ]
            if candidate_table_schema != expected[table]:
                raise ValueError(f"canonical schema mismatch in candidate table {table}")
            con.execute(
                f"INSERT INTO public.{_qi(table)} SELECT * FROM payload.public.{_qi(table)}"
            )
        con.execute("DETACH payload")
    finally:
        con.close()
    return expected


def validate_candidate_identities(
    candidate_path: Path, expected_entries: list[dict[str, Any]]
) -> dict[str, int]:
    """Fail unless a lane candidate contains exactly its planned MFL leagues.

    The canonical settings schema uses ``year`` and ``league_key`` as the
    physical identity fields.  The immutable work plan calls them ``season``
    and ``league_id``; this function bridges those names explicitly and never
    assumes non-existent columns.
    """
    expected_by_db: dict[str, tuple[int, str]] = {}
    for entry in expected_entries:
        db_name = _entry_db_name(entry)
        identity = _identity(entry)
        if db_name in expected_by_db or identity in expected_by_db.values():
            raise ValueError(f"duplicate expected lane identity: {identity}")
        expected_by_db[db_name] = identity

    con = duckdb.connect(str(candidate_path), read_only=True)
    try:
        rows = con.execute(
            "SELECT db_name, year, league_key, COUNT(*) "
            "FROM public.league_settings "
            "GROUP BY db_name, year, league_key"
        ).fetchall()
        actual_by_db = {str(db_name): (int(year), str(league_key)) for db_name, year, league_key, _ in rows}
        if len(rows) != len(actual_by_db) or any(count != 1 for _, _, _, count in rows):
            raise ValueError("candidate has duplicate league_settings identities")
        if actual_by_db != expected_by_db:
            raise ValueError(
                f"candidate identity mismatch expected={expected_by_db} actual={actual_by_db}"
            )
        counts: dict[str, int] = {}
        for table in CANONICAL_MFL_TABLES:
            counts[table] = int(con.execute(f"SELECT COUNT(*) FROM public.{_qi(table)}").fetchone()[0])
            source_dbs = {
                str(row[0])
                for row in con.execute(
                    f"SELECT DISTINCT db_name FROM public.{_qi(table)}"
                ).fetchall()
            }
            if source_dbs != set(expected_by_db):
                raise ValueError(
                    f"candidate {table} db_name population does not match lane plan"
                )
        return counts
    finally:
        con.close()


def append_protected_source_chunk(
    candidate_path: Path,
    source_path: Path,
    entries: list[dict[str, Any]],
    *,
    expected_schema: dict[str, list[tuple[str, str]]] | None = None,
) -> list[str]:
    """Append only manifest-listed identities from the protected `main` source DB."""
    candidate_path = Path(candidate_path)
    source_path = Path(source_path)
    if not candidate_path.is_file():
        raise FileNotFoundError(f"protected append requires an existing lane candidate: {candidate_path}")
    if not entries:
        raise ValueError("protected append has no manifest identities")

    expected_by_db = {_entry_db_name(entry): _identity(entry) for entry in entries}
    if len(expected_by_db) != len(entries) or len(set(expected_by_db.values())) != len(entries):
        raise ValueError("protected append has duplicate manifest identities")

    source_con = duckdb.connect(str(source_path), read_only=True)
    try:
        source_schema: dict[str, list[tuple[str, str]]] = {}
        for table in CANONICAL_MFL_TABLES:
            source_schema[table] = [
                (str(row[0]), str(row[1]))
                for row in source_con.execute(f"DESCRIBE main.{_qi(table)}").fetchall()
            ]
        expected = expected_schema or source_schema
        if source_schema != expected:
            raise ValueError("protected source canonical schema mismatch")
        rows = source_con.execute(
            "SELECT db_name, year, league_key, COUNT(*) "
            "FROM main.league_settings GROUP BY db_name, year, league_key"
        ).fetchall()
        actual_by_db = {str(db_name): (int(year), str(league_key)) for db_name, year, league_key, _ in rows}
        for db_name, identity in expected_by_db.items():
            if actual_by_db.get(db_name) != identity:
                raise ValueError(
                    f"protected source identity mismatch for {db_name}: "
                    f"expected={identity} actual={actual_by_db.get(db_name)}"
                )
            matching_rows = [row for row in rows if str(row[0]) == db_name and (int(row[1]), str(row[2])) == identity]
            if len(matching_rows) != 1 or int(matching_rows[0][3]) != 1:
                raise ValueError(f"protected source has duplicate identity: {db_name}")
        for table in CANONICAL_MFL_TABLES:
            placeholders = ", ".join("?" for _ in expected_by_db)
            source_dbs = {
                str(row[0])
                for row in source_con.execute(
                    f"SELECT DISTINCT db_name FROM main.{_qi(table)} WHERE db_name IN ({placeholders})",
                    list(expected_by_db),
                ).fetchall()
            }
            if source_dbs != set(expected_by_db):
                raise ValueError(f"protected source {table} is missing a requested db_name")
    finally:
        source_con.close()

    con = duckdb.connect(str(candidate_path))
    try:
        source_sql = str(source_path.resolve()).replace("'", "''")
        con.execute(f"ATTACH '{source_sql}' AS protected (READ_ONLY)")
        con.execute("BEGIN TRANSACTION")
        for table in CANONICAL_MFL_TABLES:
            candidate_table_schema = [
                (str(row[0]), str(row[1]))
                for row in con.execute(f"DESCRIBE public.{_qi(table)}").fetchall()
            ]
            if candidate_table_schema != expected[table]:
                raise ValueError(f"canonical schema mismatch in candidate table {table}")
            placeholders = ", ".join("?" for _ in expected_by_db)
            con.execute(
                f"INSERT INTO public.{_qi(table)} "
                f"SELECT * FROM protected.main.{_qi(table)} WHERE db_name IN ({placeholders})",
                list(expected_by_db),
            )
        con.execute("COMMIT")
        con.execute("DETACH protected")
    except Exception:
        try:
            con.execute("ROLLBACK")
        except duckdb.Error:
            pass
        raise
    finally:
        con.close()
    return sorted(expected_by_db)


def run_lane(
    lane: dict[str, Any], *, candidate_path: Path, work_dir: Path, proof_path: Path
) -> dict[str, Any]:
    """Assemble one fresh lane and write its proof only after validation.

    A partial candidate has no proof and is therefore ineligible for the
    later all-lane merge.  Re-runs must use a new path; no existing evidence
    or candidate is ever overwritten.
    """
    candidate_path = Path(candidate_path)
    work_dir = Path(work_dir)
    proof_path = Path(proof_path)
    for path, label in (
        (candidate_path, "candidate"),
        (work_dir, "work directory"),
        (proof_path, "proof"),
    ):
        if path.exists():
            raise FileExistsError(f"refusing to reuse existing lane {label}: {path}")
    items = list(lane.get("items", []))
    campaign_items = [item for item in items if item.get("kind") == "campaign"]
    protected_items = [item for item in items if item.get("kind") == "protected"]
    if len(campaign_items) == 0 or len(protected_items) > 1 or len(campaign_items) + len(protected_items) != len(items):
        raise ValueError("lane must contain campaign items and at most one protected item")

    candidate_path.parent.mkdir(parents=True, exist_ok=True)
    work_dir.mkdir(parents=True)
    records: list[dict[str, Any]] = []
    expected_entries: list[dict[str, Any]] = []
    for item in campaign_items:
        group = {
            "archive_path": str(item["path"]),
            "archive_sha256": str(item["sha256"]),
            "entries": list(item["entries"]),
        }
        records.extend(assemble_campaign_group(group, candidate_path=candidate_path, work_dir=work_dir))
        expected_entries.extend(group["entries"])

    protected_report: dict[str, Any] | None = None
    if protected_items:
        item = protected_items[0]
        protected_archive = Path(str(item["path"]))
        archive_hash = _sha256_file(protected_archive)
        if archive_hash != str(item["sha256"]).lower():
            raise ValueError(
                f"protected archive SHA-256 mismatch: expected={item['sha256']} actual={archive_hash}"
            )
        protected_dir = work_dir / "protected"
        protected_dir.mkdir()
        chunk_path = protected_dir / "mfl_register_chunk.duckdb"
        digest = hashlib.sha256()
        with zipfile.ZipFile(protected_archive) as archive:
            with archive.open("chunk_state/mfl_register_chunk.duckdb") as source, chunk_path.open("xb") as target:
                for block in iter(lambda: source.read(1024 * 1024), b""):
                    target.write(block)
                    digest.update(block)
        protected_entries = list(item["entries"])
        appended = append_protected_source_chunk(
            candidate_path,
            chunk_path,
            protected_entries,
            expected_schema=canonical_schema(candidate_path),
        )
        expected_entries.extend(protected_entries)
        protected_report = {
            "archive_path": str(protected_archive),
            "archive_sha256": archive_hash,
            "chunk_path": str(chunk_path),
            "chunk_sha256": digest.hexdigest(),
            "appended_db_names": appended,
        }

    counts = validate_candidate_identities(candidate_path, expected_entries)
    report = {
        "ok": True,
        "lane": lane.get("lane"),
        "expected_identity_count": len(expected_entries),
        "campaign_record_count": len(records),
        "protected": protected_report,
        "table_rows": counts,
        "candidate_path": str(candidate_path),
        "candidate_sha256": _sha256_file(candidate_path),
        "work_dir": str(work_dir),
    }
    proof_path.parent.mkdir(parents=True, exist_ok=True)
    with proof_path.open("x", encoding="utf-8") as handle:
        json.dump(report, handle, sort_keys=True, indent=2)
    return report


def assemble_campaign_group(
    group: dict[str, Any], *, candidate_path: Path, work_dir: Path
) -> list[dict[str, Any]]:
    """Extract, validate, and append every selected payload in one archive.

    Payload files are written once under a lane-owned work directory and kept
    as evidence.  Existing payload paths are treated as a failed rerun rather
    than being overwritten.
    """
    archive_path = Path(str(group["archive_path"]))
    expected_archive_hash = str(group["archive_sha256"]).lower()
    actual_archive_hash = _sha256_file(archive_path)
    if actual_archive_hash != expected_archive_hash:
        raise ValueError(
            f"archive SHA-256 mismatch for {archive_path}: "
            f"expected={expected_archive_hash} actual={actual_archive_hash}"
        )
    entries = list(group.get("entries", []))
    if not entries:
        raise ValueError(f"campaign group has no entries: {archive_path}")
    identities = [_identity(entry) for entry in entries]
    if len(identities) != len(set(identities)):
        raise ValueError(f"campaign group has duplicate canonical identities: {archive_path}")

    payload_dir = Path(work_dir) / "payloads"
    payload_dir.mkdir(parents=True, exist_ok=True)
    expected_schema: dict[str, list[tuple[str, str]]] | None = None
    records: list[dict[str, Any]] = []
    for entry in sorted(entries, key=_identity):
        payload, receipt = read_validated_payload(archive_path, entry)
        payload_path = payload_dir / f"{int(entry['season'])}_{entry['league_id']}.duckdb"
        try:
            with payload_path.open("xb") as handle:
                handle.write(payload)
        except FileExistsError as error:
            raise FileExistsError(f"refusing to overwrite extracted payload evidence: {payload_path}") from error
        expected_schema = append_canonical_payload(
            candidate_path, payload_path, expected_schema=expected_schema
        )
        records.append(
            {
                "season": int(entry["season"]),
                "league_id": str(entry["league_id"]),
                "db_name": str(entry["db_name"]),
                "archive_path": str(archive_path),
                "archive_sha256": actual_archive_hash,
                "payload_path": str(payload_path),
                "payload_sha256": hashlib.sha256(payload).hexdigest(),
                "receipt": receipt,
            }
        )
    return records


def _identity(entry: dict[str, Any]) -> tuple[int, str]:
    return int(entry["season"]), str(entry["league_id"])


def _entry_db_name(entry: dict[str, Any]) -> str:
    return str(entry.get("db_name") or f"mfl_{int(entry['season'])}_{entry['league_id']}")


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
