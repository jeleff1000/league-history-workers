"""Read-only validation for recovered original MFL league payloads."""

from __future__ import annotations

import hashlib
import json
import argparse
from pathlib import Path
from pathlib import PurePosixPath
import shutil
import tempfile
from typing import Any, Iterable, Mapping
import zipfile

import duckdb

from mfl_original_recovery_audit import normalize_mfl_identity


REQUIRED_TABLES = ("league_settings", "matchup", "player_fantasy")


class PayloadAuditError(RuntimeError):
    """A receipt and its claimed full-schema database do not agree."""


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(8 * 1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _safe_members(archive: zipfile.ZipFile) -> dict[str, zipfile.ZipInfo]:
    members: dict[str, zipfile.ZipInfo] = {}
    for member in archive.infolist():
        name = member.filename.replace("\\", "/")
        path = PurePosixPath(name)
        if not name or path.is_absolute() or ".." in path.parts or name in members:
            raise PayloadAuditError(f"source artifact contains unsafe or duplicate path: {member.filename!r}")
        if not member.is_dir():
            members[name] = member
    return members


def _table_columns(connection: duckdb.DuckDBPyConnection, table: str) -> list[str]:
    return [str(row[0]) for row in connection.execute(f'DESCRIBE public."{table}"').fetchall()]


def inspect_receipt_payload(receipt: Mapping[str, Any], database: Path) -> dict[str, object]:
    """Validate one MFL receipt against its actual immutable ``league.duckdb``.

    The database is opened read-only and is never modified.  Receipt evidence
    is accepted only when its normalized identity, row counts, and declared
    core schemas all match the payload's actual public tables.
    """

    if str(receipt.get("schema_version", "")) != "mfl_register_receipt_v1":
        raise PayloadAuditError("receipt has an unsupported schema version")
    try:
        identity = normalize_mfl_identity(receipt["season"], receipt["league_id"])
    except (KeyError, TypeError, ValueError) as exc:
        raise PayloadAuditError("receipt has no valid MFL identity") from exc
    expected_db_name = f"mfl_{identity[0]}_{identity[1]}"
    if str(receipt.get("db_name", "")) != expected_db_name:
        raise PayloadAuditError("receipt db_name does not match its normalized identity")
    if not Path(database).is_file():
        raise PayloadAuditError(f"receipt is missing its league database: {database}")

    connection = duckdb.connect(str(database), read_only=True)
    try:
        tables = {
            str(row[0])
            for row in connection.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'"
            ).fetchall()
        }
        missing = sorted(set(REQUIRED_TABLES) - tables)
        if missing:
            raise PayloadAuditError(f"payload lacks required full-schema tables: {missing}")

        actual_counts = {
            table: int(connection.execute(f'SELECT COUNT(*) FROM public."{table}"').fetchone()[0])
            for table in REQUIRED_TABLES
        }
        if actual_counts["league_settings"] <= 0:
            raise PayloadAuditError("payload has no league_settings row")
        receipt_counts = receipt.get("row_counts")
        if not isinstance(receipt_counts, Mapping):
            raise PayloadAuditError("receipt lacks row_counts")
        for table in ("matchup", "player_fantasy"):
            try:
                expected_count = int(receipt_counts[table])
            except (KeyError, TypeError, ValueError) as exc:
                raise PayloadAuditError(f"receipt lacks {table} row count") from exc
            if actual_counts[table] != expected_count:
                raise PayloadAuditError(
                    f"receipt {table} row count {expected_count} differs from payload {actual_counts[table]}"
                )

        schema_columns = receipt.get("schema_columns")
        if not isinstance(schema_columns, Mapping):
            raise PayloadAuditError("receipt lacks schema_columns")
        for table in ("matchup", "player_fantasy"):
            expected_columns = schema_columns.get(table)
            if not isinstance(expected_columns, list) or not all(isinstance(column, str) for column in expected_columns):
                raise PayloadAuditError(f"receipt lacks {table} schema columns")
            actual_columns = _table_columns(connection, table)
            if set(actual_columns) != set(expected_columns) or len(actual_columns) != len(expected_columns):
                raise PayloadAuditError(f"receipt {table} schema does not match payload")

        settings_columns = {column.lower(): column for column in _table_columns(connection, "league_settings")}
        year_column = settings_columns.get("year") or settings_columns.get("season")
        db_name_column = settings_columns.get("db_name")
        if not year_column or not db_name_column:
            raise PayloadAuditError("league_settings lacks payload identity columns")
        settings_identities = {
            normalize_mfl_identity(year, db_name)
            for year, db_name in connection.execute(
                f'SELECT DISTINCT "{year_column}", "{db_name_column}" FROM public.league_settings'
            ).fetchall()
        }
        if settings_identities != {identity}:
            raise PayloadAuditError(f"league_settings identity differs from receipt: {sorted(settings_identities)}")
        return {"identity": identity, "row_counts": actual_counts}
    finally:
        connection.close()


def inspect_source_archive(
    archive_path: Path,
    *,
    provenance: Mapping[str, Any],
    scratch_directory: Path,
) -> dict[str, object]:
    """Inspect every receipt carried by one immutable original source archive.

    A malformed receipt or payload is reported in ``invalid_payloads`` while
    other receipts in the same source artifact continue to be inspected.  The
    only temporary extraction is one ``league.duckdb`` at a time below the
    caller-provided scratch directory; it is removed immediately after the
    read-only validation completes.
    """

    required_provenance = ("artifact_id", "artifact_name", "run_id")
    missing_provenance = [field for field in required_provenance if provenance.get(field) is None]
    if missing_provenance:
        raise PayloadAuditError(f"source artifact lacks provenance: {missing_provenance}")
    scratch_directory.mkdir(parents=True, exist_ok=True)
    valid_payloads: list[dict[str, object]] = []
    invalid_payloads: list[dict[str, object]] = []
    batch_failures: list[dict[str, object]] = []
    try:
        archive = zipfile.ZipFile(archive_path)
    except (OSError, zipfile.BadZipFile) as exc:
        raise PayloadAuditError(f"cannot open original source artifact: {archive_path}") from exc
    with archive:
        members = _safe_members(archive)
        for name, member in members.items():
            if PurePosixPath(name).name != "batch_receipt.json":
                continue
            try:
                value = json.loads(archive.read(member).decode("utf-8"))
                failures = value.get("failures", []) if isinstance(value, dict) else []
                if not isinstance(failures, list):
                    raise ValueError("failures is not a list")
                batch_failures.extend(dict(row) for row in failures if isinstance(row, dict))
            except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
                invalid_payloads.append({"receipt_path": name, "reason": f"malformed batch receipt: {exc}"})

        for name, member in members.items():
            if PurePosixPath(name).name != "receipt.json":
                continue
            try:
                receipt = json.loads(archive.read(member).decode("utf-8"))
                if not isinstance(receipt, dict):
                    raise PayloadAuditError("receipt is not a JSON object")
                database_member_name = f"{PurePosixPath(name).parent.as_posix()}/league.duckdb"
                database_member = members.get(database_member_name)
                if database_member is None:
                    raise PayloadAuditError("receipt is missing paired league.duckdb")
                with tempfile.NamedTemporaryFile(
                    prefix="mfl-payload-", suffix=".duckdb", dir=scratch_directory, delete=False
                ) as temporary:
                    temporary_path = Path(temporary.name)
                    with archive.open(database_member) as source:
                        shutil.copyfileobj(source, temporary, length=8 * 1024 * 1024)
                try:
                    result = inspect_receipt_payload(receipt, temporary_path)
                    valid_payloads.append(
                        {
                            "artifact_id": int(provenance["artifact_id"]),
                            "artifact_name": str(provenance["artifact_name"]),
                            "run_id": int(provenance["run_id"]),
                            "created_at": provenance.get("created_at"),
                            "identity": result["identity"],
                            "payload_sha256": _sha256(temporary_path),
                        }
                    )
                finally:
                    temporary_path.unlink(missing_ok=True)
            except (UnicodeDecodeError, json.JSONDecodeError, PayloadAuditError, OSError) as exc:
                invalid_payloads.append({"receipt_path": name, "reason": str(exc)})
    return {
        "valid_payloads": valid_payloads,
        "invalid_payloads": invalid_payloads,
        "batch_failures": batch_failures,
    }


def audit_landed_source_archives(
    destination: Path,
    *,
    run_id: int,
    scratch_directory: Path,
) -> dict[str, object]:
    """Inspect all original source archives inside a fully landed lane run.

    This builds evidence only.  It neither merges the payloads nor changes a
    cache, and carries the original Actions artifact id/name/run/timestamp all
    the way into every valid-payload record.
    """

    campaign = Path(destination) / "campaigns" / str(run_id)
    if not campaign.is_dir():
        raise PayloadAuditError(f"landed source campaign does not exist: {campaign}")
    source_archive_count = 0
    valid_payloads: list[dict[str, object]] = []
    invalid_payloads: list[dict[str, object]] = []
    batch_failures: list[dict[str, object]] = []
    for landing in sorted(path for path in campaign.iterdir() if path.is_dir()):
        manifest_path = landing / "lane-manifest.json"
        try:
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise PayloadAuditError(f"cannot read landed lane manifest: {manifest_path}") from exc
        artifacts = manifest.get("artifacts") if isinstance(manifest, dict) else None
        if not isinstance(artifacts, list):
            raise PayloadAuditError(f"landed lane manifest has no source artifact list: {manifest_path}")
        for artifact in artifacts:
            if not isinstance(artifact, dict):
                raise PayloadAuditError(f"landed lane manifest has malformed source artifact: {manifest_path}")
            try:
                artifact_id = int(artifact["artifact_id"])
                source_run_id = int(artifact["run_id"])
                artifact_name = str(artifact["name"])
            except (KeyError, TypeError, ValueError) as exc:
                raise PayloadAuditError(f"landed lane artifact lacks provenance: {manifest_path}") from exc
            raw_archive = landing / "artifacts" / f"{artifact_id}.zip"
            if not raw_archive.is_file():
                raise PayloadAuditError(f"landed lane artifact is missing its archive: {raw_archive}")
            source_archive_count += 1
            result = inspect_source_archive(
                raw_archive,
                provenance={
                    "artifact_id": artifact_id,
                    "artifact_name": artifact_name,
                    "run_id": source_run_id,
                    "created_at": artifact.get("created_at"),
                },
                scratch_directory=scratch_directory,
            )
            valid_payloads.extend(result["valid_payloads"])
            invalid_payloads.extend(
                [{"artifact_id": artifact_id, **row} for row in result["invalid_payloads"]]
            )
            batch_failures.extend(
                [{"artifact_id": artifact_id, **row} for row in result["batch_failures"]]
            )
    return {
        "run_id": run_id,
        "source_archive_count": source_archive_count,
        "valid_payload_count": len(valid_payloads),
        "valid_payloads": valid_payloads,
        "invalid_payload_count": len(invalid_payloads),
        "invalid_payloads": invalid_payloads,
        "batch_failure_count": len(batch_failures),
        "batch_failures": batch_failures,
    }


def _require_d_path(path: Path, *, label: str) -> None:
    if Path(path).drive.upper() != "D:":
        raise PayloadAuditError(f"{label} must be on D:")


def main(argv: Iterable[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Audit recovered original MFL payloads without cache mutation")
    parser.add_argument("--destination", type=Path, required=True)
    parser.add_argument("--run-id", type=int, required=True)
    parser.add_argument("--scratch-directory", type=Path, required=True)
    parser.add_argument("--output-directory", type=Path, required=True)
    args = parser.parse_args(list(argv) if argv is not None else None)
    _require_d_path(args.destination, label="destination")
    _require_d_path(args.scratch_directory, label="scratch directory")
    _require_d_path(args.output_directory, label="output directory")
    if args.output_directory.exists():
        raise PayloadAuditError(f"audit output directory already exists: {args.output_directory}")

    report = audit_landed_source_archives(
        args.destination,
        run_id=args.run_id,
        scratch_directory=args.scratch_directory,
    )
    args.output_directory.mkdir(parents=True, exist_ok=False)
    (args.output_directory / "artifact_recovery_report.json").write_text(
        json.dumps(report, indent=2, sort_keys=True), encoding="utf-8"
    )
    (args.output_directory / "invalid_payload_report.json").write_text(
        json.dumps(report["invalid_payloads"], indent=2, sort_keys=True), encoding="utf-8"
    )
    (args.output_directory / "batch_failure_report.json").write_text(
        json.dumps(report["batch_failures"], indent=2, sort_keys=True), encoding="utf-8"
    )
    print(
        json.dumps(
            {
                key: report[key]
                for key in ("run_id", "source_archive_count", "valid_payload_count", "invalid_payload_count", "batch_failure_count")
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except PayloadAuditError as error:
        print(f"payload audit failed: {error}")
        raise SystemExit(2)
