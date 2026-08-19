"""Resumable, hash-verified upload primitives for MFL recovery inputs."""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import shlex
import subprocess
from typing import Any


def _sha256_file(path: Path, chunk_size: int) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(chunk_size):
            digest.update(chunk)
    return digest.hexdigest()


def _normalized_path(path: Path) -> str:
    return os.path.normcase(os.path.normpath(str(path)))


def _archive_inventory(artifact_root: Path) -> dict[str, Path]:
    if not artifact_root.is_dir():
        raise FileNotFoundError(f"artifact root is not present locally: {artifact_root}")
    try:
        result = subprocess.run(
            ["rg", "--files", "--glob", "*.zip", str(artifact_root)],
            check=False,
            capture_output=True,
            text=True,
        )
    except FileNotFoundError as exc:
        raise RuntimeError("ripgrep (rg) is required to inventory MFL source archives") from exc
    if result.returncode not in (0, 1):
        raise RuntimeError(f"rg failed while inventorying MFL source archives: {result.stderr.strip()}")
    return {
        _normalized_path(Path(line)): Path(line)
        for line in result.stdout.splitlines()
        if line.strip()
    }


def _require_within(root: Path, path: Path) -> str:
    root_key = _normalized_path(root)
    path_key = _normalized_path(path)
    if os.path.commonpath([root_key, path_key]) != root_key:
        raise ValueError(f"manifest archive is outside artifact root: {path}")
    return path_key


def build_upload_plan(
    manifest_path: Path,
    *,
    artifact_root: Path,
    protected_source: Path | None = None,
) -> dict[str, Any]:
    """Resolve every immutable manifest source or fail before any transfer."""
    manifest_path = Path(manifest_path)
    artifact_root = Path(artifact_root)
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    entries = manifest.get("entries")
    expected_count = int(manifest.get("expected_count", 0))
    if not isinstance(entries, list) or expected_count <= 0 or len(entries) != expected_count:
        raise ValueError(
            f"manifest entry count is invalid: expected={expected_count} actual={len(entries) if isinstance(entries, list) else 'not-a-list'}"
        )
    if int(manifest.get("missing_count", 0)) != 0:
        raise ValueError("manifest itself reports missing identities")

    archive_inventory = _archive_inventory(artifact_root)
    archives: set[Path] = set()
    protected_entries: list[dict[str, Any]] = []
    for entry in entries:
        source_type = entry.get("source_type")
        if source_type == "campaign_artifact":
            archive_value = entry.get("archive")
            if not archive_value:
                raise ValueError(f"campaign artifact entry has no archive: {entry}")
            archive = Path(str(archive_value))
            archive_key = _require_within(artifact_root, archive)
            inventory_archive = archive_inventory.get(archive_key)
            if inventory_archive is None:
                raise FileNotFoundError(f"manifest archive is not present locally: {archive}")
            archives.add(inventory_archive)
        elif source_type == "protected_source_chunk":
            protected_entries.append(entry)
        else:
            raise ValueError(f"manifest has unsupported source type: {source_type!r}")

    protected_plan: dict[str, Any] | None = None
    if protected_entries:
        if protected_source is None:
            raise FileNotFoundError("manifest requires an explicit protected source chunk")
        protected_source = Path(protected_source).resolve()
        if not protected_source.is_file():
            raise FileNotFoundError(f"protected source chunk is not present locally: {protected_source}")
        expected_digests = {
            str(entry.get("digest", "")).removeprefix("sha256:").lower()
            for entry in protected_entries
        }
        if len(expected_digests) != 1 or not next(iter(expected_digests)):
            raise ValueError("protected source entries do not have one immutable digest")
        actual_digest = _sha256_file(protected_source, 1024 * 1024)
        expected_digest = next(iter(expected_digests))
        if actual_digest != expected_digest:
            raise ValueError(
                f"protected source SHA-256 differs from manifest: expected={expected_digest} actual={actual_digest}"
            )
        protected_plan = {
            "path": protected_source,
            "sha256": actual_digest,
            "identity_count": len(protected_entries),
        }

    return {
        "manifest_path": manifest_path.resolve(),
        "expected_identity_count": expected_count,
        "campaign_archives": sorted(archives),
        "protected_source": protected_plan,
    }


def build_campaign_transfer_records(
    manifest_path: Path,
    *,
    artifact_root: Path,
    remote_campaign_root: str,
) -> list[dict[str, Path | str]]:
    """Create one immutable, deduplicated transfer record per campaign ZIP."""
    manifest = json.loads(Path(manifest_path).read_text(encoding="utf-8"))
    entries = manifest.get("entries")
    expected_count = int(manifest.get("expected_count", 0))
    if not isinstance(entries, list) or expected_count <= 0 or len(entries) != expected_count:
        raise ValueError("manifest entry count is invalid")
    if int(manifest.get("missing_count", 0)) != 0:
        raise ValueError("manifest itself reports missing identities")

    artifact_root = Path(artifact_root)
    inventory = _archive_inventory(artifact_root)
    records_by_archive: dict[str, dict[str, Path | str]] = {}
    remote_root = PurePosixPath(remote_campaign_root)
    for entry in entries:
        if entry.get("source_type") != "campaign_artifact":
            continue
        archive_value = entry.get("archive")
        archive_sha256 = str(entry.get("archive_sha256", "")).lower()
        if not archive_value or len(archive_sha256) != 64 or any(
            character not in "0123456789abcdef" for character in archive_sha256
        ):
            raise ValueError(f"campaign artifact entry has invalid archive provenance: {entry}")
        archive_key = _require_within(artifact_root, Path(str(archive_value)))
        local_path = inventory.get(archive_key)
        if local_path is None:
            raise FileNotFoundError(f"manifest archive is not present locally: {archive_value}")
        relative_path = local_path.relative_to(artifact_root)
        remote_path = str(remote_root.joinpath(*relative_path.parts))
        record = {"local_path": local_path, "remote_path": remote_path, "sha256": archive_sha256}
        existing = records_by_archive.get(archive_key)
        if existing is not None and existing != record:
            raise ValueError(f"one archive has conflicting provenance in the manifest: {local_path}")
        records_by_archive[archive_key] = record
    return sorted(records_by_archive.values(), key=lambda record: str(record["remote_path"]))


class ParamikoTransport:
    """Small Paramiko adapter used by the hash-verified upload primitive."""

    def __init__(self, *, sftp: Any, ssh: Any) -> None:
        self._sftp = sftp
        self._ssh = ssh

    def size(self, path: str) -> int:
        return int(self._sftp.stat(path).st_size)

    def append(self, path: str) -> Any:
        return self._sftp.file(path, "ab")

    def sha256(self, path: str) -> str:
        _, stdout, stderr = self._ssh.exec_command(f"sha256sum -- {shlex.quote(path)}")
        exit_code = stdout.channel.recv_exit_status()
        output = stdout.read().decode("utf-8", errors="replace").strip()
        error = stderr.read().decode("utf-8", errors="replace").strip()
        if exit_code != 0:
            raise RuntimeError(f"remote sha256sum failed for {path}: {error or output}")
        digest = output.split(maxsplit=1)[0] if output else ""
        if len(digest) != 64 or any(character not in "0123456789abcdef" for character in digest.lower()):
            raise RuntimeError(f"remote sha256sum returned an invalid digest for {path}: {output!r}")
        return digest.lower()


def sync_file(
    sftp: Any,
    local_path: Path,
    remote_path: str,
    *,
    chunk_size: int = 1024 * 1024,
) -> dict[str, int | str]:
    """Append a local file to a partial remote file and verify its SHA-256.

    ``sftp`` supplies only the small interface this operation needs: ``size``,
    ``append``, and ``sha256``.  The production transport adapter is kept out
    of this integrity-critical primitive so it can be tested without a server.
    """
    local_path = Path(local_path)
    if not local_path.is_file():
        raise FileNotFoundError(local_path)
    if chunk_size <= 0:
        raise ValueError("chunk_size must be positive")

    local_size = local_path.stat().st_size
    local_sha256 = _sha256_file(local_path, chunk_size)
    try:
        remote_size = sftp.size(remote_path)
    except FileNotFoundError:
        remote_size = 0

    if remote_size > local_size:
        raise ValueError(
            f"remote file is larger than local source: remote={remote_size} local={local_size}"
        )

    if remote_size == local_size:
        remote_sha256 = sftp.sha256(remote_path)
        if remote_sha256 != local_sha256:
            raise ValueError("remote file has local size but a different SHA-256")
        return {
            "status": "already_verified",
            "local_bytes": local_size,
            "resumed_bytes": remote_size,
            "uploaded_bytes": 0,
            "sha256": local_sha256,
        }

    uploaded_bytes = 0
    remote_handle = sftp.append(remote_path)
    try:
        with local_path.open("rb") as local_handle:
            local_handle.seek(remote_size)
            while chunk := local_handle.read(chunk_size):
                remote_handle.write(chunk)
                uploaded_bytes += len(chunk)
    finally:
        remote_handle.close()

    final_remote_size = sftp.size(remote_path)
    if final_remote_size != local_size:
        raise ValueError(
            f"remote size mismatch after upload: remote={final_remote_size} local={local_size}"
        )
    remote_sha256 = sftp.sha256(remote_path)
    if remote_sha256 != local_sha256:
        raise ValueError("remote SHA-256 mismatch after upload")

    return {
        "status": "uploaded_verified",
        "local_bytes": local_size,
        "resumed_bytes": remote_size,
        "uploaded_bytes": uploaded_bytes,
        "sha256": local_sha256,
    }
