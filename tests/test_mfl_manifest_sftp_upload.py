from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from scripts.extraplatform_corpus.mfl_manifest_sftp_upload import (
    ParamikoTransport,
    build_campaign_transfer_records,
    build_upload_plan,
    sync_file,
)


class _AppendHandle:
    def __init__(self, storage: dict[str, bytearray], path: str) -> None:
        self._storage = storage
        self._path = path

    def write(self, chunk: bytes) -> None:
        self._storage.setdefault(self._path, bytearray()).extend(chunk)

    def close(self) -> None:
        pass


class _FakeSftp:
    def __init__(self, storage: dict[str, bytes]) -> None:
        self.storage = {path: bytearray(data) for path, data in storage.items()}

    def size(self, path: str) -> int:
        return len(self.storage[path])

    def append(self, path: str) -> _AppendHandle:
        return _AppendHandle(self.storage, path)

    def sha256(self, path: str) -> str:
        return hashlib.sha256(self.storage[path]).hexdigest()


class _FakeChannel:
    def recv_exit_status(self) -> int:
        return 0


class _FakeStream:
    def __init__(self, data: bytes) -> None:
        self._data = data
        self.channel = _FakeChannel()

    def read(self) -> bytes:
        return self._data


class _FakeSsh:
    def __init__(self) -> None:
        self.command = ""

    def exec_command(self, command: str):
        self.command = command
        return None, _FakeStream(b"a" * 64 + b"  /remote/source.bin\n"), _FakeStream(b"")


def test_sync_file_resumes_partial_remote_bytes_and_verifies_hash(tmp_path) -> None:
    local = tmp_path / "source.bin"
    local.write_bytes(b"abcdef")
    sftp = _FakeSftp({"/remote/source.bin": b"abc"})

    result = sync_file(sftp, local, "/remote/source.bin", chunk_size=2)

    assert bytes(sftp.storage["/remote/source.bin"]) == b"abcdef"
    assert result["status"] == "uploaded_verified"
    assert result["resumed_bytes"] == 3
    assert result["uploaded_bytes"] == 3


def test_sync_file_rejects_remote_file_larger_than_local(tmp_path) -> None:
    local = tmp_path / "source.bin"
    local.write_bytes(b"abc")
    sftp = _FakeSftp({"/remote/source.bin": b"abcdef"})

    with pytest.raises(ValueError, match="larger than local"):
        sync_file(sftp, local, "/remote/source.bin")


def test_paramiko_transport_hashes_remote_path_as_one_shell_argument() -> None:
    ssh = _FakeSsh()
    transport = ParamikoTransport(sftp=object(), ssh=ssh)

    digest = transport.sha256("/remote/a file;not-a-command")

    assert digest == "a" * 64
    assert ssh.command == "sha256sum -- '/remote/a file;not-a-command'"


def test_build_upload_plan_refuses_a_manifest_archive_missing_on_disk(tmp_path) -> None:
    artifact_root = tmp_path / "artifacts"
    artifact_root.mkdir()
    present = artifact_root / "present.zip"
    present.write_bytes(b"archive")
    manifest = tmp_path / "manifest.json"
    manifest.write_text(
        json.dumps(
            {
                "expected_count": 2,
                "missing_count": 0,
                "entries": [
                    {"source_type": "campaign_artifact", "archive": str(present)},
                    {
                        "source_type": "campaign_artifact",
                        "archive": str(artifact_root / "absent.zip"),
                    },
                ],
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(FileNotFoundError, match="absent.zip"):
        build_upload_plan(manifest, artifact_root=artifact_root)


def test_build_campaign_transfer_records_deduplicates_archive_and_keeps_manifest_hash(tmp_path) -> None:
    artifact_root = tmp_path / "artifacts"
    nested = artifact_root / "123"
    nested.mkdir(parents=True)
    archive = nested / "source.zip"
    archive.write_bytes(b"archive")
    manifest = tmp_path / "manifest.json"
    manifest.write_text(
        json.dumps(
            {
                "expected_count": 2,
                "missing_count": 0,
                "entries": [
                    {
                        "source_type": "campaign_artifact",
                        "archive": str(archive),
                        "archive_sha256": "a" * 64,
                    },
                    {
                        "source_type": "campaign_artifact",
                        "archive": str(archive),
                        "archive_sha256": "a" * 64,
                    },
                ],
            }
        ),
        encoding="utf-8",
    )

    records = build_campaign_transfer_records(
        manifest,
        artifact_root=artifact_root,
        remote_campaign_root="/remote/campaign",
    )

    assert records == [
        {
            "local_path": archive,
            "remote_path": "/remote/campaign/123/source.zip",
            "sha256": "a" * 64,
        }
    ]
