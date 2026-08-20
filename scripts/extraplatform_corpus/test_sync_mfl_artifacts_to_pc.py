from __future__ import annotations

import hashlib
import json
from pathlib import Path
import zipfile

from sync_mfl_artifacts_to_pc import ArtifactLandingError, land_artifact


def _archive(path: Path, files: dict[str, bytes]) -> str:
    with zipfile.ZipFile(path, "w") as zipped:
        for name, content in files.items():
            zipped.writestr(name, content)
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _artifact(archive_sha256: str) -> dict[str, object]:
    return {
        "id": 123,
        "name": "mfl-register-chunk-456",
        "archive_download_url": "https://example.invalid/artifact.zip",
        "digest": f"sha256:{archive_sha256}",
        "size_in_bytes": 1,
        "created_at": "2026-08-20T12:00:00Z",
        "workflow_run": {"id": 456},
    }


def test_landing_a_chunk_is_resumable_and_writes_a_provenance_receipt(tmp_path: Path) -> None:
    archive = tmp_path / "source.zip"
    digest = _archive(
        archive,
        {
            "mfl_register_chunk.duckdb": b"duckdb",
            "mfl_register_all_runs.json": b"{}",
            "cache_append_proof.json": b'{"schema_unchanged": true}',
            "mfl_research_overlay.duckdb": b"overlay",
            "mfl_research_overlay_proof.json": b"{}",
        },
    )
    destination = tmp_path / "D" / "pc_artifacts"
    calls: list[str] = []

    def download(url: str, target: Path) -> None:
        calls.append(url)
        target.write_bytes(archive.read_bytes())

    first = land_artifact(
        _artifact(digest),
        destination=destination,
        download=download,
        required_files={
            "mfl_register_chunk.duckdb",
            "mfl_register_all_runs.json",
            "cache_append_proof.json",
            "mfl_research_overlay.duckdb",
            "mfl_research_overlay_proof.json",
        },
    )

    landed = destination / "campaigns" / "456"
    receipt = json.loads((landed / ".landed.json").read_text(encoding="utf-8"))
    assert first["status"] == "landed"
    assert calls == ["https://example.invalid/artifact.zip"]
    assert receipt["artifact_id"] == 123
    assert receipt["archive_sha256"] == digest
    assert sorted(receipt["files"]) == [
        "cache_append_proof.json",
        "mfl_register_all_runs.json",
        "mfl_register_chunk.duckdb",
        "mfl_research_overlay.duckdb",
        "mfl_research_overlay_proof.json",
    ]

    second = land_artifact(
        _artifact(digest),
        destination=destination,
        download=download,
        required_files={"mfl_register_chunk.duckdb"},
    )
    assert second["status"] == "already_landed"
    assert len(calls) == 1


def test_landing_refuses_to_publish_an_incomplete_chunk(tmp_path: Path) -> None:
    archive = tmp_path / "source.zip"
    digest = _archive(archive, {"mfl_register_chunk.duckdb": b"duckdb"})
    destination = tmp_path / "D" / "pc_artifacts"

    try:
        land_artifact(
            _artifact(digest),
            destination=destination,
            download=lambda _url, target: target.write_bytes(archive.read_bytes()),
            required_files={"mfl_register_chunk.duckdb", "mfl_register_all_runs.json"},
        )
    except ArtifactLandingError as exc:
        assert "required files" in str(exc)
    else:
        raise AssertionError("incomplete artifact was published")

    assert not (destination / "campaigns" / "456").exists()
