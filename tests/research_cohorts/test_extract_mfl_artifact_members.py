from pathlib import Path
import subprocess
import sys
import zipfile


SCRIPT = (
    Path(__file__).resolve().parents[2]
    / "scripts"
    / "research_cohorts"
    / "extract_mfl_artifact_members.py"
)


def _archive(path: Path, *, include_bridge: bool = True) -> None:
    with zipfile.ZipFile(path, "w") as out:
        out.writestr("out/team_signals.parquet", b"team-signals")
        if include_bridge:
            out.writestr("out/player_bridge.parquet", b"player-bridge")
        out.writestr("cache_before.json", b"not-promotable")


def test_extracts_only_required_members(tmp_path: Path) -> None:
    archive = tmp_path / "artifact.zip"
    output = tmp_path / "output"
    _archive(archive)

    result = subprocess.run(
        [sys.executable, str(SCRIPT), "--archive", str(archive), "--out", str(output)],
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    assert (output / "team_signals.parquet").read_bytes() == b"team-signals"
    assert (output / "player_bridge.parquet").read_bytes() == b"player-bridge"
    assert not (output / "cache_before.json").exists()


def test_refuses_archive_missing_required_member(tmp_path: Path) -> None:
    archive = tmp_path / "artifact.zip"
    _archive(archive, include_bridge=False)

    result = subprocess.run(
        [sys.executable, str(SCRIPT), "--archive", str(archive), "--out", str(tmp_path / "output")],
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode != 0
    assert "player_bridge" in result.stderr
