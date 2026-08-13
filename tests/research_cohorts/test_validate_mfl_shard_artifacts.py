from pathlib import Path
import subprocess
import sys


SCRIPT = (
    Path(__file__).resolve().parents[2]
    / "scripts"
    / "research_cohorts"
    / "validate_mfl_shard_artifacts.py"
)


def _run(tmp_path: Path, *, jobs: str, artifacts: str, excluded: str = "") -> subprocess.CompletedProcess[str]:
    jobs_path = tmp_path / "jobs.tsv"
    artifacts_path = tmp_path / "artifacts.txt"
    report_path = tmp_path / "report.json"
    jobs_path.write_text(jobs, encoding="utf-8")
    artifacts_path.write_text(artifacts, encoding="utf-8")
    return subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "--jobs", str(jobs_path),
            "--artifacts", str(artifacts_path),
            "--excluded-shards", excluded,
            "--report", str(report_path),
        ],
        text=True,
        capture_output=True,
        check=False,
    )


def test_accepts_exact_complete_nonexcluded_artifact_set(tmp_path: Path) -> None:
    result = _run(
        tmp_path,
        jobs="rescue (0, 256)\tsuccess\nrescue (1, 256)\tfailure\nrescue (2, 256)\tcancelled\n",
        artifacts=(
            "research-mfl-classification-roster-bridge-shard-0-31663728938\n"
            "research-mfl-classification-roster-bridge-shard-1-31663728938\n"
        ),
        excluded="2",
    )
    assert result.returncode == 0, result.stderr


def test_rejects_missing_nonexcluded_artifact(tmp_path: Path) -> None:
    result = _run(
        tmp_path,
        jobs="rescue (0, 256)\tsuccess\nrescue (1, 256)\tfailure\n",
        artifacts="research-mfl-classification-roster-bridge-shard-0-31663728938\n",
    )
    assert result.returncode != 0
    assert "artifact indices do not equal selected rescue indices" in result.stderr


def test_rejects_nonterminal_shard_unless_explicitly_excluded(tmp_path: Path) -> None:
    result = _run(
        tmp_path,
        jobs="rescue (156, 256)\tin_progress\n",
        artifacts="",
    )
    assert result.returncode != 0
    assert "nonterminal selected rescue shards" in result.stderr
