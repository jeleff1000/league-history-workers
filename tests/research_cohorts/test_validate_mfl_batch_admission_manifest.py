from pathlib import Path
import subprocess
import sys


SCRIPT = (
    Path(__file__).resolve().parents[2]
    / "scripts"
    / "research_cohorts"
    / "validate_mfl_batch_admission_manifest.py"
)


def test_accepts_the_complete_mfl_batch_admission_partition(tmp_path: Path) -> None:
    manifest = tmp_path / "manifest.csv"
    manifest.write_text(
        "artifact,artifact_id,admission_state,candidate_rows\n"
        "a,1,included_in_verified_cache_batch,2\n"
        "b,2,admitted_no_promotable_delta,0\n"
        "c,3,source_diagnostic_only,0\n"
        "d,4,explicitly_excluded_no_candidate,0\n",
        encoding="utf-8",
    )
    result = subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "--manifest",
            str(manifest),
            "--expected-total",
            "4",
            "--expected-included",
            "1",
            "--expected-zero-delta",
            "1",
            "--expected-diagnostic",
            "1",
            "--expected-excluded",
            "1",
        ],
        text=True,
        capture_output=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr


def test_rejects_included_artifact_without_a_candidate_row(tmp_path: Path) -> None:
    manifest = tmp_path / "manifest.csv"
    manifest.write_text(
        "artifact,artifact_id,admission_state,candidate_rows\n"
        "a,1,included_in_verified_cache_batch,0\n",
        encoding="utf-8",
    )
    result = subprocess.run(
        [sys.executable, str(SCRIPT), "--manifest", str(manifest)],
        text=True,
        capture_output=True,
        check=False,
    )
    assert result.returncode != 0
    assert "must have candidate_rows > 0" in result.stderr
