from pathlib import Path


WORKFLOW = Path(__file__).parents[1] / ".github/workflows/mfl_reduce_saved_artifact.yml"


def test_artifact_reducer_downloads_source_and_uses_canonical_merger():
    text = WORKFLOW.read_text(encoding="utf-8")
    assert "actions/download-artifact@v5" in text
    assert "merge_mfl_saved_chunk.py" in text
    assert "prior_index_key" in text


def test_artifact_reducer_requires_exact_output_cache_validation():
    text = WORKFLOW.read_text(encoding="utf-8")
    assert "fail-on-cache-miss: true" in text
    assert "cache_append_proof.json" in text
    assert "Save recovered canonical index" in text
