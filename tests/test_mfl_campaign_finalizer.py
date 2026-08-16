from pathlib import Path


WORKFLOW = Path(__file__).parents[1] / ".github/workflows/mfl_register_batch_campaign.yml"


def test_chunked_campaign_finalizes_canonical_cache_in_combine():
    text = WORKFLOW.read_text(encoding="utf-8")
    assert "merge_mfl_saved_chunk.py" in text
    assert "Save canonical successor index" in text


def test_next_wave_uses_canonical_successor_index():
    text = WORKFLOW.read_text(encoding="utf-8")
    assert "CANONICAL_INDEX_KEY" in text
    assert 'PRIOR_INDEX_KEY="$CANONICAL_INDEX_KEY"' in text


def test_finalizer_proof_is_required_before_canonical_save():
    text = WORKFLOW.read_text(encoding="utf-8")
    assert "canonical_append_proof.json" in text
    assert "fail-on-cache-miss: true" in text


def test_fetch_only_continuation_reads_the_chunk_index():
    text = WORKFLOW.read_text(encoding="utf-8")
    assert '"$STORAGE_MODE" = fetch_only' in text
    assert "ROOT=index_state" in text
