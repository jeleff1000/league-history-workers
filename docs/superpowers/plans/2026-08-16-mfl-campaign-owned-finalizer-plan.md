# MFL Campaign-Owned Finalizer Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the chunked MFL campaign save and validate its canonical successor cache in the campaign workflow, so successful campaigns are automatically registered without manually dispatched reducer runs.

**Architecture:** Keep parallel `ingest` jobs and the existing `combine` receipt/chunk construction. In the same `combine` runner, invoke the existing validated saved-chunk merger using the exact source chunk/index and prior canonical index, then save both source artifacts and canonical successor artifacts. `verify_cache` validates the canonical successor and `continue_population` passes its index key to the next campaign.

**Tech Stack:** GitHub Actions YAML, Bash, Python 3.11, DuckDB, `actions/cache@v5`, pytest.

**Spec:** `docs/superpowers/specs/2026-08-16-mfl-campaign-owned-finalizer-design.md`

## Global Constraints

- Preserve immutable Actions cache keys; never overwrite or prefix-restore canonical state.
- Preserve `schema_unchanged`, `prior_rows_preserved`, `new_lineage`, and `duplicate_league_year_keys` proof invariants.
- Keep the standalone reducer workflow available for recovery of already-saved chunks.
- Do not change the canonical table schemas or lineage identifiers.

---

### Task 1: Add failing workflow-shape tests

**Files:**
- Create: `tests/test_mfl_campaign_finalizer.py`
- Test workflow: `.github/workflows/mfl_register_batch_campaign.yml`

**Interfaces:**
- Test reads the workflow as text and checks the campaign-owned finalizer contract.

- [ ] **Step 1: Write the failing tests**

```python
from pathlib import Path


WORKFLOW = Path(__file__).parents[1] / ".github/workflows/mfl_register_batch_campaign.yml"


def test_chunked_campaign_finalizes_canonical_cache_in_combine():
    text = WORKFLOW.read_text(encoding="utf-8")
    assert "merge_mfl_saved_chunk.py" in text
    assert "Save canonical successor index" in text


def test_next_wave_uses_canonical_successor_index():
    text = WORKFLOW.read_text(encoding="utf-8")
    assert "CANONICAL_INDEX_KEY" in text
    assert "prior_index_key=\"$CANONICAL_INDEX_KEY\"" in text


def test_finalizer_proof_is_required_before_canonical_save():
    text = WORKFLOW.read_text(encoding="utf-8")
    assert "canonical_append_proof.json" in text
    assert "fail-on-cache-miss: true" in text
```

- [ ] **Step 2: Run the tests and verify they fail for the missing finalizer**

Run: `python -m pytest tests/test_mfl_campaign_finalizer.py -q`

Expected: FAIL because the campaign workflow does not yet contain the saved-chunk merger, canonical successor key, or canonical proof.

- [ ] **Step 3: Commit the failing tests**

```bash
git add tests/test_mfl_campaign_finalizer.py
git commit -m "test: specify campaign-owned MFL finalizer"
```

### Task 2: Add campaign-owned canonical finalization

**Files:**
- Modify: `.github/workflows/mfl_register_batch_campaign.yml` in `combine`, cache staging, and `verify_cache` sections.

**Interfaces:**
- Inputs remain backward-compatible. `prior_index_key` remains the exact prior canonical index for chunked campaigns.
- Produces `mfl-registered-2004-2018-v2-index-campaign-${{ github.run_id }}`-style immutable successor keys through an explicit `canonical_namespace` input defaulting to `mfl-registered-2004-2018-v2`.

- [ ] **Step 1: Add the canonical namespace input and finalizer environment**

Add `canonical_namespace` with default `mfl-registered-2004-2018-v2`. Define `CANONICAL_SUFFIX=campaign-${{ github.run_id }}` and derive exact canonical chunk/index/overlay keys from that suffix.

- [ ] **Step 2: Restore the prior canonical index into a dedicated path**

Keep the current source-index restore for `merge_mfl_chunk.py`; additionally restore `inputs.prior_index_key` into `prior_canonical_index` with `fail-on-cache-miss: true`, so source construction and canonical append cannot accidentally read the same mutable path.

- [ ] **Step 3: Run the existing saved-chunk merger in `combine`**

After `merge_mfl_chunk.py` creates `mfl_register_chunk.duckdb` and `mfl_register_all_runs.json`, run:

```bash
python3 code/scripts/extraplatform_corpus/merge_mfl_saved_chunk.py \
  --source-db mfl_register_chunk.duckdb \
  --source-index mfl_register_all_runs.json \
  --prior-index prior_canonical_index/mfl_register_all_runs.json \
  --output-db canonical_mfl_register_chunk.duckdb \
  --output-index canonical_mfl_register_all_runs.json \
  --proof canonical_append_proof.json \
  --output-key "$CANONICAL_CHUNK_KEY"
```

Require the same proof invariants and `added_league_count > 0` before proceeding.

- [ ] **Step 4: Build the canonical research overlay**

Run `build_mfl_research_overlay.py` against `canonical_mfl_register_chunk.duckdb`, and retain its exact schema proof.

- [ ] **Step 5: Save canonical successor caches**

Stage and save canonical chunk, canonical index, canonical overlay, and both proofs under immutable keys. Keep saving the original source chunk/index for recovery and audit.

- [ ] **Step 6: Add a live canonical cache verification step**

Restore the exact canonical successor index and proof with `fail-on-cache-miss: true`, then assert all proof invariants and the index count match. This verification must complete before any next-wave dispatch.

- [ ] **Step 7: Run the workflow-shape tests and commit**

Run: `python -m pytest tests/test_mfl_campaign_finalizer.py -q`

Expected: PASS.

```bash
git add .github/workflows/mfl_register_batch_campaign.yml tests/test_mfl_campaign_finalizer.py
git commit -m "feat: finalize MFL campaigns into canonical cache"
```

### Task 3: Route population continuation through canonical state

**Files:**
- Modify: `.github/workflows/mfl_register_batch_campaign.yml` in `verify_cache` dispatch logic.
- Test: `tests/test_mfl_campaign_finalizer.py`

- [ ] **Step 1: Add the failing assertion**

Assert that the continuation environment assigns `CURRENT_INDEX_KEY` from the canonical successor key, not the source chunk index key.

- [ ] **Step 2: Run the focused test and verify it fails**

Run: `python -m pytest tests/test_mfl_campaign_finalizer.py -q`

Expected: FAIL until continuation references the canonical successor.

- [ ] **Step 3: Route `prior_index_key` through `CANONICAL_INDEX_KEY`**

For chunked/fetch-only continuation, pass the canonical successor index and canonical namespace. Preserve the existing snapshot path unchanged.

- [ ] **Step 4: Run focused tests and commit**

Run: `python -m pytest tests/test_mfl_campaign_finalizer.py -q`

Expected: PASS.

```bash
git add .github/workflows/mfl_register_batch_campaign.yml tests/test_mfl_campaign_finalizer.py
git commit -m "feat: continue MFL campaigns from canonical successor"
```

### Task 4: Verify workflow syntax and recovery compatibility

**Files:**
- Inspect: `.github/workflows/mfl_register_batch_campaign.yml`
- Inspect: `.github/workflows/mfl_reduce_saved_chunk.yml`

- [ ] **Step 1: Parse YAML locally**

Run: `python -c "import yaml, pathlib; yaml.safe_load(pathlib.Path('.github/workflows/mfl_register_batch_campaign.yml').read_text())"`

Expected: no parser error.

- [ ] **Step 2: Run all focused tests**

Run: `python -m pytest tests/test_mfl_campaign_finalizer.py -q`

Expected: all tests pass.

- [ ] **Step 3: Confirm reducer recovery remains dispatchable**

Check that `.github/workflows/mfl_reduce_saved_chunk.yml` remains present and unchanged in its required inputs and proof gates.

- [ ] **Step 4: Inspect the final diff and working tree**

Run: `git diff --check` and `git status --short`.

Expected: no whitespace errors; only intended workflow, test, and plan/spec files changed.

