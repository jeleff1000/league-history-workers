# Canonical Snapshot Receipt Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Read the eleven canonical player snapshots against the approved cache, determine the unique cache join key from observed cardinality, and replace only their stale ledger evidence.

**Architecture:** Add a dedicated `canonical_player_snapshot` source kind to the existing immutable receipt workflow. Its reader uses the observed source key `(db_name, year, week, NFL_player_id)` and accepts it only when it maps to exactly one canonical player row; source `manager` and `platform` are consistency assertions. It compares existing canonical fields, emits a per-artifact receipt, and updates the same 9,479-row ledger.

**Tech Stack:** Python 3.12, DuckDB, PyArrow/Parquet, pytest, GitHub Actions cache restore.

## Global Constraints

- Cache key remains `research-public-lake-v4-Linux-20260806-championship-v2-final-playoff-anchor-outcomes-31147899613`.
- The cache opens read-only; no save, delete, rename, or replacement operation is allowed.
- No player-schema, row-count, or ops-cache mutation is permitted.
- Compare only `win`, `team_points`, `is_playoffs`, `champion`, `has_po_signal`, `final_playoff_seed`, `made_playoffs`, and `clutch_equity`.
- Source rows must be uniquely matched; ambiguous source or canonical identity is unmatched, never fanned out.
- The ledger must remain exactly 9,479 rows and only the eleven selected artifacts may have their receipt counters replaced.

---

### Task 1: Add a fail-closed canonical-snapshot receipt reader

**Files:**
- Modify: `scripts/research_cohorts/finalize_artifact_cache_receipts.py`
- Test: `tests/research_cohorts/test_finalize_artifact_cache_receipts.py`

**Interfaces:**
- Consumes: `canonical_snapshot_manifest: Path | None`, JSON entries with `artifact_id` and Parquet `path`.
- Produces: `build_receipts(..., canonical_snapshot_manifest=...)` counters per artifact.

- [ ] **Step 1: Write the failing tests**

```python
def test_receipt_reads_canonical_snapshot_by_unique_player_week_key(tmp_path):
    # Source has null team_key/team_name, one source player row, and a cache
    # row with the same db/year/week/NFL_player_id but populated team identity.
    # Assert source_cells=4, equal=3, missing=1, unmatched=0.

def test_receipt_keeps_ambiguous_canonical_snapshot_key_unmatched(tmp_path):
    # Two cache rows share db/year/week/NFL_player_id.
    # Assert no candidate is emitted and source cells are unmatched.

def test_receipt_ignores_snapshot_source_only_columns(tmp_path):
    # Include arbitrary source-only column and assert schema/counters are unchanged.
```

- [ ] **Step 2: Run the tests to verify RED**

Run:
`pytest tests/research_cohorts/test_finalize_artifact_cache_receipts.py -k canonical_snapshot -v`

Expected: FAIL because `canonical_snapshot_manifest` is not an accepted `build_receipts` argument.

- [ ] **Step 3: Implement the smallest reader**

```python
CANONICAL_SNAPSHOT_FIELDS = {
    "win": "win", "team_points": "team_points", "is_playoffs": "is_playoffs",
    "champion": "champion", "has_po_signal": "has_po_signal",
    "final_playoff_seed": "final_playoff_seed", "made_playoffs": "made_playoffs",
    "clutch_equity": "clutch_equity",
}
SNAPSHOT_KEY = ["db_name", "year", "week", "NFL_player_id"]
```

For each source identity, resolve it only if exactly one canonical player row
has the four-field key.  Require source `manager` and `platform` to equal that
canonical row when non-null.  Add equality/null/conflict counts for non-null
source cells.  Add all zero-or-many canonical matches to unmatched counts.

- [ ] **Step 4: Run targeted and full receipt tests**

Run:
`pytest tests/research_cohorts/test_finalize_artifact_cache_receipts.py -v`

Expected: PASS.

- [ ] **Step 5: Commit the reader and tests**

```bash
git add scripts/research_cohorts/finalize_artifact_cache_receipts.py tests/research_cohorts/test_finalize_artifact_cache_receipts.py
git commit -m "feat: read canonical player snapshots into receipts"
```

### Task 2: Route the eleven snapshots through the immutable workflow

**Files:**
- Modify: `.github/workflows/research_readback_team_signal_artifacts.yml`
- Modify: `scripts/research_cohorts/select_retained_artifacts.py`
- Test: `tests/research_cohorts/test_finalize_artifact_cache_receipts.py`

**Interfaces:**
- Consumes: workflow input `source_kind=canonical_player_snapshot` and the exact eleven artifact IDs.
- Produces: `final_artifact_cache_receipts.json` with receipt coverage for all selected IDs.

- [ ] **Step 1: Write the failing selector test**

```python
def test_selector_accepts_canonical_player_snapshot_artifacts(tmp_path):
    # The eleven full snapshot rows are retained when source kind is canonical_player_snapshot.
```

- [ ] **Step 2: Run the selector test to verify RED**

Run:
`pytest tests/research_cohorts/test_select_retained_artifacts.py -k canonical_player_snapshot -v`

Expected: FAIL because the source kind is unsupported.

- [ ] **Step 3: Add the dedicated source kind**

Add `canonical_player_snapshot` to the workflow input choices and selector.
The manifest builder must require exactly one `promotable_delta.parquet` per
selected artifact.  The workflow must pass it as `--canonical-snapshot-manifest`.
Keep the existing pre/post schema, row-count, ops-SHA, and no-mutation gates
unchanged.

- [ ] **Step 4: Run selector and workflow YAML validation tests**

Run:
`pytest tests/research_cohorts/test_select_retained_artifacts.py tests/research_cohorts/test_finalize_artifact_cache_receipts.py -v`

Expected: PASS.

- [ ] **Step 5: Commit routing change**

```bash
git add .github/workflows/research_readback_team_signal_artifacts.yml scripts/research_cohorts/select_retained_artifacts.py tests/research_cohorts/
git commit -m "feat: route canonical snapshots through receipt audit"
```

### Task 3: Execute immutable receipt run and update the ledger

**Files:**
- Modify: `docs/research_matchup_artifact_cache_ledger.csv`
- Modify: `docs/research_matchup_artifact_cache_ledger.md`

**Interfaces:**
- Consumes: successful workflow artifact containing the exact eleven receipt rows.
- Produces: revised ledger with one final direct-readback disposition per artifact.

- [ ] **Step 1: Dispatch only the eleven artifact IDs**

Run the receipt workflow with the approved key, `download_mode=exact`, and
`source_kind=canonical_player_snapshot`.  Do not dispatch any cache-promotion workflow.

- [ ] **Step 2: Verify runner safeguards before ledger update**

Require all of these receipt assertions:

```text
cache_mutated == false
new_lineage == false
pre/post player schema identical
pre/post player row count identical
pre/post ops SHA-256 identical
selected artifact count == 11
```

- [ ] **Step 3: Overlay only the eleven receipt rows**

Run `refresh_artifact_cache_ledger.py` with the new receipt and exact selected
manifest. Assert 9,479 ledger rows before and after, 11 updated rows, and no
other artifact ID changed.

- [ ] **Step 4: Verify the ledger classification**

Run a Python invariant check that every artifact has `final_status`,
`final_reason`, and `next_action`; report source/equal/null/conflict/unmatched
cells for each of the eleven entries. Any cache-null cells must remain
`candidate_built_pending_canonical_promotion`, not `cache_verified`.

- [ ] **Step 5: Commit and push ledger evidence**

```bash
git fetch origin main
git rebase origin/main
git add docs/research_matchup_artifact_cache_ledger.csv docs/research_matchup_artifact_cache_ledger.md
git commit -m "docs: receipt canonical player snapshot artifacts"
git push origin main
```

### Task 4: Completion checkpoint

**Files:**
- Verify: `docs/research_matchup_artifact_cache_ledger.csv`
- Verify: `docs/research_matchup_artifact_cache_ledger.md`

- [ ] **Step 1: Reconcile every artifact status**

Confirm 9,479 unique IDs and each row has a source receipt or an explicit
non-data/source-only/schema/precedence reason.

- [ ] **Step 2: Separate ledger completion from cache completion**

Report separately:

```text
cache verified cells
unpromoted strict null-fill candidates
preserved conflicts
unmatched source identities
schema-bound fields
```

- [ ] **Step 3: Do not promote without a separate authorization**

The receipt job provides the exact candidate count and field list.  It does
not authorize replacing the immutable canonical cache.
