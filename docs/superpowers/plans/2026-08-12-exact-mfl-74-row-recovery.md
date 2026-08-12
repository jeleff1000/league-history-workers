# Exact MFL 74-Row Recovery Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use `superpowers:executing-plans` to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Restore the 74 source-proven missing MFL player-week rows to the one approved research cache and prove their presence by a fresh cache restore/readback.

**Architecture:** Use the retained source receipt for the exact 74 keys in `smpl_mfl_2015_10030` (2012, weeks 1-15) plus direct MFL weekly results and the protected ESPN-to-NFL crosswalk to create an exact-current-schema row-add sidecar. A transactional in-place apply writes only those absent keys to the restored working cache. The workflow validates the source, full schema equality, operations-cache hash, row-count delta of exactly 74, and a separate restored-cache readback before recording the ledger receipt.

**Tech Stack:** Python 3.12, DuckDB 1.5.1, Parquet, GitHub Actions cache API, MFL public export API.

## Global Constraints

- One approved cache key only: `research-public-lake-v4-Linux-20260806-championship-v2-final-playoff-anchor-outcomes-31147899613`.
- No new cache key or cache lineage.
- No canonical schema change; the candidate Parquet schema must equal `DESCRIBE public.player_fantasy` byte-for-byte in column order.
- The ops cache must be byte-identical before and after the apply.
- Add only rows whose exact `(db_name, year, week, NFL_player_id, manager)` key is absent in the restored cache and independently source-proven.
- Do not overwrite confirmed non-null canonical values.
- The final workflow must restore the named approved cache in a separate job and read back all 74 keys before it can claim success.

---

### Task 1: Build a complete source-backed exact-row candidate

**Files:**
- Create: `D:/yahoo_oauth/scripts/research_cohorts/build_exact_mfl_missing_player_rows.py`
- Create: `D:/yahoo_oauth/tests/research_cohorts/test_build_exact_mfl_missing_player_rows.py`

**Interfaces:**
- Consumes: raw MFL memberships, MFL player directory, ESPN-to-NFL crosswalk, raw MFL team/week outcomes, current canonical cache, and current league settings.
- Produces: a Parquet file with exactly the current canonical player schema and a JSON receipt containing all 74 logical keys, source identifiers, and source-field completeness.

- [ ] **Step 1: Write a failing test** that provides one source membership whose exact cache key is absent and asserts one complete current-schema candidate row is emitted, while an existing cache key is rejected.
- [ ] **Step 2: Run the test** and verify it fails because the candidate builder does not exist.
- [ ] **Step 3: Implement the minimal builder** using exact source membership IDs, direct MFL values, and copied league-year cohort fields; fail if any required source field is absent or the candidate columns differ from the cache.
- [ ] **Step 4: Run the focused test** and confirm it passes.

### Task 2: Apply only the source-proven additions to a working cache copy

**Files:**
- Create: `D:/yahoo_oauth/scripts/research_cohorts/apply_exact_player_row_additions_in_place.py`
- Create: `D:/yahoo_oauth/tests/research_cohorts/test_apply_exact_player_row_additions_in_place.py`

**Interfaces:**
- Consumes: restored working `corpus_snapshot.duckdb`, an exact-schema candidate Parquet, and the expected candidate key count.
- Produces: a receipt with exact before/after schema, row count, inserted key count, rejected existing keys, and per-key readback.

- [ ] **Step 1: Write a failing test** for a two-row candidate containing one absent and one existing key; assert the apply aborts rather than silently updating the existing row.
- [ ] **Step 2: Run the test** and verify it fails because the in-place row-additioner does not exist.
- [ ] **Step 3: Implement a single DuckDB transaction** that rejects any existing or duplicate candidate key, inserts the exact candidate rows, validates schema unchanged and `after_rows = before_rows + candidates`, and reads each added key back before commit.
- [ ] **Step 4: Run the focused test** and confirm it passes.

### Task 3: Dispatch the same-key recovery and verify the approved cache

**Files:**
- Create: `D:/yahoo_oauth/_public_fresh/.github/workflows/research_apply_exact_mfl_74_row_recovery.yml`
- Modify: `D:/yahoo_oauth/_public_fresh/docs/research_matchup_artifact_cache_ledger.csv`
- Modify: `D:/yahoo_oauth/_public_fresh/docs/research_matchup_artifact_cache_ledger.md`

**Interfaces:**
- Restores only the approved cache key; restores the ops cache as part of the same directory.
- Downloads immutable retained diagnostic/source evidence, builds and validates the candidate, applies it in the restored working copy, performs an exact same-key replacement only after pre-publication gates, then separately restores and reads all 74 keys.

- [ ] **Step 1: Add workflow fail-fast gates** for exact schema, source key count 74, candidate key count 74, no pre-existing candidate keys, unchanged ops hash, and no unexpected row-count change.
- [ ] **Step 2: Add an independent readback job** that restores the approved key anew and proves all 74 exact keys are present with their source-backed identity fields.
- [ ] **Step 3: Update the durable ledger only from the successful readback receipt**, including source artifact IDs, candidate receipt SHA, cache key, cache ID, and 74/74 readback count.
- [ ] **Step 4: Run the workflow and inspect the artifacts**; do not claim recovery until the separate restore/readback gate passes.
