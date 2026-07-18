# Yahoo Corpus Runner Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build and launch two GitHub Actions pilots that ingest era-stratified Yahoo league-years into the existing four-table research corpus while rotating OAuth grants and preventing private identity leakage.

**Architecture:** `league-history-workers` contains the planner, scheduler, driver, validation, and workflow. At runtime it checks out the private `yahoo_oauth` pipeline, reads credentials from Fly, discovers eligible Yahoo league-years without persisting tokens, runs one-year offline imports, and folds validated outputs with the existing corpus snapshot contract.

**Tech Stack:** Python 3.11, pytest, DuckDB, PyArrow, Yahoo OAuth/API, Fly DuckDB read API, GitHub Actions.

## Global Constraints

- GitHub Actions execute only in `league-history-workers`; the `yahoo_oauth` workflow is a non-operational synchronized mirror.
- Never upload OAuth contexts, raw league directories, manager names, team names, GUIDs, access tokens, or refresh tokens.
- Never write corpus data to Fly; the import subprocess must remove all Fly credentials and run with `CORPUS_MODE=1`, `--skip-track-1`, and `--skip-track-2-upload`.
- The plan unit is one completed Yahoo league-year from 2001 through 2025.
- Never dispatch the same grant consecutively when another ready grant exists; one grant may have at most one task in flight.
- Pilot A targets twelve league-years across the four specified eras and distinct grants; Pilot B targets three widely separated years from each of four grants and verifies resume behavior.
- The folded output must use the canonical `league_settings`, `draft`, `transactions`, and `player_fantasy` contract from `scripts/sleeper_corpus/build_corpus_snapshot.py`.

---

### Task 1: Deterministic Candidate Selection and Credential Scheduler

**Files:**
- Create: `scripts/yahoo_corpus/__init__.py`
- Create: `scripts/yahoo_corpus/scheduler.py`
- Create: `tests/yahoo_corpus/test_scheduler.py`

**Interfaces:**
- Produces: `Candidate`, `DispatchState`, `select_cross_era(candidates, limit)`, `select_spacing(candidates, grants, years_per_grant)`, and `CredentialScheduler.next(now)`.
- Consumes: candidate rows from Task 2; emits ordered tasks to Task 3.

- [ ] **Step 1: Write failing scheduler tests**

Cover exact assertions for four-era representation, distinct-grant preference, widest per-grant year spread, no adjacent grant repeats, least-recently-used ordering, cooldown skipping, 429 requeue, and resume reconstruction from serialized state.

```python
def test_scheduler_never_repeats_grant_when_alternative_ready():
    scheduler = CredentialScheduler([
        Candidate("a1", "g1", "1.l.1", 2005, "10t_flx_std_4pt"),
        Candidate("a2", "g1", "2.l.1", 2020, "10t_flx_std_4pt"),
        Candidate("b1", "g2", "3.l.1", 2012, "12t_flx_half_4pt"),
    ])
    assert [scheduler.next(0).grant_id, scheduler.next(1).grant_id] == ["g1", "g2"]
```

- [ ] **Step 2: Run tests and confirm RED**

Run: `python -m pytest tests/yahoo_corpus/test_scheduler.py -q`

Expected: collection fails because `scripts.yahoo_corpus.scheduler` does not exist.

- [ ] **Step 3: Implement the scheduler**

Use immutable candidate records and a JSON-safe state. `next(now)` filters completed/in-flight/cooling candidates, excludes `last_grant_id` when another grant is ready, chooses the least-recently-used grant, then chooses the candidate maximizing `abs(year - last_year_by_grant[grant])`. `complete`, `fail`, and `rate_limit` update state; `rate_limit` records a cooldown and returns the task to pending.

- [ ] **Step 4: Run scheduler tests and confirm GREEN**

Run: `python -m pytest tests/yahoo_corpus/test_scheduler.py -q`

Expected: all scheduler tests pass.

- [ ] **Step 5: Commit**

```bash
git add scripts/yahoo_corpus tests/yahoo_corpus/test_scheduler.py
git commit -m "feat(yahoo-corpus): add credential-aware scheduler"
```

### Task 2: Private Runtime Inventory and Era-Stratified Planning

**Files:**
- Create: `scripts/yahoo_corpus/inventory.py`
- Create: `scripts/yahoo_corpus/planner.py`
- Create: `tests/yahoo_corpus/test_inventory.py`
- Create: `tests/yahoo_corpus/test_planner.py`

**Interfaces:**
- Produces: `load_grants(reader, encryption_keys) -> list[Grant]`, `discover_candidates(grants, client_factory, checkpoint) -> list[Candidate]`, and `build_plan(mode, candidates, limit) -> Plan`.
- Consumes: private pipeline modules `multi_league.utils.credential_store`, `scripts.yahoo_corpus.client`, and `scripts.yahoo_corpus.settings_parser` from the checked-out `yahoo_oauth` tree.

- [ ] **Step 1: Write failing inventory and planner tests**

Use fake Fly rows, decryptors, and Yahoo clients. Assert plaintext-token deduplication, opaque stable grant IDs, renewal-chain traversal, completed-season filtering, round-robin discovery between grants, sanitized failure classes, documented era fallback, and a plan JSON containing no token/name fields.

- [ ] **Step 2: Run tests and confirm RED**

Run: `python -m pytest tests/yahoo_corpus/test_inventory.py tests/yahoo_corpus/test_planner.py -q`

Expected: imports fail because inventory and planner modules do not exist.

- [ ] **Step 3: Implement inventory discovery**

Query `___ops.main.league_credentials`, decrypt in memory, deduplicate plaintext refresh tokens, and assign grant IDs using a SHA-256 digest prefix without serializing the token. Rotate one discovery operation per grant per round. Use known credential anchor league keys first, follow `renew`/`renewed` settings links, and call user game/league discovery only until the selected pilot can satisfy its era/grant requirements. Persist only opaque checkpoint state and candidate metadata.

- [ ] **Step 4: Implement plan generation and redaction**

`cross-era` uses `select_cross_era`; `spacing-resume` uses `select_spacing`. The serialized plan contains `task_id`, `grant_id`, `league_key`, `season`, `cohort_slug`, and era. The private in-memory grant map remains separate and is never written by the artifact writer.

- [ ] **Step 5: Run inventory/planner tests and confirm GREEN**

Run: `python -m pytest tests/yahoo_corpus/test_inventory.py tests/yahoo_corpus/test_planner.py -q`

Expected: all tests pass.

- [ ] **Step 6: Commit**

```bash
git add scripts/yahoo_corpus/inventory.py scripts/yahoo_corpus/planner.py tests/yahoo_corpus
git commit -m "feat(yahoo-corpus): plan private cross-era inventory"
```

### Task 3: One-Year Import Driver, Validation, Folding, and Redacted Reports

**Files:**
- Create: `scripts/yahoo_corpus/runner.py`
- Create: `scripts/yahoo_corpus/validation.py`
- Create: `tests/yahoo_corpus/test_runner.py`
- Create: `tests/yahoo_corpus/test_validation.py`
- Modify: `scripts/sleeper_corpus/build_corpus_snapshot.py`

**Interfaces:**
- Produces: `build_context(task, grant, data_dir)`, `build_subprocess_env(base_env)`, `validate_source(db_path, task)`, `run_plan(plan, private_grants, output, ...)`, and redacted `ledger.json`/`report.json`.
- Consumes: Task 1 scheduler, Task 2 plan, private `initial_import_v3.py`, and generalized `fold_database(con, db_path, db_name)`.

- [ ] **Step 1: Write failing driver and validation tests**

Assert a one-year context has exactly one league key, full import mode, and ephemeral OAuth credentials; subprocess environment removes every Fly write/read/admin secret while retaining offline dependency paths; validation rejects missing tables, cohort mismatch, empty rostered population, and zero/multiple champions; report serialization rejects forbidden keys and token-like values.

- [ ] **Step 2: Run tests and confirm RED**

Run: `python -m pytest tests/yahoo_corpus/test_runner.py tests/yahoo_corpus/test_validation.py -q`

Expected: imports fail because runner and validation modules do not exist.

- [ ] **Step 3: Generalize the corpus folder**

Extract the path-specific body of `fold_league` into `fold_database(con, db_path, db_name)`. Keep `fold_league` as a compatibility wrapper so the Sleeper workflow and tests retain their existing behavior.

- [ ] **Step 4: Implement one-year execution**

Write the OAuth context only beneath the task's temporary directory. Invoke:

```text
python code/fantasy_football_data_scripts/initial_import_v3.py
  --context <ephemeral-context>
  --data-dir <ephemeral-task-dir>
  --skip-track-1
  --skip-track-2-upload
```

Capture stdout/stderr to a private temporary log, classify failures from sanitized patterns, print only stage/task/error-class summaries, validate the local DuckDB, fold it, update the scheduler ledger atomically, and remove the raw directory/context in a `finally` block.

- [ ] **Step 5: Implement pilot checkpoint/resume and reports**

For `spacing-resume`, `--stop-after 4` exits successfully after four tasks while preserving scheduler state; `--resume` reloads it. `report.json` groups opaque outcomes by era/year/cohort/stage and includes the dispatch trace needed to verify rotation.

- [ ] **Step 6: Run all driver tests and confirm GREEN**

Run: `python -m pytest tests/yahoo_corpus -q`

Expected: all Yahoo corpus unit tests pass.

- [ ] **Step 7: Commit**

```bash
git add scripts/yahoo_corpus scripts/sleeper_corpus/build_corpus_snapshot.py tests/yahoo_corpus
git commit -m "feat(yahoo-corpus): run and fold one-year imports"
```

### Task 4: GitHub Actions Workflow and Private-Repository Mirror

**Files:**
- Create: `.github/workflows/yahoo_corpus_pilot.yml`
- Create mirror: `D:/yahoo_oauth/.worktrees/yahoo-settings-census-spec/.github/workflows/yahoo_corpus_pilot.yml`
- Create: `tests/yahoo_corpus/test_workflow_contract.py`

**Interfaces:**
- Produces: manually dispatchable `Yahoo Corpus Pilot` workflow with `cross-era`, `spacing-resume`, and `plan-only` modes.
- Consumes: worker scripts from Tasks 1-3 and existing repository secrets/cache keys.

- [ ] **Step 1: Write failing workflow contract tests**

Parse the YAML and assert checkout of both repositories, worker-owned script paths, `main` default private ref, required Yahoo/Fly-read/decryption secrets, no Fly admin/write token, task limit 12, worker count 1, dependency-cache restore, always-uploaded allowlisted artifacts, and no raw/context glob.

- [ ] **Step 2: Run workflow test and confirm RED**

Run: `python -m pytest tests/yahoo_corpus/test_workflow_contract.py -q`

Expected: fails because the workflow does not exist.

- [ ] **Step 3: Implement the worker workflow**

Checkout this repository under `worker` and private pipeline under `code`; install private requirements; restore `corpus-dependencies-v1-Linux-20260717`; expose only `DATABASE_SERVER_URL`, `DATABASE_READ_TOKEN`, `CREDENTIAL_ENCRYPTION_KEY`, `YAHOO_CLIENT_ID`, and `YAHOO_CLIENT_SECRET` to the planning step; run the driver; validate privacy/schema; upload only slice, redacted ledger/report, and sanitized log with `if: always()`.

- [ ] **Step 4: Mirror the workflow definition**

Copy the identical YAML into the clean `yahoo_oauth` main worktree to satisfy synchronized-workflow policy. Add a top comment that `league-history-workers` is the only execution owner. Do not add worker scripts to `yahoo_oauth`.

- [ ] **Step 5: Run workflow tests and YAML validation**

Run: `python -m pytest tests/yahoo_corpus/test_workflow_contract.py -q`

Run: `python -c "import pathlib,yaml; yaml.safe_load(pathlib.Path('.github/workflows/yahoo_corpus_pilot.yml').read_text())"`

Expected: both exit zero.

- [ ] **Step 6: Commit both repositories**

```bash
git add .github/workflows/yahoo_corpus_pilot.yml tests/yahoo_corpus/test_workflow_contract.py
git commit -m "ci: add Yahoo corpus pilot workflow"
```

Commit the mirrored workflow separately in the clean private-repository worktree.

### Task 5: Full Verification, Push, and Cross-Era Pilot Launch

**Files:**
- Modify only if verification reveals a defect in files from Tasks 1-4.

**Interfaces:**
- Produces: pushed worker `main` and a live `cross-era` GitHub Actions run.

- [ ] **Step 1: Run fresh local verification**

Run: `python -m pytest tests/yahoo_corpus -q`

Run: `python -m compileall -q scripts/yahoo_corpus scripts/sleeper_corpus/build_corpus_snapshot.py`

Run: `git diff --check && git status --short`

Expected: tests and compile pass, diff check is clean, and only intended committed changes exist.

- [ ] **Step 2: Push both synchronized `main` branches**

Push `league-history-workers/main` first, then the mirrored private workflow commit. Confirm both remote heads contain the workflow.

- [ ] **Step 3: Dispatch Pilot A**

```bash
gh workflow run yahoo_corpus_pilot.yml --repo jeleff1000/league-history-workers \
  -f mode=cross-era -f task_limit=12 -f workers=1 -f yahoo_oauth_ref=main
```

- [ ] **Step 4: Monitor Pilot A to a terminal result**

Use `gh run watch` and inspect failed-step logs. Fix pipeline defects, repeat local verification, commit/push, and rerun until the workflow produces a valid artifact or an external Yahoo/secret/cache blocker is proven.

### Task 6: Spacing/Resume Pilot and Final Corpus Readability Report

**Files:**
- Modify only if Pilot A or B reveals a reproducible runner defect.

**Interfaces:**
- Produces: a live `spacing-resume` run, downloaded/verified artifacts, and final pilot results.

- [ ] **Step 1: Dispatch Pilot B**

```bash
gh workflow run yahoo_corpus_pilot.yml --repo jeleff1000/league-history-workers \
  -f mode=spacing-resume -f task_limit=12 -f workers=1 -f yahoo_oauth_ref=main
```

- [ ] **Step 2: Monitor and verify resume invariants**

Confirm the workflow's controlled checkpoint resumes with a different grant from the last pre-checkpoint dispatch when another grant is ready. Confirm no grant appears in adjacent dispatch-trace rows.

- [ ] **Step 3: Download and independently inspect artifacts**

Verify the compact DuckDB has all four tables, `_sources` matches successful tasks, completed seasons have validated champion outcomes, and JSON/log artifacts contain no forbidden fields or token patterns.

- [ ] **Step 4: Report scale readiness**

Summarize successes/failures by era, year, cohort, and stage; identify historical Yahoo headaches; state whether the runner is ready for a larger batch and what concurrency/global-delay settings the pilots support.
