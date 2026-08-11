# Research matchup artifact cache ledger

This ledger records the disposition of every one of the 9,479 research-matchup
artifacts inventoried by Actions run 31448116124, against the one approved
canonical cache:

`research-public-lake-v4-Linux-20260806-championship-v2-final-playoff-anchor-outcomes-31147899613`

The authoritative detailed rows are in
[`research_matchup_artifact_cache_ledger.csv`](research_matchup_artifact_cache_ledger.csv).
They were generated from the immutable, read-only cache-receipt Action run
[31500865474](https://github.com/jeleff1000/league-history-workers/actions/runs/31500865474).
The source receipt SHA-256 is
`df8a21d802928f3f0215bff49a272ff7168acb0a6412cc181fd94bd51cf8dbbd`.

Direct source readbacks append their evidence to the same CSV; they do not
create a cache or lineage. The latest is the read-only MFL manager-week fanout
candidate build [31512049632](https://github.com/jeleff1000/league-history-workers/actions/runs/31512049632),
receipt SHA-256
`c157f3544dacab02e6fc6d8078141e97df009c7d6c4355b0bb5809814925f36d`.

## Receipt rules

Each row has an explicit `final_status` and `next_action`.

- `cache_verified`: the candidate cells were read back from the restored
  approved cache with zero missing or conflicting cells.
- `no_promotable_candidate_emitted`: the frozen extraction/comparison ledger
  found no cell to promote. This is closed unless a new source artifact is
  discovered.
- `not_data_bearing`: the artifact is aggregate evidence, metadata, or
  validation rather than a source of canonical cells.
- `candidate_provenance_not_found`: the artifact declared candidate cells, but
  the supplied comparison deltas did not retain its original source path. Its
  listed source file must be extracted and compared directly before closure.
- `unmatched_cache_key`: the artifact has candidate cells, but its source team
  key has no matching canonical player row. Reconcile the identity or record a
  source-only closure; never fabricate a player-row update.
- `candidate_built_pending_canonical_promotion`: a read-only build produced
  exact canonical row IDs and strict null-cell updates, but no cache mutation
  occurred. It is not complete until an approved canonical-cache promotion and
  exact cache readback both pass.

## Current completion state

| Status | Artifacts | Meaning |
| --- | ---: | --- |
| `cache_verified` | 297 | 13,714 source cells match the canonical cache; no missing/conflicting cells. |
| `no_promotable_candidate_emitted` | 4,522 | Candidate-classified artifact, but no promotable cell was emitted by the frozen comparison. |
| `not_data_bearing` | 3,927 | Aggregate/validation/metadata artifact; not a canonical-cell source. |
| `candidate_provenance_not_found` | 709 | 5,504,005 declared candidate rows require direct source-file comparison. |
| `unmatched_cache_key` | 9 | 1,306 candidate rows / 2,612 cells require team-key-to-player-row reconciliation. |
| `candidate_built_pending_canonical_promotion` | 15 | MFL team-week evidence yielded 50,298 safe canonical player-row candidates; they are not yet in the cache. |

The receipt workflow is read-only. It asserts, before and after the audit,
that player schema, player-row count, and the ops-cache hash are unchanged;
it has no cache-save, cache-delete, or lineage-creation step.

## Canonical promotion contract

The approved GitHub Actions cache is immutable: a runner can restore and
modify its workspace copy, but cannot update that existing cache object in
place. The repository currently contains two distinct promotion paths:

| Workflow | Behavior | Status under the one-cache rule |
| --- | --- | --- |
| `research_promote_mfl_sidecars.yml` | Deletes the canonical cache key, then saves a replacement under the same key. | Not authorized: deletion is destructive, even though the key text is unchanged. |
| `research_artifact_candidate_audit.yml` | Applies a validated delta only in the runner workspace, validates it, then explicitly refuses cache replacement. | Safe evidence path; it does not persist a cache update. |

Therefore `candidate_built_pending_canonical_promotion` has one exact meaning:
the artifact has passed the row/field safety gate, but persisting it requires
an approved replacement transaction for the single canonical cache. It does
not mean the candidate was applied, and it does not authorize a second
lineage, a schema change, an ops-cache change, or a cache deletion.

## MFL manager-week outcome candidate

The 15 `mfl-player-outcome-classification-*` artifacts contain 1,359,202
source records. The source is a **manager-week team signal**, not a player
snapshot: target rows with a null player ID made a player-key join ambiguous.
The correct fanout is `(db_name, year, week, manager)` to all matching existing
canonical player rows.

| Evidence | Cells |
| --- | ---: |
| Unique MFL team-week source groups | 75,673 |
| Source groups with no manager, quarantined | 7,559 |
| Non-null-manager source groups with no canonical match | 0 |
| Safe player-row candidates | 50,298 |
| Win conflicts preserved | 2,361 |
| Team-points conflicts preserved | 739 |
| Candidate rows with null NFL player ID | 858 |

The 15 CSV rows are now marked `candidate_built_pending_canonical_promotion`.
No cache cell was changed by this receipt. These artifacts do **not** provide
missing player records; they provide outcome cells for player records already
in the canonical table. The next action is a canonical promotion that updates
only these 50,298 candidate cells, followed by a cache readback.

## Open source-comparison worklist

The 733 unclosed artifacts are finite and are grouped below. These are not new
API pulls: each needs the already-retained candidate artifact compared to the
approved cache and then either promoted as a strict improvement or closed with
its source-specific reason.

| Family | Artifacts | Candidate rows | Required evidence |
| --- | ---: | ---: | --- |
| `research-sparse-playoff-*` | 621 | 732,640 | Team-week source file -> canonical team-week fan-out. |
| `sleeper-missing-outcome-*` | 56 | 35,410 | Player source file -> canonical full player key. |
| `promotable-rescue-delta-*` | 18 | 330,425 | `promotable_delta.parquet` -> canonical full player key. |
| `research-source-matchup-rescue-*` | 14 | 4,405,530 | Team-week source file -> canonical team-week fan-out. |
| `research-championship-identity-probe-*` | 9 | 1,306 | Reconcile source team key to existing canonical player rows, or explicitly close source-only. |
| `mfl-player-outcome-classification-*` | 15 | 50,298 canonical candidate rows | Promote the strict manager-week fanout candidates, then read back the cache. |
