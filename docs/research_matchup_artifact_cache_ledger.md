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
create a cache or lineage. The latest is the read-only direct MFL player-key
audit [31515802840](https://github.com/jeleff1000/league-history-workers/actions/runs/31515802840),
receipt SHA-256
`4c7608c16ccd8cfc35390d5d3f1980ad02f955b221522451433caedabf5f5f24`.
The latest team/week receipt is
[31521837322](https://github.com/jeleff1000/league-history-workers/actions/runs/31521837322),
receipt SHA-256
`a5e41d7373d5f52dc4525f1458cc0931e7851feacd58027f88e3f220358ca5c6`.

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
- `partial_schema_blocked_unmatched`: source has both fields outside the frozen
  player schema and supported fields with no exact canonical key match. It is
  not a cache update; reconcile the source identity or close it source-only.
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
| `candidate_provenance_not_found` | 695 | 5,504,005 declared candidate rows still require direct source-file comparison. |
| `unmatched_cache_key` | 9 | 1,306 candidate rows / 2,612 cells require team-key-to-player-row reconciliation. |
| `partial_schema_blocked_cache_updates` | 15 | Direct MFL audit produced 31,870 safe exact-row candidates; remaining source cells include ambiguous identities, preserved conflicts, and 288,160 loss/tie values blocked by the current schema. |
| `partial_schema_blocked_unmatched` | 14 | Raw team/week source artifacts were read against the canonical cache and have zero exact team-key matches; 7,402,392 supported cells cannot be safely fanned out. |

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

## MFL player-outcome evidence

The 15 `mfl-player-outcome-classification-*` artifacts contain 1,359,202
source records. Although the source does not contain a complete roster/player
snapshot, it is **player-indexed outcome evidence**: its correct audit key is
`(db_name, year, week, NFL_player_id, manager)`. The direct source readback is
[31515802840](https://github.com/jeleff1000/league-history-workers/actions/runs/31515802840).
It passed cache-schema, player-row-count, ops-hash, and no-new-lineage gates.

| Evidence | Cells |
| --- | ---: |
| Source player rows | 1,359,202 |
| Distinct exact player keys | 1,301,859 |
| Source player rows with an exact canonical match | 1,359,202 |
| Source player rows absent from cache | 0 |
| Matching source cells already in cache | 379,062 |
| Supported null cells still missing | 82,466 |
| Existing non-null conflicts preserved | 1,987 |
| Loss/tie cells blocked by current schema | 288,160 |

The former 50,298-row manager-week fanout candidate was incomplete: it was a
safe lower bound, not the full direct player-key comparison. The ledger now
marks all 15 artifacts `partial_schema_blocked_cache_updates`. No cache cell
was changed by this receipt. The direct candidate build
[31518641442](https://github.com/jeleff1000/league-history-workers/actions/runs/31518641442)
produced 31,870 unambiguous canonical player-row candidates: 21,604 with
win/playoff values and all 31,870 with team points. Its report SHA-256 is
`a8c9436b559cd1bba7baef092672a7b0178bcc0559032e11066b18d2084444b1`.

The remaining direct-source identities are intentionally not folded into that
candidate: 55,180 source keys map to more than one canonical row. Of those,
43,669 have only a null manager, 6,743 only a null NFL player ID, 4,691 have
both null, and only 77 are fully specified duplicate canonical identities.
The next action is to promote the 31,870 exact-key candidates, then reconcile
the 77 fully specified duplicates and apply the manager-week evidence only
where its team identity is independently unambiguous. Loss/tie remain a
separate frozen-schema decision.

## Raw team/week source mismatch

The 14 retained `research-source-matchup-rescue-*` artifacts were directly
read against the approved cache in
[31521837322](https://github.com/jeleff1000/league-history-workers/actions/runs/31521837322).
The read-only run passed the player-schema, player-row-count, ops-hash, and
no-new-lineage gates.

It examined 1,850,598 retained source team/week rows (including retained
duplicates across artifact generations). Every one of their 1,850,598 exact
`(db_name, year, week, team_key)` identities had **zero** matching canonical
player team/week keys. Consequently, the artifacts provide no safe fan-out to
current player rows: zero source cells matched or filled cache cells. The
ledger records all 14 as `partial_schema_blocked_unmatched`, not as applied or
cache-verified. Their only valid next action is exact team-identity
reconciliation, followed by another receipt, or an explicit source-only
closure.

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
| `research-source-matchup-rescue-*` | 14 | 4,405,530 | Exact team-key receipt completed: zero canonical matches; reconcile identity or close source-only. |
| `research-championship-identity-probe-*` | 9 | 1,306 | Reconcile source team key to existing canonical player rows, or explicitly close source-only. |
| `mfl-player-outcome-classification-*` | 15 | 50,298 canonical candidate rows | Promote the strict manager-week fanout candidates, then read back the cache. |
