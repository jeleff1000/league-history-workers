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
create a cache or lineage. The latest is the MFL player-outcome readback
[31508788009](https://github.com/jeleff1000/league-history-workers/actions/runs/31508788009),
receipt SHA-256
`4c7608c16ccd8cfc35390d5d3f1980ad02f955b221522451433caedabf5f5f24`.

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
- `partial_schema_blocked_cache_updates`: source evidence has strict null-cell
  repairs for existing canonical player rows, but also has source fields that
  the fixed cache schema does not yet contain. Apply the supported fields;
  retain the absent-schema fields as an explicit remaining decision.

## Current completion state

| Status | Artifacts | Meaning |
| --- | ---: | --- |
| `cache_verified` | 297 | 13,714 source cells match the canonical cache; no missing/conflicting cells. |
| `no_promotable_candidate_emitted` | 4,522 | Candidate-classified artifact, but no promotable cell was emitted by the frozen comparison. |
| `not_data_bearing` | 3,927 | Aggregate/validation/metadata artifact; not a canonical-cell source. |
| `candidate_provenance_not_found` | 709 | 5,504,005 declared candidate rows require direct source-file comparison. |
| `unmatched_cache_key` | 9 | 1,306 candidate rows / 2,612 cells require team-key-to-player-row reconciliation. |
| `partial_schema_blocked_cache_updates` | 15 | MFL player-outcome artifacts have 82,466 supported cache-null cells and 288,160 loss/tie source cells outside the current schema. |

The receipt workflow is read-only. It asserts, before and after the audit,
that player schema, player-row count, and the ops-cache hash are unchanged;
it has no cache-save, cache-delete, or lineage-creation step.

## MFL player-outcome readback

The 15 `mfl-player-outcome-classification-*` artifacts contain 1,359,202
player-indexed source records (1,301,859 distinct exact player keys). All
1,359,202 source records match an existing canonical player row on
`(db_name, year, week, NFL_player_id, manager)`: this family cannot create a
missing roster/player row, but it can repair its existing outcome attributes.

| Evidence | Cells |
| --- | ---: |
| Source cells inspected | 751,675 |
| Already equal in cache | 379,062 |
| Supported cache-null repairs | 82,466 |
| Existing non-null conflicts, preserved | 1,987 |
| Unmatched canonical keys | 0 |
| Source loss/tie cells outside current schema | 288,160 |

The 15 CSV rows are now marked `partial_schema_blocked_cache_updates`, with
the required next action recorded as: apply supported null-cell updates, then
separately adjudicate the loss/tie schema fields. No cache cell was changed by
this receipt.

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
| `mfl-player-outcome-classification-*` | 15 | 1,359,202 source records | Apply its 82,466 strict null-cell repairs; separately adjudicate loss/tie fields and 1,987 conflicts. |
