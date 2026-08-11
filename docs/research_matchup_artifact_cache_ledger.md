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

## Current completion state

| Status | Artifacts | Meaning |
| --- | ---: | --- |
| `cache_verified` | 297 | 13,714 source cells match the canonical cache; no missing/conflicting cells. |
| `no_promotable_candidate_emitted` | 4,537 | Candidate-classified artifact, but no promotable cell was emitted by the frozen comparison. |
| `not_data_bearing` | 3,927 | Aggregate/validation/metadata artifact; not a canonical-cell source. |
| `candidate_provenance_not_found` | 709 | 5,504,005 declared candidate rows require direct source-file comparison. |
| `unmatched_cache_key` | 9 | 1,306 candidate rows / 2,612 cells require team-key-to-player-row reconciliation. |

The receipt workflow is read-only. It asserts, before and after the audit,
that player schema, player-row count, and the ops-cache hash are unchanged;
it has no cache-save, cache-delete, or lineage-creation step.

## Open source-comparison worklist

The 718 unclosed artifacts are finite and are grouped below. These are not new
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
