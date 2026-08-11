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
The latest corrected raw-team identity receipt is
[31532904897](https://github.com/jeleff1000/league-history-workers/actions/runs/31532904897).
It is read-only and uses the safe team-to-player fan-out hierarchy: direct
team identity, MFL franchise identity, manager-plus-team-name, then a manager
only when that source manager has exactly one team in that league-week.
The latest exact unique-manager fan-out receipt is
[31533448048](https://github.com/jeleff1000/league-history-workers/actions/runs/31533448048).
It re-read all 56 retained `sleeper-missing-outcome-rescue-*` artifacts against
the same cache without mutation. It safely fanned 190,317 source manager-weeks
to existing player rows; no supported cache-null cell remained to fill.
The latest canonical-player snapshot receipt is
[31545187959](https://github.com/jeleff1000/league-history-workers/actions/runs/31545187959).
It read 11 previously-unresolved snapshots through the actual unique player
key `(db_name, year, week, NFL_player_id)`, with manager and platform as
consistency guards: all 3,441,112 non-null source cells matched the approved
cache, with zero missing, conflicting, or ambiguous cells.

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
- `cache_conflict_preserved`: direct source-to-cache comparison completed. No
  supported cache-null cell is available to fill; existing non-null conflicts
  remain intentionally unmodified, and unmatched source identities remain
  explicitly tracked.
- `unmatched_cache_key`: the artifact has candidate cells, but its source team
  key has no matching canonical player row. Reconcile the identity or record a
  source-only closure; never fabricate a player-row update.
- `partial_schema_blocked_unmatched`: source has both fields outside the frozen
  player schema and supported fields with no exact canonical key match. It is
  not a cache update; reconcile the source identity or close it source-only.
- `source_only_team_signal_missing_player_team_bridge`: raw matchup data has a
  team outcome and the cache has player rows for the league-week, but neither
  source nor cache retains the player-to-fantasy-team roster edge. It cannot
  be fanned out without a roster/lineup bridge keyed to the player row.
- `candidate_built_pending_canonical_promotion`: a read-only build produced
  exact canonical row IDs and strict null-cell updates, but no cache mutation
  occurred. It is not complete until an approved canonical-cache promotion and
  exact cache readback both pass.

## Current completion state

| Status | Artifacts | Meaning |
| --- | ---: | --- |
| `cache_verified` | 308 | 3,454,826 source cells have direct receipts matching the canonical cache; no missing/conflicting cells. |
| `no_promotable_candidate_emitted` | 4,522 | Candidate-classified artifact, but no promotable cell was emitted by the frozen comparison. |
| `not_data_bearing` | 3,927 | Aggregate/validation/metadata artifact; not a canonical-cell source. |
| `cache_conflict_preserved` | 7 | 2,142,503 supported cells already match; 9,397 non-null conflicts are preserved; 480,208 source cells remain identity-unmatched; zero cache-null fill candidates. |
| `unmatched_cache_key` | 9 | 1,306 candidate rows / 2,612 cells require team-key-to-player-row reconciliation. |
| `partial_schema_blocked_cache_updates` | 15 | Direct MFL audit produced 31,870 safe exact-row candidates; remaining source cells include ambiguous identities, preserved conflicts, and 288,160 loss/tie values blocked by the current schema. |
| `partial_schema_blocked_unmatched` | 473 | 24,920,856 supported cells already match; 334,144 source cells still lack a resolved player-team edge; 577,482 loss/tie cells require the explicitly permitted schema fields. |
| `partial_schema_blocked_conflicts` | 214 | 32,532,612 supported cells already match; 128,484 existing non-null values conflict and are preserved; 7,044,748 source cells still lack a resolved player-team edge; 4,169,612 loss/tie cells require schema support. |
| `blocked_schema` | 4 | Sparse-playoff evidence has 20,960 already-matching supported cells and 464 required loss/tie cells. Those cells remain open until the cache schema supports loss/tie and a readback passes. |

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

## Raw team/week source receipt

The 14 retained `research-source-matchup-rescue-*` artifacts were directly
read against the approved cache in
[31532904897](https://github.com/jeleff1000/league-history-workers/actions/runs/31532904897).
The earlier [31521837322](https://github.com/jeleff1000/league-history-workers/actions/runs/31521837322)
result used only raw `team_key` and was invalid for this purpose; it must not
be used as a data-gap claim.

The corrected receipt examined 1,850,598 retained source team/week rows
(including retained duplicate snapshots across artifact generations). It
safely fanned matching source teams into existing player rows and found:

| Result across the 14 ledger artifacts | Source-backed cells |
| --- | ---: |
| Already equal to approved cache | 28,032,962 |
| Supported cache nulls to fill | 0 |
| Existing non-null conflicts, preserved | 112,542 |
| Source cells without a resolved player-team identity | 6,261,600 |
| Loss/tie cells outside the current cache schema | 3,701,196 |

Therefore these artifacts do **not** contain supported null-cell repairs to
apply. The ledger now records their real match/conflict/unmatched evidence;
it does not claim a cache update. The remaining unmatched cells require either
an independently resolvable team-to-player identity or an explicit source-only
closure, while non-null conflicts require a source-precedence decision.

## Completed sparse-playoff source comparison

All 621 candidate-bearing `research-sparse-playoff-championship-*` artifacts
were directly read against the approved cache in
[31529079339](https://github.com/jeleff1000/league-history-workers/actions/runs/31529079339)
and [31529603334](https://github.com/jeleff1000/league-history-workers/actions/runs/31529603334).
The receipts safely fanned 53,401 unique manager-week source teams into
1,266,542 canonical player rows. They found **zero supported null cells** to
apply: every supported source cell was either already equal to cache truth,
conflicted with an existing non-null cache value, or belonged to a source team
for which no canonical player row exists. This family is no longer an
unread-artifact work item.

## Remaining worklist

Only 11 artifacts still lack a usable canonical-field source-to-cache receipt. The rest have a
cache result and need either a strict promotion transaction, a source-identity
decision, or an explicit schema/precedence closure. None requires a new API
pull.

| Family | Artifacts | Current state | Required action |
| --- | ---: | ---: | --- |
| `sleeper-missing-outcome-*` | 56 | Direct manager-week fan-out receipt complete: 24,382,006 supported cells equal cache; zero supported null fills; 90 non-null conflicts; 171,648 source cells identity-unmatched; 466,458 loss/tie cells outside the prior schema. | Resolve/close residual source-only identities and adjudicate conflicts; no null-cell promotion for this family. |
| `promotable-rescue-delta-*` team-week subset | 7 | Direct fan-out receipt complete: 2,142,503 supported cells equal cache; zero supported null fills; 9,397 preserved conflicts; 480,208 identity-unmatched cells. | Resolve/close residual source-only identities and adjudicate conflicts; no null-cell promotion for this subset. |
| `promotable-rescue-delta-*` player-row subset | 11 | The files are full player-row snapshots with canonical field names; the generic `source_*` receipt correctly emitted no comparison rather than guessing. | Run the exact canonical-field snapshot comparator on the full player key. |
| `mfl-player-outcome-classification-*` | 15 | 31,870 strict exact-row candidates; 82,466 supported null cells in the direct audit. | Perform the approved single-cache promotion transaction, then exact readback. |
| `research-source-matchup-rescue-*` | 14 | Corrected fan-out receipt: 28,032,962 cells already equal cache; zero supported null fills; remaining cells are conflicts, schema-blocked loss/tie, or unresolved team identity. | Adjudicate non-null conflicts; resolve/close residual identity gaps; do not run a null-cell promotion for this family. |
| `research-championship-identity-probe-*` | 9 | 1,306 candidate rows with unresolved team identity. | Reconcile to player rows or explicitly close source-only. |
| `research-sparse-playoff-championship-*` | 621 candidate-bearing | Fully receipted; zero supported null-cell fills. | Close source-only/conflict rows under the explicit precedence and loss/tie schema decisions. |
