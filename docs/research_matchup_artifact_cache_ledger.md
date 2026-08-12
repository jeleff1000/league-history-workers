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

The CSV now has an explicit `record_type` on every row. The frozen inventory
is exactly 9,479 `artifact_inventory` rows; cache-changing work is recorded as
a separate `cache_recovery_receipt` row in the *same* ledger, not smuggled into
the 9,479-artifact denominator. There is currently one such recovery receipt:
the independently restored 74-row MFL repair documented below. The ledger
validator fails if a row lacks this distinction.

## Current approved-cache contract

The pre-recovery cache profile is the read-only key-profile receipt
[31609240997](https://github.com/jeleff1000/league-history-workers/actions/runs/31609240997).
It restored the approved cache, made no write, and confirmed the same cache
key before and after the audit. The 74-row recovery later changed the player
row count, so its count is historical context only; every future mutation must
take a fresh before-count from the currently restored approved cache. Its
identity facts remain relevant:

| Fact | Verified value | Consequence |
| --- | ---: | --- |
| Canonical player rows | 269,197,734 | Pre-recovery row-count baseline; do not reuse as a current mutation baseline. |
| MFL player rows | 13,158,043 | The MFL bridge must cover this actual cache population, not an older snapshot. |
| MFL rows with canonical `mfl_player_id` | 0 | A direct native-MFL-ID cache join is prohibited: it cannot resolve any row. |
| MFL rows with `NFL_player_id` | 12,803,086 | The only available MFL player bridge terminus is the protected NFL-player identity. |
| Populated `team_key` / `team_name` player rows | 0 / 0 | Neither can be used as a cache-side fantasy-team join key. |

The cache is no longer the historical base-29-column snapshot: it also has
the user-approved propagated cohort dimensions. Those columns are part of the
current fixed cache schema and are not a separate lineage. They do not provide
MFL franchise or native-player identity.

Consequently, every MFL promotion must first produce a read-only bridge
receipt with the following proof chain:

`weeklyResults.franchise + weeklyResults.player.id`
`-> MFL players.espn_id -> protected ops ESPN-to-NFL crosswalk`
`-> exactly one canonical recipient, or an explicit unresolved exception.`

The bridge receipt must also prove the source franchise maps to the recipient's
canonical fantasy team. It may not infer that edge from a copied target
`manager` field, a manager-name similarity, or an empty cache team field.
Raw MFL records without an ESPN ID are separate explicit cases: deterministic
DST team/year resolution or a one-to-one protected player-bio proof is
required; otherwise they remain source-only.

The read-only receipt implementation is
`scripts/research_cohorts/build_mfl_roster_bridge_receipt.py`, added in commit
`6ae77ce`. Its input contract requires `source_manager_origin` to be either
`mfl_franchise_owner_name` or `mfl_franchise_name`; a copied target-inventory
manager is rejected before it can become a bridge. The implementation test
also rejects two raw franchises for one manager, a non-unique MFL-to-NFL
crosswalk, and duplicate canonical recipients. This is an implementation
completion, **not** an artifact or cache completion: no MFL bridge receipt has
yet been run against the target population, so the ledger's 711 open artifact
statuses remain unchanged.

The source-side predecessor is
`scripts/research_cohorts/extract_mfl_target_roster_memberships.py`. It fetches
only a flagged league-week and writes raw roster membership plus its MFL player
directory; it records every attempted source-ID/year pair, including the
historical seed ID encoded in the database name. Its companion
`build_mfl_roster_player_crosswalk.py` resolves only exact one-to-one
ESPN-to-NFL IDs from the protected ops player bio. Neither program opens the
canonical player cache for writing. A future Action must first run these
read-only source receipts, then run the bridge receipt, and only then create a
strict cache-update candidate.

The first 15-target runtime pilot, [31613290294](https://github.com/jeleff1000/league-history-workers/actions/runs/31613290294),
is recorded as **invalid bridge evidence**, not as a closure. It restored the
approved cache and passed its no-mutation gate, and it successfully returned
4,732 raw MFL roster memberships from all 15 source weeks. However, the
extractor called the MFL client directory method with its `(year, league_id)`
arguments reversed. All 15 directory calls were therefore invalid and the
crosswalk had 451 `missing_espn_id` keys / zero resolved recipients. Commit
`03eb5da` fixes only that argument order and adds its regression test.

The corrected re-run, [31613754399](https://github.com/jeleff1000/league-history-workers/actions/runs/31613754399),
is valid **source and identity evidence**, but is still not a cache promotion:
all 15 source groups resolved, producing 4,732 memberships and 4,732 player
directory rows. The protected crosswalk resolved 429 of 451 MFL player IDs;
1,785 memberships reached an exact canonical player-team recipient. The
remaining 2,676 `no_canonical_recipient` memberships were not yet split into
"canonical player-week absent" versus "same player under a different canonical
manager," so this run cannot close or promote any open artifact.

The two-way re-run, [31614575246](https://github.com/jeleff1000/league-history-workers/actions/runs/31614575246),
completed successfully against commit `c13d45f`, with cache schema, canonical
row count, and ops SHA-256 unchanged before versus after. It establishes the
following bridge partition for the same 15 source groups:

| Receipt outcome | Memberships | Meaning |
| --- | ---: | --- |
| Exact canonical player-team recipient | 1,785 | Valid player-team bridge evidence. This is not a promotion receipt yet. |
| Same canonical player-week, different manager | 2,602 | The player identity exists. The unresolved edge is MFL franchise to canonical manager/team identity. |
| Canonical player-week absent | 74 | No corresponding canonical player-week exists for the mapped player. This cannot be a cell update. |
| Missing player crosswalk | 271 | 20 MFL IDs have no ESPN identity and two ESPN IDs are absent from ops; repeated memberships produce the total. |

This receipt proves the next task is a franchise-to-canonical-manager resolver,
not another broad MFL pull. The resolver must derive a one-to-one edge from
raw franchise identity and existing league-week evidence, then emit only exact
canonical player-row recipients. It must leave the 74 absent player-weeks and
all non-unique franchise-manager cases explicitly unresolved. No artifact
status is closed or promoted by this bridge diagnostic alone.

The 20 no-ESPN MFL IDs are not unresolved NFL players: the source directory
identifies all of them as `Coach` records (for example Bill Belichick, Pete
Carroll, and Mike Tomlin). They have no canonical NFL-player recipient and
must be closed as `source_non_player_coach`, not treated as a player-map gap.
The two real player-bio gaps are source ESPN IDs `14946` (Bruce Irvin) and
`13213` (LeGarrette Blount). They require a protected player-bio identity
repair, verified by an exact player/year/team match, before they can enter any
MFL recipient bridge.

The retained diagnostic receipt from
[31616814643](https://github.com/jeleff1000/league-history-workers/actions/runs/31616814643)
also resolved the prior generic `canonical_player_week_absent: 74` count. All
74 are real NFL-player memberships in `smpl_mfl_2015_10030`, 2012, that are
absent from the canonical player table for the listed league-weeks:

| MFL player | NFL player ID | Missing weeks | Memberships |
| --- | --- | --- | ---: |
| Michael Vick | `00-0020245` | 1–15 | 15 |
| Stevie Johnson | `00-0026364` | 1–15 | 15 |
| Steve Smith (CAR) | `00-0020337` | 1–15 | 15 |
| Steve Smith (STL) | `00-0025438` | 1–15 | 15 |
| Beanie Wells | `00-0027007` | 1–8 | 8 |
| Christopher Ivory | `00-0027531` | 10–15 | 6 |

This is a historical canonical **player-row omission** for that MFL league,
not a bye, no-opponent week, coach record, or unresolved source-player
identity. The source membership and protected NFL-player mapping both exist,
but the canonical cache has zero player rows for each exact league/week/player
key. These 74 have no existing cell to update; recovery requires an approved
row-addition path with the same schema and an exact source-to-player receipt.

Existing `mfl-underpopulated-week-rescue-*` artifacts are retained source-team
signals, not roster bridges. A direct schema receipt from
`mfl-underpopulated-week-rescue-17-31217899167` shows team/franchise IDs,
manager identity, outcomes, playoff/championship flags, and points—but no MFL
player ID or per-player roster membership. They remain valuable as the source
of team outcomes after a bridge is proved; they cannot by themselves decide
which player rows receive those outcomes. The raw target roster membership is
therefore the one missing source edge, not a request to re-fetch an entire
league-season population.

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

The latest championship-identity probe receipt is
[31550932175](https://github.com/jeleff1000/league-history-workers/actions/runs/31550932175),
receipt SHA-256
`bc9976c85815971c9eef243a0910575cfff3f9b81d2acee50103d3c1f63e056c`.
It is read-only and passed the player-schema, player-row-count, and ops-hash
gates. It corrected the old false assumption that a raw team key is required:
seven MFL probes fan out through a unique normalized manager-week, while two
probes have no cache-side manager bridge and remain source-only.

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
| `cache_verified` | 312 | 3,476,250 source cells have direct receipts matching the canonical cache; no missing/conflicting cells. This includes the four loss/tie source artifacts read back in run `31633809547`. |
| `no_promotable_candidate_emitted` | 4,529 | Candidate-classified artifact, but no promotable cell was emitted by the frozen comparison. |
| `not_data_bearing` | 3,927 | Aggregate/validation/metadata artifact; not a canonical-cell source. |
| `cache_conflict_preserved` | 7 | 2,142,503 supported cells already match; 9,397 non-null conflicts are preserved; 480,208 source cells remain identity-unmatched; zero cache-null fill candidates. |
| `source_only_team_signal_missing_player_team_bridge` | 2 | 151 source team-weeks overlap cache league-weeks but have no safe manager/team bridge to player rows; they cannot be fanned out. |
| `partial_schema_blocked_direct_identity` | 15 | Legacy pre-loss/tie receipt: 31,870 safe exact-row candidates had prior cache evidence. Its loss/tie portion must now be re-audited against the approved cache before any remaining gap is asserted. |
| `partial_schema_blocked_unmatched` | 473 | Legacy pre-loss/tie receipt: the player-team identity gap remains a real unresolved category, while its recorded loss/tie schema count requires fresh comparison against the current cache. |
| `partial_schema_blocked_conflicts` | 214 | Legacy pre-loss/tie receipt: non-null conflicts remain preserved pending precedence policy, while its recorded loss/tie schema count requires fresh comparison against the current cache. |

The receipt workflow is read-only. It asserts, before and after the audit,
that player schema, player-row count, and the ops-cache hash are unchanged;
it has no cache-save, cache-delete, or lineage-creation step.

## Cache-completion matrix

The 9,479 artifacts are now partitioned without double-counting by their
evidence state:

| Completion state | Artifacts | Meaning |
| --- | ---: | --- |
| Closed by direct cache evidence | 312 | Every compared canonical cell equals the approved cache. |
| Closed: no promotable cache cell emitted | 4,529 | The frozen extraction/comparison emitted no canonical-cell candidate. |
| Closed: non-data-bearing | 3,927 | Aggregate, validation, inventory, or metadata evidence; not a source of canonical cells. |
| **Open: supported null-cell promotion** | **0** | No safe, unpromoted null-cell candidate is currently recorded in the ledger. |
| **Legacy loss/tie gate requiring re-audit** | **702** | These artifacts were classified before the approved cache gained canonical `loss`/`tie`. They must be read back again against the current cache; the old schema gate is not proof of a current gap. |
| **Open: player-team identity bridge or source-only closure** | **711** | Source has team-week or direct-player evidence that cannot yet be connected safely to a single eligible player row, or must be explicitly closed as source-only. |
| **Open: source-precedence adjudication** | **233** | Source differs from an existing non-null cache value; no overwrite may occur without a field-level precedence rule. |

The final three open categories overlap. The 702 legacy loss/tie gate rows are
being re-audited against the current schema before any remaining schema gap is
asserted. The artifact
partition is still exact: **8,768 closed, 711 open**.

### Machine-checkable cache-admission gates

The CSV carries four derived columns for every artifact:
`loss_tie_gate`, `player_team_bridge_gate`, `source_precedence_gate`, and
`cache_admission_state`. They are derived only from the existing quantified
receipt fields; they do not assert a cache write. The ledger validator rejects
an open artifact if any gate is missing or inconsistent with its recorded
schema-blocked, unmatched, or conflict cells.

| Admission state | Artifacts | Required work before any cache write |
| --- | ---: | --- |
| `open_loss_tie` | 0 | The four schema-only artifacts were promoted and independently read back in `31633809547`. |
| `open_player_team_bridge` | 2 | Produce raw roster-membership evidence or an explicit source-only closure. |
| `open_loss_tie_and_player_team_bridge` | 476 | Legacy gate values; re-audit against the current loss/tie schema before treating this as an active schema blocker. |
| `open_player_team_bridge_and_source_precedence` | 7 | Complete bridge proof and field-level conflict adjudication. |
| `open_all_required_gates` | 226 | Legacy schema component requires exact re-audit; bridge and precedence evidence still require adjudication. |
| `closed_*` | 8,768 | Closed by the final status shown above; no cache update is implied unless the status is `closed_cache_verified`. |

The only state that proves a cache value is present is `closed_cache_verified`
with its restored-cache receipt. `closed_no_promotable_candidate_emitted` and
`closed_not_data_bearing` mean, respectively, that no safe candidate existed or
that the artifact was never a source of canonical cells.

The cell totals in this section are receipt totals, not a count of distinct
cache cells: retained artifacts can contain duplicate source snapshots. A
promotion run must deduplicate on its approved field-level key before writing
anything.

### Exact open-work groups

| Obligations on the artifact | Artifacts | Evidence requiring action |
| --- | ---: | --- |
| Loss/tie schema only | 4 | 464 loss/tie cells; all 20,960 supported cells already match. |
| Remaining direct-source schema/identity work | 15 | 75,078 exact supported cells are promoted and cache-verified. A fresh current-cache audit found zero safe null candidates; 1,532 null cells require direct identity resolution, 1,201 cells conflict, and 288,160 loss/tie cells require schema support. |
| Loss/tie schema + player-team bridge | 473 | 577,482 loss/tie cells and 334,144 unsupported player-team identities. |
| Loss/tie schema + precedence + player-team bridge | 214 | 4,169,612 loss/tie cells, 128,484 conflicts, and 7,044,748 unresolved identities. |
| Precedence + player-team bridge | 7 | 9,397 conflicts and 480,208 unresolved identities. |
| Player-team bridge only | 2 | 151 supported playoff cells have no canonical player-team match. |

### Identity evidence already attached

The CSV now carries existing read-only profile evidence in
`identity_profile_run_id`, `source_team_keys`,
`matched_source_team_keys`, `unmatched_source_team_keys`,
`league_week_overlap_keys`, `league_week_absent_keys`, and
`identity_bridge_status`. This is evidence from prior receipts, not a new
fetch or a cache mutation.

| Identity state among the 711 open artifacts | Artifacts | Meaning |
| --- | ---: | --- |
| `fully_matched` | 11 | All source teams fan out to player rows; their remaining work is limited to the recorded schema or source-precedence evidence. |
| `player_key_profiled` | 15 | Direct MFL player-key evidence; the strict null-cell candidate is promoted and cache-verified, while the remaining nulls are identity-ambiguous. |
| `partial_bridge` | 559 | Some source teams fan out safely; the remaining teams need a bridge. |
| `partial_bridge_with_source_only` | 135 | A partial bridge exists and some source league-weeks have no player rows to receive data. |
| `no_player_team_bridge` | 2 | The two remaining championship probes overlap cache league-weeks but lack a usable player-to-team bridge. |

### What the unresolved bridge actually lacks

The bridge counters below are retained-receipt counters, so repeated artifact
generations can describe the same source team-week more than once. They are
evidence of coverage and failure mode, not a distinct league-week census. A
future bridge candidate must deduplicate by its source team-week key before it
can write a single canonical player cell.

| Bridge state | Source team-weeks in receipts | Already matched to canonical player rows | Still lacking a player-team edge | No canonical player league-week exists |
| --- | ---: | ---: | ---: | ---: |
| `fully_matched` | 232 | 232 | 0 | 0 |
| `partial_bridge` | 393,851 | 230,218 | 163,633 | 0 |
| `partial_bridge_with_source_only` | 2,121,073 | 320,375 | 1,800,698 | 15,918 |
| `no_player_team_bridge` | 151 | 0 | 151 | 0 |

The 15,918 source-only league-weeks have no canonical player row that could
receive a player outcome, playoff, or championship value. They require an
explicit source-only closure after a direct player-population check; they are
not candidate cache updates. The remaining 1,964,482 retained unmatched
source-team observations overlap a canonical league-week but need a
deterministic roster-membership edge before they can fan out to players.

The canonical cache does not currently retain populated `team_key` or
`team_name` values. A bridge therefore cannot rely on raw team identifiers.
It must prove membership through an exact player key, a unique normalized
manager-week, or a platform roster-player identity mapped to an exact canonical
player key. If none exists, the ledger must retain `ambiguous` or `source_only`,
never infer a recipient.

## Required gates before any further promotion

No artifact is complete merely because it has a source value. Every future
cache update must satisfy these three independent gates and retain an
artifact-level receipt for each one:

| Gate | Required proof | Prohibited shortcut |
| --- | --- | --- |
| `loss_tie_schema` | The fixed canonical schema contains only the permitted `loss` and `tie` fields, with types and pre/post schema hashes recorded. | BF/fallback fields, source-only columns, or a second cache key. |
| `player_team_bridge` | Raw source roster membership maps the platform player ID through its protected crosswalk to exactly one canonical `(db_name, year, week, NFL_player_id)` recipient. | Manager-name similarity, a target-inventory manager, or an artifact derived from the cache. |
| `source_precedence` | A field-level rule plus exact old/new values and source provenance authorizes any non-null overwrite. | A blanket source-wins rule or an implicit overwrite. |

After a write, an independent restored-cache readback must prove the exact
changed canonical cells, unchanged row count, unchanged non-target fields, the
same approved cache key, and the same ops-cache hash. Until that receipt
exists, a candidate is open regardless of workflow success.

### Retained player-to-team bridge evidence

The completed artifact receipt
[`31448116124`](https://github.com/jeleff1000/league-history-workers/actions/runs/31448116124)
initially identified 526 non-empty files whose *schemas* contained player and
team-like fields. A follow-up provenance review excluded 508 of them: 507
`promotable_delta.parquet` files and one `outcome_targets_*.parquet` file are
derived from the canonical target/cache plus team signals, so they cannot
independently prove player-to-team membership. The remaining 18 artifacts
have only `(db_name, year, week, NFL_player_id, manager)`; none retains a raw,
populated player-to-team key.

| Candidate class | Artifacts | Required source fields | Meaning |
| --- | ---: | --- | --- |
| Independent raw player/team membership evidence | 0 | — | No retained artifact currently proves a player belonged to a particular source fantasy team. |
| `candidate_player_manager_only` | 18 | `db_name`, `year`, `week`, `NFL_player_id`, `manager` | Insufficient alone. It may be used only after a complete source league-week proves the manager owned exactly one team. |

The CSV columns `bridge_evidence_file_count`, `bridge_evidence_row_count`,
`bridge_evidence_strength`, `bridge_evidence_schemas`, and
`bridge_evidence_receipt_run_id` record this per artifact. The ledger
validator fails if a row claims bridge evidence without positive row coverage,
a candidate classification, and its source receipt. It does not call any
candidate a completed bridge; that requires a separate cache-side join receipt.

### Read-only team-signal bridge pilot

The 15-artifact, exact-download pilot
[`31566023441`](https://github.com/jeleff1000/league-history-workers/actions/runs/31566023441)
tested the real team-to-player fan-out against the approved canonical cache.
It restored the immutable cache read-only, downloaded only the selected source
artifacts, and passed its cache-schema-row-count and ops-hash gates.

| Measure | Result |
| --- | ---: |
| Source team-week rows | 12,838 |
| Source team identities | 12,403 |
| Deterministically resolved team identities | 5,515 |
| Canonical player rows reached by those identities | 117,906 |
| Supported source cells already equal to cache | 354,135 |
| Supported cache-null cells | 0 |
| Supported non-null conflicts | 0 |
| Source cells blocked only by missing `loss`/`tie` schema | 25,676 |
| Supported source cells without a resolved canonical recipient | 22,001 |

This establishes two separate closure paths. Matched cells from this pilot do
not need promotion because the cache already contains the same supported value.
The remaining cells require either the approved `loss`/`tie` schema addition or
a deterministic roster-membership bridge; they cannot be inferred from a
league-week overlap alone. Each of the 15 selected CSV ledger rows now cites
this receipt and carries both obligations where applicable.

### Loss/tie admission matrix

Schema support alone will not close every `loss`/`tie` artifact. The ledger
currently identifies 713 source artifacts with either `loss`/`tie` evidence or
schema-blocked cells. These are retained-receipt counts, not distinct cache
cells; duplicate source snapshots are deduplicated only during an eventual
field-level promotion.

| Admission condition | Artifacts | Retained loss/tie cells | Additional obligation |
| --- | ---: | ---: | --- |
| Schema only | 4 | 464 | Add `loss`/`tie`, then exact null-fill/readback. |
| Direct MFL player evidence | 15 | 288,160 | Resolve 1,532 ambiguous identities and preserve/adjudicate 1,201 non-null conflicts. |
| Schema plus identity gap, conflict, or both | 694 | 4,747,094 | Require a deterministic player-team bridge and, where present, source-precedence adjudication. |

Across this source family the ledger retains 7,860,632 unmatched identity
cells and 139,082 non-null conflicts. Therefore a successful schema migration
must not mark the source family complete by itself: it can close only the four
schema-only artifacts and any later candidates with an exact proven recipient.

### Non-null conflict inventory

There are 233 artifacts with a source value that differs from an existing
non-null cache value. The ledger guard requires every one to name
`precedence` in `next_action`; no current workflow is allowed to overwrite
these values implicitly.

| Evidence family | Artifacts | Conflict cells | Source fields represented | Required decision |
| --- | ---: | ---: | --- | --- |
| Team-week signal evidence | 221 | 137,881 | `win`, `loss`, `tie`, `team_points`, `is_playoffs`, `champion` | Compare exact platform provenance at the proven player-team recipient; preserve same-tier disagreement. |
| Direct MFL player evidence | 12 | 1,201 | `win`, `team_points`, `is_playoffs` | Direct player-week source may outrank a derived cache value only after its identity is unique; otherwise preserve. |

Championship precedence remains especially strict: a season-level champion
marker never outranks or creates championship-game credit. Only an explicit
championship-week marker with a champion result is eligible evidence.

## Canonical promotion contract

The approved GitHub Actions cache is immutable: a runner can restore and
modify its workspace copy, but cannot update that existing cache object in
place. The repository currently contains two distinct promotion paths:

| Workflow | Behavior | Status under the one-cache rule |
| --- | --- | --- |
| `research_promote_mfl_sidecars.yml` | Legacy broad replacement workflow. | Not authorized for this ledger: it lacks the exact structured candidate and independent restored-cache receipt. |
| `research_promote_structured_player_null_fill.yml` | Restores the approved cache, requires exact player identity and cache-null cells, applies only `win`, `team_points`, and `is_playoffs`, then saves only under the same key. | Used successfully in [31559895461](https://github.com/jeleff1000/league-history-workers/actions/runs/31559895461); followed by independent restored-cache verification. |
| `research_artifact_candidate_audit.yml` | Applies a validated delta only in the runner workspace, validates it, then explicitly refuses cache replacement. | Safe evidence path; it does not persist a cache update. |

Therefore `candidate_built_pending_canonical_promotion` has one exact meaning:
the artifact has passed the row/field safety gate, but persisting it requires
an approved replacement transaction for the single canonical cache. It does
not mean the candidate was applied, and it does not authorize a second
lineage, a schema change, an ops-cache change, or a cache deletion.

## MFL player-outcome evidence

The 15 `mfl-player-outcome-classification-*` artifacts contain 1,359,202
player-indexed outcome records. They are **not** raw roster-membership
evidence: their `manager` value is carried from the target inventory and does
not independently prove that a particular source fantasy team rostered the
named player. Their audit key is `(db_name, year, week, NFL_player_id,
manager)`, but that key can validate an already-built comparison only; it
cannot authorize a new team-to-player fan-out. The direct source readback is
[31515802840](https://github.com/jeleff1000/league-history-workers/actions/runs/31515802840).
It passed cache-schema, player-row-count, ops-hash, and no-new-lineage gates.

| Evidence | Cells |
| --- | ---: |
| Source player rows | 1,359,202 |
| Distinct exact player keys | 1,301,859 |
| Source player rows with an exact canonical match | 1,359,202 |
| Source player rows absent from cache | 0 |
| Matching source cells already in cache | 441,708 |
| Safe supported null cells still missing | 0 |
| Null cells blocked by ambiguous canonical identity | 1,532 |
| Existing non-null conflicts preserved | 1,201 |
| Loss/tie cells blocked by current schema | 288,160 |

The former 50,298-row manager-week fanout candidate was incomplete: it was a
safe lower bound, not the full direct player-key comparison. The ledger keeps
all 15 artifacts `partial_schema_blocked_direct_identity` because loss/tie,
identity, and preserved-conflict work remains. The direct candidate build
[31518641442](https://github.com/jeleff1000/league-history-workers/actions/runs/31518641442)
produced 31,870 unambiguous canonical player-row candidates: 21,604 with
win/playoff values and all 31,870 with team points. Its report SHA-256 is
`a8c9436b559cd1bba7baef092672a7b0178bcc0559032e11066b18d2084444b1`.
Those 75,078 cells were promoted in
[31559895461](https://github.com/jeleff1000/league-history-workers/actions/runs/31559895461)
and independently restored/read back with zero remaining candidate cells in
[31560162399](https://github.com/jeleff1000/league-history-workers/actions/runs/31560162399).

The current-cache re-audit
[31562866791](https://github.com/jeleff1000/league-history-workers/actions/runs/31562866791)
then re-read all 15 source artifacts without mutating the cache. It found zero
safe current null candidates. The 1,532 remaining null cells are all attached
to source identities that map to multiple canonical rows; they cannot be
promoted until a raw roster/player identity bridge resolves their recipient.

The remaining direct-source identities are intentionally not folded into that
candidate: 55,180 source keys map to more than one canonical row. Of those,
43,669 have only a null manager, 6,743 only a null NFL player ID, 4,691 have
both null, and only 77 are fully specified duplicate canonical identities.
The next action is to add the explicitly allowed loss/tie schema, then resolve
identity only where an independently auditable roster/player edge exists. For
MFL, that means `weeklyResults.player.id -> players.espn_id -> protected ops
ESPN-to-NFL map -> (db_name, year, week, NFL_player_id)`. All other ambiguous
identities remain unpromoted. Non-null disagreements remain preserved until
source precedence is decided.

## Historical base-player join contract

The approved-cache read-only audit
[31547677629](https://github.com/jeleff1000/league-history-workers/actions/runs/31547677629)
verified the base-player join behavior and the unchanged cache/ops seed for
that receipt. The current row count, schema, and MFL-native-ID availability
are governed by the later receipt 31609240997 above. This older receipt remains
valid only for the key behavior it measured.

| Cache-side candidate | Evidence | Contract decision |
| --- | ---: | --- |
| `(db_name, year, week, NFL_player_id)` | 313,086 duplicate key groups | Never a universal player-row key. |
| `(db_name, year, week, NFL_player_id, normalized manager)` | 44,322,065 joinable rows; 11,731 duplicate groups | Use only when the particular source key resolves to exactly one canonical player row. |
| `team_key` | 0 populated player rows | Not a cache-side join key. |
| `team_name` | 0 populated player rows | Not a cache-side join key. |
| `(db_name, year, week, normalized manager)` | 1,689,546 manager-weeks fanning to 44,707,248 player rows | The only supported team-signal-to-player fan-out. |

The artifact receipts agree. Across the 733 profiled raw team-signal artifacts,
551,987 source team-weeks resolved only through the unique normalized
manager-week fan-out. Zero resolved through raw `team_key`, MFL
manager/franchise GUID, or manager-plus-team-name. The CSV ledger stores this
per artifact in `cache_join_strategy` and the three matched-strategy count
columns. Its current open partition is 698 `manager_week_fanout`, 15
`strict_player_key`, and two `unresolved_team_identity` artifacts.

Therefore no future receipt or promotion may claim a `team_key`,
`team_name`, MFL GUID, or manager-plus-team-name cache join unless a new
read-only audit proves that the corresponding cache column is populated and
the source-to-cache resolution is one-to-many safe. Manager-week fan-out must
exclude placeholder manager values and require exactly one source team for
that normalized manager in that league-week.

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

## Championship-identity probe adjudication

The stronger nine-artifact, read-only receipt
[31552757341](https://github.com/jeleff1000/league-history-workers/actions/runs/31552757341)
supersedes the earlier provisional statement that each MFL probe contained a
promotable player cell. It preserved the approved cache, schema, row count,
and ops seed unchanged.

| Evidence | Result |
| --- | ---: |
| Source rows across all nine probe artifacts | 1,313 |
| Duplicate source rows collapsed | 996 |
| Distinct source team-weeks | 317 |
| MFL manager-week signals safely fanned to existing cache rows | 166 team-weeks → 3,327 rows |
| Valid strict NULL fills on real NFL player rows | 0 |
| Valid strict NULL fills overall | 1 `is_playoffs` cell on a row with `NFL_player_id = NULL` |
| Champion fills | 0 |
| Source-only team-weeks with no cache-side identity bridge | 151 |

The seven MFL files use the confirmed unique normalized manager-week fan-out.
Their apparent champion candidates are season-level champion flags and cannot
be interpreted as championship-game starts. The one surviving strict-null cell
is not an NFL player row, so it cannot affect the research player tables. The
two non-MFL probes have no manager/team identity in the cache and remain open
only as explicitly source-only evidence; no cache value was invented.

## Exact MFL missing-player-row recovery — closed

The source-bridge receipt from `mfl-roster-bridge-pilot-31616814643` identified
74 real MFL player-week rows absent from the approved cache. The recovery was
applied and independently read back in
[run 31624673691](https://github.com/jeleff1000/league-history-workers/actions/runs/31624673691)
without a schema change, a second cache key, or an ops-cache change.

| Gate | Verified result |
| --- | ---: |
| Exact source rows inserted | 74 |
| Independently restored cache rows present | 74 / 74 |
| Structurally complete rows | 74 / 74 |
| Source-confirmed starts | 43 |
| Starts with source score and player-week LAMAR | 43 / 43 |
| Cache objects under approved exact key | 1 |

The durable readback receipt SHA-256 is
`ae0917ae66f45ac2e66661e8505b4d515829d71603fe80cab6026dccccf35c50`.
It is the one `cache_recovery_receipt` record in the ledger; it does not alter
the frozen 9,479-artifact partition.

## Remaining worklist

Every artifact now has a direct cache disposition. The remaining open work is
a source-identity decision or an explicit schema/precedence closure. None
currently has a safe, unpromoted null-cell candidate and none requires a new
API pull.

## Exact team-to-player loss/tie promotion — proven, but not over-claimed

Source artifact `8952555354` (`research-source-matchup-rescue-3-31062083630`)
was re-read through its safe unique-manager-week fan-out, then promoted in
[run 31639212646](https://github.com/jeleff1000/league-history-workers/actions/runs/31639212646).
The update filled exactly 52,667 existing player-row `loss` cells and 52,667
existing `tie` cells. It inserted no player rows; all fields other than loss
and tie had zero eligible null fills; the player schema, player-row count, and
ops-cache SHA-256 were unchanged.

The independent fresh restore in that same run proved all 52,667 target rows
matched and that zero source-backed loss/tie nulls remained. The one approved
cache key remains a single object. The follow-up read-only receipt
[31639682101](https://github.com/jeleff1000/league-history-workers/actions/runs/31639682101)
then found zero remaining safe fills for this artifact.

This artifact is deliberately still **open** in the CSV as
`unmatched_cache_key`, rather than being falsely marked complete: 3,606 source
team-week identities have no canonical player recipient. Its loss/tie schema
gate is now closed; its only remaining gate is the documented player-team
bridge or a source-only closure.

The CSV is the machine-readable authority: **8,768 of 9,479 artifacts are
closed** and **711 remain open**. The open partition is 473 identity-unmatched,
214 precedence conflicts, 15 legacy schema-blocked safe-null repairs, 7
preserved-conflict artifacts, and 2 source-only player-identity gaps. Every one of the 711 has a
non-empty reason and next action in the CSV; an artifact is never considered
closed merely because its parent workflow completed.

| Family | Artifacts | Current state | Required action |
| --- | ---: | ---: | --- |
| `sleeper-missing-outcome-*` | 56 | Direct manager-week fan-out receipt complete: 24,382,006 supported cells equal cache; zero supported null fills; 90 non-null conflicts; 171,648 source cells identity-unmatched; 466,458 loss/tie cells outside the prior schema. | Resolve/close residual source-only identities and adjudicate conflicts; no null-cell promotion for this family. |
| `promotable-rescue-delta-*` team-week subset | 7 | Direct fan-out receipt complete: 2,142,503 supported cells equal cache; zero supported null fills; 9,397 preserved conflicts; 480,208 identity-unmatched cells. | Resolve/close residual source-only identities and adjudicate conflicts; no null-cell promotion for this subset. |
| `promotable-rescue-delta-*` player-row subset | 11 | The files are full player-row snapshots with canonical field names; the generic `source_*` receipt correctly emitted no comparison rather than guessing. | Run the exact canonical-field snapshot comparator on the full player key. |
| `mfl-player-outcome-classification-*` | 15 | 31,870 strict exact-row candidates / 75,078 cells promoted and independently read back. Current re-audit: zero safe null candidates; 1,532 identity-ambiguous null cells and 1,201 preserved conflicts remain. | Add loss/tie schema; resolve only deterministic direct identity; preserve conflicts and ambiguous identities. |
| `research-source-matchup-rescue-*` | 14 | Corrected fan-out receipt: 28,032,962 cells already equal cache; zero supported null fills; remaining cells are conflicts, schema-blocked loss/tie, or unresolved team identity. | Adjudicate non-null conflicts; resolve/close residual identity gaps; do not run a null-cell promotion for this family. |
| `research-championship-identity-probe-*` | 9 | Fully adjudicated by receipt 31552757341: seven MFL files have a safe manager-week fan-out but zero valid player-row fills; two files remain source-only because no cache-side identity bridge exists. | Do not promote championship or playoff values from this family; retain the two source-only identity gaps as explicitly unresolved. |
| `research-sparse-playoff-championship-*` | 621 candidate-bearing | Fully receipted; zero supported null-cell fills. | Close source-only/conflict rows under the explicit precedence and loss/tie schema decisions. |
