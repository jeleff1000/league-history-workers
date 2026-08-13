# Research matchup artifact cache ledger

This ledger records the disposition of every one of the 9,479 research-matchup
artifacts inventoried by Actions run 31448116124, against the one approved
canonical cache:

`research-public-lake-v4-Linux-20260806-championship-v2-final-playoff-anchor-outcomes-31147899613`

The authoritative detailed rows are in
[`research_matchup_artifact_cache_ledger.csv`](research_matchup_artifact_cache_ledger.csv).
It contains the immutable 9,479-artifact inventory from read-only cache-receipt
Action run [31500865474](https://github.com/jeleff1000/league-history-workers/actions/runs/31500865474),
plus every later artifact known to this workstream. The frozen-inventory source
receipt SHA-256 is `df8a21d802928f3f0215bff49a272ff7168acb0a6412cc181fd94bd51cf8dbbd`.

Direct source readbacks append their evidence to the same CSV; they do not
create a cache or lineage. The latest is the read-only direct MFL player-key
audit [31515802840](https://github.com/jeleff1000/league-history-workers/actions/runs/31515802840),
receipt SHA-256
`4c7608c16ccd8cfc35390d5d3f1980ad02f955b221522451433caedabf5f5f24`.

The CSV now has an explicit `record_type` on every row. The frozen inventory
is exactly 9,479 `artifact_inventory` rows; later artifacts are 231
`artifact_admission` rows; cache-changing work is recorded as a separate
`cache_recovery_receipt` row in the *same* ledger. Neither later artifacts nor
receipts are smuggled into the 9,479-artifact denominator. There are currently
11 such recovery receipts. They include the independently restored 74-row MFL
repair, direct MFL player/team bridge work, exact MFL roster-identity work, and
the two most recent loss/tie promotions described below. The ledger validator
fails if a row lacks this distinction.

The seventh receipt is the exact MFL loss/tie promotion: candidate run
[31689836532](https://github.com/jeleff1000/league-history-workers/actions/runs/31689836532),
same-key apply/readback [31690044486](https://github.com/jeleff1000/league-history-workers/actions/runs/31690044486),
and independent post-save audit [31690332564](https://github.com/jeleff1000/league-history-workers/actions/runs/31690332564).
It filled 28,085 `loss` cells and 27,988 `tie` cells on 28,085 exact existing
player rows; its post-save candidate delta is zero. Cache object `6601873724`
was saved under the same approved key, with unchanged schema, player-row count,
and ops-cache SHA-256.

The eighth receipt is the direct-source conflict replacement: read-only
candidate [31691317379](https://github.com/jeleff1000/league-history-workers/actions/runs/31691317379),
guarded same-key promotion [31692380121](https://github.com/jeleff1000/league-history-workers/actions/runs/31692380121),
and post-save audit [31692615938](https://github.com/jeleff1000/league-history-workers/actions/runs/31692615938).
It replaced 1,025 `win`, 1,267 `loss`, and 270 `team_points` values across
1,976 exact player rows. The post-save audit found zero direct-source conflicts
and zero new candidates. Cache object `6602753769` remains the sole object
under the approved key; schema, player-row count, and ops-cache SHA-256 are
unchanged.

## Current residual queue

The latest ledger validation reports 9,721 ledger rows: 9,479 frozen
raw-artifact rows, 231 later artifact-admission rows, and 11 supplemental
cache-recovery receipts. Of the frozen artifact rows, 8,786 are closed and 693
are still open. Those 693 entries are a work queue, **not** a claim that 693 artifacts still
contain unapplied values: each must be re-read against the current approved
cache before it can be closed or kept open with a current, specific reason.

| Historical raw-artifact status | Count | Required disposition |
| --- | ---: | --- |
| `partial_schema_blocked_unmatched` | 472 | Historical pre-loss/tie label. Current gate is an exact player/team bridge and readback; schema support is already closed. |
| `partial_schema_blocked_conflicts` | 214 | Historical pre-loss/tie label. Current gates are exact player/team bridge plus explicit source-precedence receipt. |
| `partial_schema_blocked_direct_identity` | 4 | Direct MFL exact-key re-audit: all other retained direct-MFL artifacts equal the cache; these four retain duplicate-recipient ambiguity. |
| `source_only_team_signal_missing_player_team_bridge` | 2 | Build a source-roster-to-existing-player bridge or retain as source-only. |
| `unmatched_cache_key` | 1 | Separate already-filled recipients from true source-only player/team records and preserve the residual count. |

The machine-derived **current** frozen-inventory admission queue is simpler
than those historical labels: 479 artifacts require a deterministic recipient
bridge or an explicit source-only closure, while 214 require field-level
source-precedence adjudication in addition to their recorded identity work. All
open rows carrying loss/tie evidence are now marked `closed_schema_supported`;
no open row may ask for a loss/tie schema migration again.

The raw source artifact `8952555354` is the last frozen-inventory row above. Its current
receipt proves 26,936 existing-player fills from 256 source candidates are in
the cache. Its older 19,560 source-only cells are deliberately still open until
the remaining team/player identities are independently resolved or explicitly
classified as having no canonical recipient.

## Latest loss/tie promotion receipts

Two further same-key promotions were made only after a read-only candidate
check, then independently restored and read back. Neither changed the cache
schema, the protected ops-cache hash, or the approved cache key.

| Receipt | Candidate type | Verified canonical fills | Readback result |
| --- | --- | ---: | --- |
| [31709912567](https://github.com/jeleff1000/league-history-workers/actions/runs/31709912567) | Exact existing player key: `(db_name, year, week, NFL_player_id)` | 141,354 loss/tie rows | Zero remaining source-backed `loss` or `tie` nulls on all 141,354 recipients. |
| [31711617686](https://github.com/jeleff1000/league-history-workers/actions/runs/31711617686) | Verified null-player-ID manager/team/week fan-out | 11,713 loss/tie rows | Zero remaining source-backed `loss` or `tie` nulls on all 11,713 recipients. |

The follow-up read-only audit
[31713108535](https://github.com/jeleff1000/league-history-workers/actions/runs/31713108535)
re-read the six contributing raw team-signal artifacts against the restored
approved cache. It found **zero** additional safe null fills for `win`, `loss`,
`tie`, `team_points`, `is_playoffs`, `champion`, `final_playoff_seed`, or
`made_playoffs`. One non-null championship disagreement remains quarantined;
it is not evidence to overwrite a canonical championship-start flag.

## Direct MFL classification re-audit

Read-only run [31716352919](https://github.com/jeleff1000/league-history-workers/actions/runs/31716352919)
compared all 15 retained MFL outcome-classification artifacts using the direct
player key `(db_name, year, week, NFL_player_id)`. It restored the approved
cache and made no write. Its 142,073 uniquely matched recipients agree on all
supported outcome fields: `win`, `loss`, `tie`, `team_points`, and
`is_playoffs`. No cache-null fill and no non-null conflict remained.

Eleven of the 15 artifacts are therefore closed by this readback. Four remain
open solely because their source player key matches multiple canonical rows;
their retained ambiguous cell counts are 40, 1,000, 1,005, and 720.

The follow-up receipt
[31720418092](https://github.com/jeleff1000/league-history-workers/actions/runs/31720418092)
identifies the precise cache-grain defect: 553 player-week keys fan out to
2,446 cache rows (2–6 rows per key), and **every one** of those rows lacks a
manager, MFL player ID, team key/name, and lineup slot. The source rows also
lack a usable manager identity for these keys. This is neither a source-fetch
gap nor a safe upsert: the affected cache rows must first be reconstructed from
the retained MFL roster memberships before outcomes can be attached. A
manager-name fallback or arbitrary overwrite remains forbidden.

The latest bridge-negative receipt,
[31694545464](https://github.com/jeleff1000/league-history-workers/actions/runs/31694545464),
tested team-points identity for the 15 direct MFL artifacts after their exact
source replacement. Across 16,613 manager-null source player-weeks with a
team-point value, zero identified a canonical recipient with the same
league/week/player and team total. This does not change the cache or close the
artifacts; it records that team points cannot resolve this particular missing
manager identity, so the remaining 4,068 null cells require a roster/team
identity bridge or an explicit source-only disposition.

The current manager-fanout audit,
[31695967974](https://github.com/jeleff1000/league-history-workers/actions/runs/31695967974),
then read three distinct retained team-signal artifacts against 230,225 existing
player rows. It found zero NULL fills: `win`, `team_points`, and `is_playoffs`
all agree on every source-backed row. The only remaining disagreement is 3,877
`champion` cells. Those are deliberately preserved rather than overwritten,
because these team-level source flags do not prove a player started the actual
championship matchup—the failure mode that previously inflated championship
rates. The source is therefore fully explained for those fields: no missing
cells remain, and champion requires championship-start evidence rather than
this season/team flag.

## Post-inventory MFL execution records

The residual MFL batch ran after the 9,479-artifact inventory was frozen. Its
231 records are now appended to the **same CSV**, not maintained as a separate
checklist. Their exact dispositions are also preserved in the source manifest
[`research_matchup_mfl_batch_31663728938_admission_manifest.csv`](research_matchup_mfl_batch_31663728938_admission_manifest.csv).

| State | Records | Cache consequence |
| --- | ---: | --- |
| Candidate-bearing batch member | 219 | Included in the same-key MFL apply/readback receipt `31682677203`. The ledger labels this `batch_member_legacy_cache_readback`: aggregate receipt evidence is retained, while the absence of historical per-cell prestate is explicit. |
| Valid zero-delta artifact | 1 | Compared against the cache and emitted no eligible change. |
| Diagnostic-only artifact | 3 | Contains no `team_signals.parquet`/`player_bridge.parquet` pair, so it has no player-table cache candidate. |
| No GitHub artifact emitted | 8 | The recovery shard stopped before output. The ledger records a deterministic logical receipt ID, not a fabricated GitHub artifact ID, and retains the precise rerun action if its target remains unresolved. |

These eight no-output records do not conceal unapplied data: there is no source
artifact to join. They are an execution-recovery queue, separate from the
693 source-identity/source-precedence rows in the frozen inventory.

## Why the remaining team-signal artifacts cannot be directly upserted

This is now source-schema evidence, not an inference from a failed join. Two
retained artifact reads are representative of the remaining raw team-signal
families:

| Artifact | Rows read | Present identity | Missing identity required for player upsert |
| --- | ---: | --- | --- |
| `9013217044` / `sleeper-missing-outcome-rescue-3-31229249246` | 4,140 | `db_name`, year/week, manager, `manager_guid`, team key/name, outcome and playoff fields | No NFL player ID, Sleeper player ID, roster membership, or player-to-team edge. |
| `9016426057` / `research-sparse-playoff-championship-rescue-5-31239084738` | 80 | `db_name`, year/week, manager/franchise/team keys, outcome and playoff/championship fields | No NFL player ID, native player ID, roster membership, or player-to-team edge. |

The 693-open partition has the same ledgered shape: 479 artifacts have only a
safe manager-week fan-out and 15 have a strict player-key profile; the final
two have no cache-side player/team identity. Therefore a direct upsert from
these team-week files would be a fabricated player attribution. The next
eligible cache change must instead prove, for each source team-week:

`source roster membership -> canonical NFL player -> existing player week -> source team`.

Where that chain is unique, outcome/playoff/championship cells can be promoted
and read back. Where it is absent or ambiguous, the ledger must retain the
specific source-only/precedence reason rather than invent a recipient.

The new MFL classification roster-identity receipt is Actions
[31660451544](https://github.com/jeleff1000/league-history-workers/actions/runs/31660451544).
It independently restored the approved cache after apply run `31659880241` and
read back all 1,720 exact existing-player candidates from pilot `31658616693`:
zero unmatched keys and zero source-backed nulls for `win`, `loss`, `tie`,
`team_points`, and `is_playoffs`. The schema, player-row count, and ops-cache
hash were unchanged; cache object `6590907333` is the single same-key object.
This receipt covers the first 15 targeted MFL league-weeks only. A fresh
post-promotion inventory, [31661107930](https://github.com/jeleff1000/league-history-workers/actions/runs/31661107930),
found 1,295 target league-weeks / 141,598 candidate cache player rows still
requiring an exact bridge. Eleven pilot weeks are no longer targets; four of
the pilot weeks retain other unresolved player rows.

The first candidate-only recovery run,
[31661452691](https://github.com/jeleff1000/league-history-workers/actions/runs/31661452691),
was stopped after its workflow design flaw was identified: six safety-gate
failures did not retain their reports. Its 25 successful shard artifacts were
preserved and independently read locally. Together they cover 130 exact
league-weeks, all source-resolved, with 38,495 roster memberships and **zero**
promotable cache player-row fills. They are source-checked/no-candidate
evidence, not cache-promotion receipts, and therefore do not change the frozen
9,479-artifact status counts.

The corrected residual-only candidate run,
[31663728938](https://github.com/jeleff1000/league-history-workers/actions/runs/31663728938),
keeps the original 256-way deterministic mapping and excludes exactly those
25 completed shard IDs. It therefore processes only the remaining 1,165
league-weeks in 231 read-only shards. Every safety-gate failure now uploads
its diagnostic artifact. No shard is complete as a cache repair until a
separate in-place promotion and independently restored cache readback produces
a `cache_recovery_receipt`.

### 2026-08-13 MFL candidate-set checkpoint

Run `31663728938` ended as cancelled only because two long-running shards were
manually cancelled after their diagnostics had not completed. Its usable source
set is nevertheless exact. Its final per-artifact admission status is recorded
in [the durable admission manifest](research_matchup_mfl_batch_31663728938_admission_manifest.csv),
which covers every one of the 231 residual shards and links the admitted set to
the independently read-back same-key receipt `31682677203`:

| Final admission state | Artifacts | Meaning |
| --- | ---: | --- |
| `included_in_verified_cache_batch` | 219 | The artifact emitted one or more source-backed candidate rows. Those rows are included in the saved apply evidence and in the readback receipt. |
| `admitted_no_promotable_delta` | 1 | The artifact was structurally valid, but its rows added no candidate after exact comparison with the cache. |
| `source_diagnostic_only` | 3 | The archive contains diagnostics but neither required `team_signals.parquet` nor `player_bridge.parquet`; it cannot supply a team-to-player promotion. |
| `explicitly_excluded_no_candidate` | 8 | The recovery shard either stopped before output or failed cache restore before recovery code began; no candidate exists to apply. |

This resolves the former ambiguous "223 retained artifacts" phrasing below.
Only the 219 candidate-bearing artifacts supplied the source rows in the
verified cache batch. The one zero-delta artifact was not silently dropped;
the three diagnostic-only artifacts and eight no-candidate exclusions are
explicitly preserved for targeted follow-up rather than misrepresented as
cache improvements.

| Classification | Shards | Evidence | Admission state |
| --- | --- | --- | --- |
| Retained named candidate artifacts | 220 | 219 candidate-bearing artifacts plus one structurally valid zero-delta artifact. | Fully accounted for in the durable manifest. |
| Incomplete cancelled diagnostics | `32, 156` | Each emitted only `cache_before.json` and `ops_before.sha256`, with no extraction, bridge, signal, or candidate-delta file. | Excluded; must be rerun separately. |
| Cache-restore failures | `190, 191, 192, 193, 194, 195` | Each failed in under 20 seconds at `fail-on-cache-miss`, before recovery code ran, and emitted no artifact. | Excluded; no candidate exists to promote. |

The first guarded promotion,
[31674545745](https://github.com/jeleff1000/league-history-workers/actions/runs/31674545745),
failed safely at preflight before candidate construction or cache mutation: its
artifact filter mistakenly required a retained artifact for every explicit
exclusion, including the six shards that failed before any artifact existed.
The corrected guard permits an explicit exclusion to have zero or one artifact
(never more than one), and emits a receipt listing zero-artifact exclusions.
The first retry also stopped before candidate construction because GitHub's bulk
`gh run download` transport stalled while pulling all prior-run artifacts. The
replacement transport fetches only the selected artifact IDs, at 15 concurrent
bounded/retrying requests, and verifies both required Parquet files in every
archive before candidate assembly. Its first execution exposed a temporary-file
race (`zips/.zip`): Bash expanded `zip_path` before assigning `artifact_id`.
That attempt also stopped before candidate construction or cache mutation. The
download function now assigns the artifact ID before deriving its unique ZIP
path. The following retry proved that Ubuntu's `unzip` rejects the GitHub
archive layout as an overlap/zip-bomb; Python's standard `zipfile.ZipFile`
reads the same archive and extracts only the two required Parquet members.
That attempt likewise stopped before candidate construction or cache mutation.
The batch test job now renders this exact download block and runs `bash -n`
before the apply job is even eligible to restore the cache.
The exported-function implementation then proved that heredocs are unsafe in
the `xargs` child-shell boundary despite passing `bash -n`. It has been
replaced with a tested repository script,
`extract_mfl_artifact_members.py`, which extracts exactly
`out/team_signals.parquet` and `out/player_bridge.parquet` from each downloaded
archive. No cache-facing behavior changed in these transport-only revisions.
Run [31678670544](https://github.com/jeleff1000/league-history-workers/actions/runs/31678670544)
then established an additional source fact without mutating the cache: shards
`92`, `111`, and `139` contain roster-membership/player-directory diagnostics
but neither required bridge member. They are **source-diagnostic-only**, not
transfer failures and not cache improvements. The next guarded preflight
records this state per artifact, excludes those artifacts from candidate
assembly, and still fails closed for any archive that is neither complete nor
explicitly diagnostic-only.
The subsequent candidate build in
[31679763905](https://github.com/jeleff1000/league-history-workers/actions/runs/31679763905)
passed artifact admission and stopped at the duplicate-key guard before any
cache replacement: 247 cache keys appeared in both the NULL-fill and direct
MFL win-replacement classes. This is an expected overlap in candidate classes,
not conflicting source evidence. The next preflight merges such overlaps by
the fixed canonical key, rejects any disagreement among actual `source_*`
values, and preserves deterministic provenance. It still cannot reach the
cache until the merge report has zero conflicting source keys.
Run [31680569001](https://github.com/jeleff1000/league-history-workers/actions/runs/31680569001)
passed the in-place apply invariants and saved the same approved cache key. Its
first readback rule was too broad because it compared deliberately preserved
non-null cache values. The corrected independent readback
[31682677203](https://github.com/jeleff1000/league-history-workers/actions/runs/31682677203)
has now appended the sixth supplemental cache receipt. It verifies the
authorized NULL fills and direct MFL `win=0 -> 1` replacements while preserving
all other non-null disagreements for source-precedence adjudication. Schema,
player-row count, ops hash, and the single approved cache object were unchanged.

Because the historical saved delta lacks per-cell canonical prestate, this
receipt is deliberately labeled `legacy_apply_receipt_minimum_match`: the
transaction apply report is the authoritative count, while fresh readback
proves matching source-backed cells and reports preserved non-null source
conflicts. This closes the batch-level cache receipt; it does **not** erase the
separate current-open-artifact work queue or silently classify unresolved source
identity/conflict evidence as fixed.

## Active work: not yet applied

The following Actions runs are deliberately recorded as **in flight**, not as
cache improvements. Both restore the same approved cache key read-only; neither
can save a cache, alter the schema, or create a lineage.

| Run | Exact scope | Required next action before any ledger closure |
| --- | --- | --- |
| [31663728938](https://github.com/jeleff1000/league-history-workers/actions/runs/31663728938) | The 231 residual deterministic MFL roster-identity shards, covering only the 1,165 league-weeks not already examined by the preserved 25 successful shards from 31661452691. | **Batch cache receipt complete.** The 231 artifacts are fully dispositioned in the admission manifest; 219 candidate-bearing artifacts are included in the independent readback receipt [31682677203](https://github.com/jeleff1000/league-history-workers/actions/runs/31682677203). Diagnostic-only and no-candidate shards remain explicit follow-up records. |
| [31668247614](https://github.com/jeleff1000/league-history-workers/actions/runs/31668247614) | Two championship-probe artifacts, `8956468822` and `8956471264`, read back with the corrected dynamic ledger guard. | Inspect the retained receipt and strict-null candidate. Apply only if exact existing-player recipients and source-backed null cells are present. |
| [31668497497](https://github.com/jeleff1000/league-history-workers/actions/runs/31668497497) | The two smallest blocked sparse playoff/championship artifacts, `9016485043` and `9016453846`, totaling 2,816 source cells. It emitted an exact 600-key, loss/tie-only NULL-fill delta with zero source conflicts. | **Complete.** Its dedicated promotion/readback [31669704187](https://github.com/jeleff1000/league-history-workers/actions/runs/31669704187) freshly restored the approved cache and proved 600/600 loss cells and 600/600 tie cells are filled, with zero unmatched keys; schema, player rows, ops seed, and the single approved cache lineage are unchanged. Receipt SHA-256: `746936c0fce4ba25df78513b0787e84223f736d0d733c64c6db22370da4888e0`. |

### Fleaflicker team-to-player identity recovery

The two historical championship-probe artifacts above have expired. That does
**not** make their target data unrecoverable: the probe workflow built them
from the approved cache's `player_fantasy` and `matchup` tables. The next
read-only target builder must therefore regenerate the exact *current* set of
Fleaflicker player-week rows that lack cache-side team identity, together with
their matching cache matchup team signals.

For each resulting `(db_name, year, week, team_key)`, the only external call is
Fleaflicker's `FetchLeagueRosters`. It returns the same `team_key` and stable
`fleaflicker_player_id` for every roster member. The safe bridge is then:

`cache matchup team signal -> roster team_key/fleaflicker_player_id -> existing cache player row`

The final promotion remains NULL-fill-only and must use
`(db_name, year, week, fleaflicker_player_id)`, followed by the standard
fresh-cache readback. No name matching, new columns, new cache, or new lineage
is permitted.

**Current correction, recorded 2026-08-13.** The read-only target receipt
[31672174378](https://github.com/jeleff1000/league-history-workers/actions/runs/31672174378)
proved that this final native-ID join is not currently executable: all
2,730,186 cache player rows without cache-side team identity also lack a
populated `fleaflicker_player_id`; the corresponding source-signal overlap is
4,197,680 rows and likewise has zero populated native IDs. It produced zero
candidate team-weeks and made no cache mutation. Therefore `FetchLeagueRosters`
must first be used only to form a strict source roster identity bridge to an
existing canonical `NFL_player_id` (including the existing DEF franchise map).
Only exact one-to-one source-player-to-NFL-player results may then populate the
already-existing `fleaflicker_player_id` cells and fan out team signals. Any
ambiguous player, missing canonical recipient, or unresolved DEF identity stays
blocked with a receipt; display-name or manager fallbacks remain prohibited.

An Action completion by itself never changes the frozen-artifact disposition.
Only a current approved-cache readback may set `cache_verified` or add a
`cache_recovery_receipt`.

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
yet been run against the target population, so the ledger's 693 open artifact
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
| `cache_verified` | 323 | Direct current-cache readback proves these artifacts' supported cells match. This includes the loss/tie receipts and 11 direct-MFL exact-key artifacts re-audited in run `31716352919`. |
| `no_promotable_candidate_emitted` | 4,529 | Candidate-classified artifact, but no promotable cell was emitted by the frozen comparison. |
| `not_data_bearing` | 3,927 | Aggregate/validation/metadata artifact; not a canonical-cell source. |
| `cache_conflict_preserved` | 7 | 2,142,503 supported cells already match; 9,397 non-null conflicts are preserved; 480,208 source cells remain identity-unmatched; zero cache-null fill candidates. |
| `source_only_team_signal_missing_player_team_bridge` | 2 | 151 source team-weeks overlap cache league-weeks but have no safe manager/team bridge to player rows; they cannot be fanned out. |
| `partial_schema_blocked_direct_identity` | 4 | Direct MFL player-key source maps to duplicate cache recipients. It needs a deterministic recipient rule; no manager-name fallback is permitted. |
| `partial_schema_blocked_unmatched` | 472 | Historical status label only. The current open gate is a player/team identity bridge or explicit source-only closure; loss/tie schema support is complete. |
| `partial_schema_blocked_conflicts` | 214 | Historical status label only. The current open gates are a player/team bridge plus field-level source-precedence adjudication. |
| `unmatched_cache_key` | 1 | The source key has no canonical recipient; reconcile identity or close it source-only. |

The receipt workflow is read-only. It asserts, before and after the audit,
that player schema, player-row count, and the ops-cache hash are unchanged;
it has no cache-save, cache-delete, or lineage-creation step.

## Cache-completion matrix

The 9,479 artifacts are now partitioned without double-counting by their
evidence state:

| Completion state | Artifacts | Meaning |
| --- | ---: | --- |
| Closed by direct cache evidence | 323 | Every compared canonical cell equals the approved cache. |
| Closed: no promotable cache cell emitted | 4,529 | The frozen extraction/comparison emitted no canonical-cell candidate. |
| Closed: non-data-bearing | 3,927 | Aggregate, validation, inventory, or metadata evidence; not a source of canonical cells. |
| **Open: supported null-cell promotion** | **0** | No safe, unpromoted null-cell candidate is currently recorded in the ledger. |
| **Legacy loss/tie gate requiring re-audit** | **0** | All retained loss/tie evidence is now marked `closed_schema_supported`; this is not an open schema gap. |
| **Open: player-team identity bridge or source-only closure** | **479** | Source has team-week or direct-player evidence that cannot yet be connected safely to a single eligible player row, or must be explicitly closed as source-only. |
| **Open: source-precedence adjudication** | **214** | Source differs from an existing non-null cache value; no overwrite may occur without a field-level precedence rule. |

The final two open categories are disjoint in the current ledger. The loss/tie
schema gate is closed for every artifact. The artifact partition is exact:
**8,786 closed, 693 open**.

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
| `open_player_team_bridge` | 479 | Produce raw roster-membership evidence or an explicit source-only closure. |
| `open_player_team_bridge_and_source_precedence` | 214 | Complete bridge proof and field-level conflict adjudication. |
| `closed_*` | 8,786 | Closed by the final status shown above; no cache update is implied unless the status is `closed_cache_verified`. |

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
| Direct-player duplicate-recipient identity | 4 | The direct MFL re-audit found zero null candidates and zero conflicts; 2,765 source cells map to multiple canonical recipients and need exact duplicate resolution. |
| Player-team bridge or source-only closure | 475 | Source team evidence needs a deterministic roster/player bridge or a documented source-only closure. |
| Player-team bridge + source precedence | 214 | First prove the recipient bridge, then adjudicate each conflicting non-null field under the approved precedence policy. |

### Identity evidence already attached

The CSV now carries existing read-only profile evidence in
`identity_profile_run_id`, `source_team_keys`,
`matched_source_team_keys`, `unmatched_source_team_keys`,
`league_week_overlap_keys`, `league_week_absent_keys`, and
`identity_bridge_status`. This is evidence from prior receipts, not a new
fetch or a cache mutation.

| Identity state among the 693 open artifacts | Artifacts | Meaning |
| --- | ---: | --- |
| `player_key_profiled` | 4 | Direct MFL player-key evidence with duplicate canonical recipients; no null-cell candidate or non-null conflict remains. |
| `partial_bridge` | 559 | Some source teams fan out safely; the remaining teams need a bridge. |
| `partial_bridge_with_source_only` | 128 | A partial bridge exists and some source league-weeks have no player rows to receive data. |
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

This is a historical receipt matrix from before the canonical `loss`/`tie`
schema addition. The current CSV gate marks every frozen artifact
`closed_schema_supported`; it is not an open schema-work queue. The retained
counts below are provenance only, not distinct cache cells.

| Admission condition | Artifacts | Retained loss/tie cells | Additional obligation |
| --- | ---: | ---: | --- |
| Schema only | 4 | 464 | Closed historical receipt; exact null-fill/readback is complete. |
| Direct MFL player evidence | 15 | 288,160 | Re-audited on direct player key: 11 cache-verified, four duplicate-recipient identities remain. |
| Schema plus identity gap, conflict, or both | 694 | 4,747,094 | Historical receipt count. Current work is deterministic player-team bridging and, where applicable, source-precedence adjudication. |

Those historical counts must not be used as a current cache-gap total. The
machine ledger's current unresolved partition is exactly 479 identity/source-
only artifacts plus 214 identity-and-precedence artifacts.

### Non-null conflict inventory

There are 214 currently open artifacts with a source value that differs from
an existing non-null cache value. The ledger guard requires every one to name
`precedence` in `next_action`; no current workflow is allowed to overwrite
these values implicitly.

| Evidence family | Artifacts | Conflict cells | Source fields represented | Required decision |
| --- | ---: | ---: | --- | --- |
| Team-week signal evidence | 214 | Historical retained conflict evidence | `win`, `loss`, `tie`, `team_points`, `is_playoffs`, `champion` | Compare exact platform provenance at the proven player-team recipient; preserve same-tier disagreement. |
| Direct MFL player evidence | 0 | 0 | — | The direct re-audit found zero non-null conflicts; its four residual artifacts are identity-ambiguous only. |

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
cache key remains a single object. The latest independent read-only receipt
[31641698977](https://github.com/jeleff1000/league-history-workers/actions/runs/31641698977)
again found zero remaining safe fills and zero source/cache conflicts. It also
recomputed the admission gates from that fresh receipt, eliminating the stale
schema-blocked label.

This artifact is deliberately still **open** in the CSV as
`unmatched_cache_key`, rather than being falsely marked complete: 3,606 source
team-week identities have no canonical player recipient. Its loss/tie schema
gate is now closed; its only remaining gate is the documented player-team
bridge or a source-only closure.

The CSV is the machine-readable authority: **8,786 of 9,479 artifacts are
closed** and **693 remain open**. The open partition is 472
identity-unmatched artifacts, 214 identity-and-precedence artifacts, four
direct duplicate-recipient artifacts, two source-only player-identity gaps,
and one unmatched cache-key artifact. Every one of the
693 has a non-empty reason and next action in the CSV; an artifact is never considered
closed merely because its parent workflow completed.

## MFL source-team player bridge — independently proven

The original bridge required a cache manager name before it would emit a
player/team mapping. That was the wrong boundary for MFL source-only rows,
whose cache-side manager/team fields are often null. The corrected source chain
is direct and deterministic: MFL franchise roster membership → year-specific
MFL-to-NFL player crosswalk → raw MFL franchise/team-week signal → existing
canonical player row.

The read-only candidate run [31645490959](https://github.com/jeleff1000/league-history-workers/actions/runs/31645490959)
found 204 exact existing player rows from its captured shard. It had no
non-null conflicts and could fill four values on each row: `loss`, `tie`,
`team_points`, and `is_playoffs`. The same-key apply run
[31646045505](https://github.com/jeleff1000/league-history-workers/actions/runs/31646045505)
applied only those null fills. The independent fresh restore
[31646989467](https://github.com/jeleff1000/league-history-workers/actions/runs/31646989467)
proved all 204 rows match, with zero remaining source-backed nulls for each of
the four fields. It also proved the player schema, player-row count, and ops
cache were unchanged and that exactly one approved cache object remained.

The receipt SHA-256 is
`8f6ae61a79ae2c561206b2054329ca3d24d7a926449cdec5199446d05f2386e3`.
It is a supplemental `cache_recovery_receipt`; it does not alter the frozen
9,479-artifact inventory or falsely close the still-open source-only team
identities.

## Direct MFL player crosswalk bridge — independently promoted

The retained MFL roster evidence exposed 98 direct MFL-to-NFL mappings after
collapsing nine identical repeated source records (zero conflicting native
MFL keys). The resulting franchise-team-to-player bridge contributed 31
otherwise-unavailable player recipients. A read-only comparison found 14
safe existing-player NULL-fill rows; its 49 supported cells were seven `win`,
fourteen `loss`, fourteen `tie`, seven `team_points`, and seven `is_playoffs`.

The guarded promotion [31740148139](https://github.com/jeleff1000/league-history-workers/actions/runs/31740148139)
applied exactly those 49 cells. Its fresh independent restore read every
authorized cell back with zero source disagreements. It inserted no rows and
proved the player schema, player-row count, ops cache, and approved single
cache lineage were unchanged. The receipt SHA-256 is
`5d7c6c9753df405c1d31183279d2dc74ec4bd1c592b359285af67cb6770b202f`.

This is a supplemental receipt only: it closes this verified improvement, not
the broader source-artifact families that still have separately ledgered
identity gaps or precedence decisions.

| Family | Artifacts | Current state | Required action |
| --- | ---: | ---: | --- |
| `sleeper-missing-outcome-*` | 56 | Direct manager-week fan-out receipt complete: 24,382,006 supported cells equal cache; zero supported null fills; 90 non-null conflicts; 171,648 source cells identity-unmatched; 466,458 loss/tie cells outside the prior schema. | Resolve/close residual source-only identities and adjudicate conflicts; no null-cell promotion for this family. |
| `promotable-rescue-delta-*` team-week subset | 7 | Direct fan-out receipt complete: 2,142,503 supported cells equal cache; zero supported null fills; 9,397 preserved conflicts; 480,208 identity-unmatched cells. | Resolve/close residual source-only identities and adjudicate conflicts; no null-cell promotion for this subset. |
| `promotable-rescue-delta-*` player-row subset | 11 | The files are full player-row snapshots with canonical field names; the generic `source_*` receipt correctly emitted no comparison rather than guessing. | Run the exact canonical-field snapshot comparator on the full player key. |
| `mfl-player-outcome-classification-*` | 15 | Direct player-key re-audit 31716352919: 11 artifacts are cache-verified; four retain 2,765 duplicate-recipient source cells. There are zero safe null candidates and zero direct non-null conflicts. | Resolve only the four duplicate-recipient identities; do not use a manager-name fallback. |
| `research-source-matchup-rescue-*` | 14 | Corrected fan-out receipt: 28,032,962 cells already equal cache; zero supported null fills; remaining cells are conflicts, schema-blocked loss/tie, or unresolved team identity. | Adjudicate non-null conflicts; resolve/close residual identity gaps; do not run a null-cell promotion for this family. |
| `research-championship-identity-probe-*` | 9 | Fully adjudicated by receipt 31552757341: seven MFL files have a safe manager-week fan-out but zero valid player-row fills; two files remain source-only because no cache-side identity bridge exists. | Do not promote championship or playoff values from this family; retain the two source-only identity gaps as explicitly unresolved. |
| `research-sparse-playoff-championship-*` | 621 candidate-bearing | Fully receipted; zero supported null-cell fills. | Close source-only/conflict rows under the explicit precedence and loss/tie schema decisions. |

## Fleaflicker historical-roster bridge â€” source behavior gate discovered

Read-only local source probes on 2026-08-13 established that Fleaflicker has
two different historical `FetchRoster` behaviors.  League `104989`, season
2019, week 13 returned a roster consistent with that historical season.  A
ten-target sparse-playoff sample, however, returned source standings for all
ten targets but a year-valid roster for only four; six returned no
year-matching roster rows.  One of the four apparent successes (`105788`,
requested 2011/week 18) was a false historical response: it contained current
players including Baker Mayfield, Bucky Irving, and Jordan Addison.  The
endpoint accepted the requested old period but did not serve the old roster.
The companion `FetchLeagueScoreboard(105788, 2011, 1)` is independently
genuine (week-one epoch 2011-09-06), which isolates the defect to the roster
endpoint rather than the league-year resolver.

This is a source contract issue, not a cache defect.  A Fleaflicker roster
bridge is admissible only when its roster membership independently passes a
season-plausibility check against the NFL player universe.  `requested period`
metadata alone is insufficient.  For genuinely historical roster payloads,
the existing year + normalized-name + broad-position contract resolved 41 of
46 members in the `104989` sample; the remaining five are known Fleaflicker
position aliases (`EDR`, `IL`, `EDR/IL`) and require explicit normalization to
the canonical `DL` family.  DST continues to use the canonical season-franchise
ID mapper.  No Fleaflicker source result from this probe has been promoted to
the approved cache.
