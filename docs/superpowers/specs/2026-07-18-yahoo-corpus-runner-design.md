# Yahoo Corpus Runner Design

## Objective

Build and operate the Yahoo research-corpus runner from `league-history-workers`. The runner turns the private Yahoo settings census into compact, identity-stripped league-year data that can be unioned with the existing research population. The first runs are deliberately small, era-stratified pilots intended to reveal historical Yahoo API and schema failures before scaling.

The runner must preserve the existing cohort semantics. It will reuse the private `yahoo_oauth` import pipeline rather than reproduce championship, draft, transaction, LAMAR, clutch, or player normalization logic in the public worker repository.

## Ownership and Repository Boundary

`league-history-workers` owns:

- the corpus plan builder;
- the credential-aware scheduler;
- the per-league-year corpus driver;
- the GitHub Actions workflow that actually runs;
- redacted ledgers, pilot reports, and compact corpus artifacts.

The worker checks out `yahoo_oauth` at a requested ref solely as the private pipeline dependency. No Yahoo corpus Action is launched from `yahoo_oauth`. The private repository's workflow copy is a non-operational mirror required by repository synchronization policy.

## Source Population

The planner reads two private sources at workflow runtime:

1. Fly `___ops.main.league_credentials`, to obtain and decrypt distinct Yahoo refresh-token grants.
2. The Yahoo settings census manifest, to obtain accessible league keys, seasons, renewal links, settings classifications, and the grant that can access each league.

The census is not checked into the public repository. Pilot plans use opaque task and grant labels. League names, manager names, team names, Yahoo GUIDs, refresh tokens, access tokens, and Fly credentials must never be written into an uploaded artifact or unredacted workflow log.

Only completed seasons from 2001 through 2025 are eligible for the pilots. The in-progress 2026 season is excluded from outcome validation.

## Corpus Contract

Each successful Yahoo league-year is folded into the same compact four-table contract consumed by the current cohort builders:

- `league_settings`: cohort classification, eligibility gates, roster structure, playoff structure, waiver rules, platform, and source league key;
- `draft`: player identifier, pick/cost, keeper flag, value metrics, manager LAMAR, and fantasy-point context;
- `transactions`: player identifier, week, transaction type, normalized bid inputs, transaction score, and rest-of-season value fields;
- `player_fantasy`: player identifier, year/week, roster/start flags, fantasy points, win, champion, clutch equity, and manager LAMAR.

The existing `scripts/sleeper_corpus/build_corpus_snapshot.py` schema is the canonical contract. Shared folding code should be generalized or reused so Yahoo and Sleeper cannot drift into separate schemas.

Uploaded slices contain no manager, franchise, team, or OAuth identity. The stable research unit is an opaque corpus `db_name` plus platform league key and season.

## Execution Model

The plan unit is one Yahoo league-year, not an entire renewal lineage. For each task, the driver creates a one-year `LeagueContext` with:

- exactly one league key in `league_ids`;
- `start_year` and `end_year` equal to the task season;
- `import_mode=full` so the cohort-derived fields are generated;
- an ephemeral data directory;
- the selected grant supplied only in the ephemeral context.

It invokes `initial_import_v3.py` with corpus mode enabled, Track 1 disabled, and Track 2/Fly upload disabled. Required NFL and draft-baseline dependencies are restored from the same offline corpus dependency bundle used by the Sleeper grind. After validation, the league is folded into the output slice and its raw directory and OAuth context are removed from the runner.

There are no writes to Fly league tables, no production imports, and no customer-visible databases.

## Credential-Aware Scheduling

The scheduler creates a deterministic, resumable order with these invariants:

1. Never dispatch the same grant twice consecutively when any other ready grant exists.
2. Select the least-recently-used ready grant first.
3. Within a grant, select the season that maximizes distance from that grant's last dispatched season. Ties prefer the scarcer cohort, then the older season, then the opaque task ID.
4. A grant has at most one in-flight task.
5. A rate-limited task moves behind other grants and receives a per-grant cooldown. It does not block the whole queue unless the global limiter also trips.
6. Resume reconstructs last-use and cooldown state from the ledger, so restarting cannot immediately hammer the credential that ran last.

The runner also enforces a conservative global request/concurrency ceiling. Credential rotation is a fairness and burst-reduction technique, not an assumption that Yahoo has no application-wide limit.

For the initial pilots, one workflow job owns the schedule. This makes the dispatch order observable and prevents independent matrix jobs from accidentally selecting the same grant. Scaling to multiple jobs requires deterministic, disjoint grant shards; a grant may belong to only one live shard.

## Pilot Selection

### Pilot A: Cross-grant, cross-era

Select twelve completed league-years, preferably from twelve distinct grants, approximately three from each era:

- 2001-2009;
- 2010-2017;
- 2018-2022;
- 2023-2025.

Within each era, prefer different cohort slugs and avoid multiple years from one renewal lineage. If an era lacks enough valid distinct grants, the planner reports the shortfall and fills from the nearest era without silently changing the requested total.

### Pilot B: Spacing and resume

Select twelve completed league-years from four grants, three seasons per grant. For each grant, prefer the widest available year spread. The run must demonstrate that no adjacent dispatch repeats a grant and that the selected seasons for a grant are interleaved with other users' work.

Pilot B includes a controlled stop/resume checkpoint after at least four completed tasks. The resumed schedule must maintain the no-repeat and cooldown invariants.

## Workflow

The manually dispatched `league-history-workers` workflow exposes:

- pilot mode (`cross-era` or `spacing-resume`);
- private pipeline ref, defaulting to `main`;
- task limit, defaulting to 12;
- worker count, defaulting to 1 for the pilots;
- global delay/rate settings;
- optional dry-run/plan-only mode.

The workflow:

1. checks out `league-history-workers`;
2. checks out the private `yahoo_oauth` pipeline with the existing private-repository token;
3. installs dependencies and restores the corpus dependency cache;
4. reads/decrypts the Fly credential bank and private census at runtime;
5. builds and prints a redacted plan summary;
6. executes the credential-aware queue;
7. validates and folds every successful league-year;
8. generates a redacted pilot report;
9. uploads only the compact DuckDB slice, redacted ledger/report, and sanitized driver log.

The job fails if zero tasks succeed or if failures exceed successes. A partial pilot may still upload its validated successes and diagnostic report using `if: always()`.

## Validation and Failure Taxonomy

Every successful task must satisfy:

- exactly one matching `league_settings` row for the requested year;
- all four corpus tables exist with the canonical columns;
- rostered player rows exist;
- no duplicate `(db_name, year)` source;
- a completed season has exactly one distinct champion franchise before identity stripping;
- cohort classification agrees with the settings census;
- the compact artifact contains no forbidden identity or credential columns;
- no refresh/access token or private manager/team string appears in the redacted outputs.

Failures are recorded by stage and sanitized error class:

- credential refresh/authentication;
- settings/context construction;
- matchup acquisition;
- roster acquisition;
- draft acquisition;
- transaction acquisition;
- recovery exhaustion/rate limit;
- transformation;
- champion validation;
- cohort mismatch;
- fold/schema validation.

The pilot report groups successes and failures by era, season, cohort, stage, and retry/rate-limit status. It exposes opaque task IDs, never private names or tokens.

## Testing

Unit tests cover:

- era-stratified selection and documented fallback;
- least-recently-used grant rotation;
- maximum year-distance selection;
- no adjacent repeated grants;
- per-grant cooldown and 429 requeue;
- resume-state reconstruction;
- one-year context construction without token serialization to artifacts;
- sanitized logs and artifact allowlists;
- compact schema and champion validation.

Workflow tests or static assertions verify that:

- execution is owned by `league-history-workers`;
- Fly writes are disabled;
- raw/context directories are excluded from artifacts;
- failure artifacts upload even when the driver exits nonzero;
- the private pipeline ref is explicit.

## Completion Criteria

The feature is ready to scale only after both pilots run in GitHub Actions and produce:

- at least one successful league-year from every era with an eligible source;
- a valid four-table corpus slice readable by the existing cohort reader;
- an observed dispatch trace satisfying every scheduler invariant;
- a year/stage failure report that identifies historical Yahoo headaches;
- no secret or private identity leakage in uploaded artifacts or logs.

The pilots establish ingestion correctness and operational limits. They do not by themselves establish cohort reliability or the final `r >= 0.85` target.
