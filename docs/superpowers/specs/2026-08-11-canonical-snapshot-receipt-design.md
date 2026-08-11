# Canonical snapshot receipt design

## Purpose

Close the remaining eleven `promotable-rescue-delta-31283057739-*` ledger
entries with direct, reproducible evidence from the one approved research
cache.  These artifacts are full player-row snapshots.  The existing generic
receipt reader deliberately ignores them because their signal columns use
canonical names rather than `source_*` aliases.

## Scope

This is a read-only receipt job.  It does not promote data and it does not
alter the canonical cache, cache key, lineage, schema, or ops seed cache.

Inputs:

- Approved cache key:
  `research-public-lake-v4-Linux-20260806-championship-v2-final-playoff-anchor-outcomes-31147899613`
- Eleven exact artifact IDs from Actions run `31283057739` currently marked
  `candidate_provenance_not_found` in
  `docs/research_matchup_artifact_cache_ledger.csv`.
- Only the canonical columns that exist in both the source snapshot and
  `public.player_fantasy`:
  `win`, `team_points`, `is_playoffs`, `champion`, `has_po_signal`,
  `final_playoff_seed`, `made_playoffs`, and `clutch_equity`.

## Identity and comparison rules

1. Match a source row to a player row on the full canonical player identity:
   `(db_name, year, week, NFL_player_id, platform, manager, team_key,
   team_name)`, using null-safe equality.
2. When a source snapshot has null `team_key` and `team_name`, allow the
   shorter identity `(db_name, year, week, NFL_player_id, platform, manager)`
   only if it resolves to exactly one canonical player row.  Zero or multiple
   matches stay unmatched; no fan-out or guess is allowed.
3. Exclude duplicate source identities from comparison.  Do not choose a
   winner among conflicting source rows.
4. For each non-null source cell, record exactly one outcome:
   - equal to current canonical value;
   - canonical cell is null (a future promotion candidate);
   - canonical cell is non-null and conflicts;
   - source row is unmatched or ambiguous.
5. Never overwrite or resolve conflicts in this receipt job.

## Outputs and ledger treatment

The workflow produces one receipt row per artifact with source, equal,
candidate, conflict, and unmatched cell counts.  It replaces only the stale
receipt counters for these eleven artifact IDs in the existing 9,479-row
ledger; it never adds ledger rows.

Final ledger statuses:

- `cache_verified` when all non-null source cells are equal;
- `candidate_built_pending_canonical_promotion` when canonical-null supported
  cells exist;
- `cache_conflict_preserved` when only equal/conflicting non-null canonical
  values remain;
- `unmatched_cache_key` when source rows cannot be uniquely tied to a
  canonical player row.

Every status includes an explicit reason and next action.  A future promotion
requires separate approval for a controlled replacement transaction under the
same cache key, followed by an exact cache readback.

## Safety gates

Before and after the job, assert all of the following:

- the canonical player schema is byte-for-byte unchanged;
- the canonical player row count is unchanged;
- the ops cache SHA-256 is unchanged;
- `cache_mutated` is false;
- `new_lineage` is false;
- the receipt covers exactly the eleven selected IDs and the ledger remains
  exactly 9,479 rows.

## Test cases

- exact full-key match records equal/null/conflict counts correctly;
- nullable team identity resolves only where the short key is uniquely
  canonical;
- ambiguous short-key match does not emit a candidate;
- duplicate source identity is excluded;
- a source-only field is ignored rather than changing schema;
- receipt generation cannot alter the cache or ops file.

## Non-goals

- applying candidates;
- adding `loss` or `tie` fields;
- downloading new platform data;
- changing player or cache schemas;
- creating, deleting, renaming, or replacing a cache lineage.
