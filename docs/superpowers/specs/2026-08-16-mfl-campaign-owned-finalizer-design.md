# MFL campaign-owned finalizer

## Status

Approved direction from the user: keep parallel league ingestion, but make a campaign's successful completion include canonical cache registration and proof.

## Problem

`mfl_register_batch_campaign.yml` currently ends its chunked path after saving an immutable source chunk and source index. Canonical promotion is performed later by manually dispatched `mfl_reduce_saved_chunk.yml` runs. Those runs need exact predecessor cache keys, must be serialized, and can fail because of queue timing or malformed dispatch inputs.

## Design

The campaign remains parallel at the league-fetch boundary. Its finalization boundary becomes authoritative:

1. `ingest` continues to process batches concurrently and records successful and rejected league-year receipts.
2. `combine` continues to validate receipts and build the immutable chunk, source index, and research overlay.
3. A new campaign-owned `finalize` job restores the exact source chunk/index and the exact prior canonical index, runs the same append/proof logic as the reducer, builds the successor research overlay, and saves the successor chunk, index, and overlay caches.
4. `verify_cache` validates the successor caches and publishes the final canonical cache keys and proof as job outputs/artifact metadata.

The finalizer runs once per campaign and is a single serialized transaction for that campaign. Campaigns that are intentionally launched concurrently must still provide distinct predecessor keys or be sequenced by the existing population planner; the workflow must never silently merge two campaigns from the same prior index.

## Invariants

The finalizer must fail closed unless all of these are true:

- source chunk, source index, and prior canonical index restore exactly;
- canonical schema is unchanged;
- every prior league-year key is preserved;
- no new lineage is introduced;
- no duplicate league-year keys exist;
- at least one new league is added;
- research overlay schemas remain exact and non-empty;
- successor cache keys are immutable and unique.

The existing standalone reducer remains available as a recovery tool for already-saved chunks, but it is no longer the normal campaign path.

## Failure handling

Ingestion failures remain isolated to their batch receipts. Combine/finalize failures fail the campaign and retain the source artifacts for recovery. No finalizer may overwrite an existing cache key or fall back to a prefix cache when restoring canonical state.

## Testing

Add unit-level tests for deterministic predecessor/successor key construction and for rejecting missing/duplicate predecessor state. Add a workflow-shape check that confirms the campaign finalizer depends on combine and verify_cache depends on finalizer. Existing reducer proof checks remain the integration contract.
