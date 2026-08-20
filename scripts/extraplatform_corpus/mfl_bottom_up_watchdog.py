#!/usr/bin/env python3
"""Keep three ordered-manifest MFL campaigns in flight and replace slots promptly."""
from __future__ import annotations

import json
import os
import subprocess
import zipfile
from concurrent.futures import ThreadPoolExecutor
from io import BytesIO
from datetime import datetime, timezone


def gh_json(args: list[str]) -> dict | list:
    result = subprocess.run(["gh", "api", *args], check=True, capture_output=True, text=True)
    return json.loads(result.stdout)


def parse_time(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def workflow_runs(repo: str, workflow: str) -> list[dict]:
    pages = gh_json([
        f"repos/{repo}/actions/workflows/{workflow}/runs",
        "--method", "GET", "-f", "per_page=100", "--paginate", "--slurp",
    ])
    runs: list[dict] = []
    for page in pages:
        runs.extend(page.get("workflow_runs", []))
    return runs


def dispatch(repo: str, workflow: str, values: dict[str, str]) -> None:
    command = ["gh", "workflow", "run", workflow, "--repo", repo, "--ref", "main"]
    for key, value in values.items():
        command.extend(["-f", f"{key}={value}"])
    subprocess.run(command, check=True)


def cancel(repo: str, run_id: int) -> None:
    subprocess.run(["gh", "run", "cancel", str(run_id), "--repo", repo], check=False,
                   capture_output=True, text=True)


def batch_start_from_plan_artifact(repo: str, run_id: int) -> int | None:
    try:
        payload = gh_json([
            f"repos/{repo}/actions/runs/{run_id}/artifacts",
            "--method", "GET", "-f", "per_page=100",
        ])
        artifact = next((a for a in payload.get("artifacts", [])
                         if a.get("name") == f"mfl-batch-plan-{run_id}"
                         and not a.get("expired")), None)
        if not artifact:
            return None
        result = subprocess.run(
            ["gh", "api", f"repos/{repo}/actions/artifacts/{artifact['id']}/zip"],
            check=True, capture_output=True,
        )
        with zipfile.ZipFile(BytesIO(result.stdout)) as archive:
            raw = archive.read("batch_plan.json")
        return int(json.loads(raw.decode("utf-8"))["batch_start"])
    except (subprocess.CalledProcessError, KeyError, TypeError, ValueError,
            json.JSONDecodeError, zipfile.BadZipFile, OSError):
        return None


def main() -> int:
    repo = os.environ.get("MFL_REPO", "league-history-workers/mfl-league-fetcher")
    workflow = os.environ.get("MFL_CAMPAIGN_WORKFLOW", "mfl_register_batch_campaign.yml")
    epoch = parse_time(os.environ.get("MFL_SCHEDULER_EPOCH", "2026-08-19T03:10:00Z"))
    now = datetime.now(timezone.utc)
    dry_run = os.environ.get("MFL_DRY_RUN", "false").lower() == "true"
    unusable = {"failure", "cancelled", "timed_out", "startup_failure", "action_required"}
    campaigns = workflow_runs(repo, workflow)
    direct = [r for r in campaigns if r.get("created_at") and parse_time(r["created_at"]) >= epoch
              and r.get("conclusion") not in unusable]
    direct.sort(key=lambda r: (r.get("created_at", ""), int(r.get("id", 0))))
    grace = int(os.environ.get("MFL_QUEUE_GRACE_MINUTES", "30"))
    stale = [r for r in direct if r.get("status") == "queued" and r.get("created_at")
             and (now - parse_time(r["created_at"])).total_seconds() >= grace * 60]
    stale_ids = {int(r["id"]) for r in stale}
    if not dry_run:
        for run in stale:
            cancel(repo, int(run["id"]))
    direct = [r for r in direct if int(r["id"]) not in stale_ids]
    active_states = {"queued", "in_progress", "waiting", "requested", "pending"}
    active = [r for r in direct if r.get("status") in active_states]

    manifest_path = os.environ.get("MFL_ORDERED_MANIFEST",
                                   "derived/mfl_register/quota_planning/mfl_ordered_quota_manifest.json")
    batch_size = int(os.environ.get("MFL_ORDERED_BATCH_SIZE", "2"))
    batches_per_campaign = int(os.environ.get("MFL_BATCHES_PER_CAMPAIGN", "250"))
    target_active = int(os.environ.get("MFL_CAMPAIGNS_PER_WAVE", "3"))
    total_rows = int(os.environ.get("MFL_ORDERED_TOTAL_ROWS", "128940"))
    total_slots = (total_rows + batch_size - 1) // batch_size
    with ThreadPoolExecutor(max_workers=min(12, max(1, len(direct)))) as pool:
        starts_by_run = pool.map(
            lambda r: batch_start_from_plan_artifact(repo, int(r["id"])), direct
        )
        plans = {int(r["id"]): start for r, start in zip(direct, starts_by_run)}
    starts = [s for s in plans.values() if s is not None and s % batches_per_campaign == 0]
    known_slots = [s // batches_per_campaign for s in starts]
    next_slot = max(max(known_slots, default=-1) + 1, len(direct))
    exhausted = next_slot * batches_per_campaign >= total_slots
    needed = max(0, target_active - len(active))
    dispatch_count = min(needed, max(0, total_slots - next_slot * batches_per_campaign))
    decision = {
        "mode": "ordered_manifest_inflight_controller", "repo": repo,
        "checked_at": now.isoformat().replace("+00:00", "Z"),
        "manifest_path": manifest_path, "direct_campaigns_seen": len(direct),
        "active_campaigns": len(active), "target_active_campaigns": target_active,
        "active_run_ids": [int(r["id"]) for r in active],
        "stale_queued_runs": sorted(stale_ids),
        "cancelled_stale_runs": [] if dry_run else sorted(stale_ids),
        "cursor_batch_starts": sorted(set(starts)),
        "next_batch_start": next_slot * batches_per_campaign,
        "batch_size": batch_size, "batches_per_campaign": batches_per_campaign,
        "total_rows": total_rows, "total_batch_slots": total_slots,
        "exhausted": exhausted, "dispatched": [],
    }
    if exhausted:
        decision["reason"] = "ordered_manifest_exhausted"
    elif dispatch_count <= 0:
        decision["reason"] = "target_active_campaign_count_reached"
    else:
        for offset in range(dispatch_count):
            slot = next_slot + offset
            values = {
                "manifest_path": manifest_path,
                "yahoo_oauth_ref": os.environ.get("MFL_YAHOO_OAUTH_REF", "codex/mfl-gates"),
                "batch_start": str(slot * batches_per_campaign), "batch_count": str(batches_per_campaign),
                "batch_size": str(batch_size), "max_parallel": os.environ.get("MFL_MAX_PARALLEL", "180"),
                "cache_namespace": os.environ.get("MFL_CACHE_NAMESPACE", "mfl-ordered-schedule-v1"),
                "canonical_namespace": os.environ.get("MFL_CANONICAL_NAMESPACE", "mfl-ordered-schedule-canonical-v1"),
                "attempt_suffix": f"-ordered-slot-{slot}", "continue_population": "false",
                "collect_all": "true", "storage_mode": "chunked",
                "target_years": os.environ.get("MFL_TARGET_YEARS", "2000,2001,2002,2003,2004,2005,2006,2007,2008,2009,2010,2011,2012,2013,2014,2015,2016,2017,2018"),
            }
            if not dry_run:
                dispatch(repo, workflow, values)
            decision["dispatched"].append({"slot": slot, "batch_start": int(values["batch_start"]),
                                            "batch_count": batches_per_campaign,
                                            "attempt_suffix": values["attempt_suffix"]})
        decision["reason"] = "inflight_slots_replenished"
    print(json.dumps(decision, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
