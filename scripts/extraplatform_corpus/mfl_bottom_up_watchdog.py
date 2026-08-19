#!/usr/bin/env python3
"""Dispatch three fresh ordered-manifest MFL waves every cooldown period."""
from __future__ import annotations

import json
import os
import subprocess
import sys
from datetime import datetime, timedelta, timezone


def gh_json(args: list[str]) -> dict | list:
    result = subprocess.run(["gh", "api", *args], check=True, capture_output=True, text=True)
    return json.loads(result.stdout)


def parse_time(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def workflow_runs(repo: str, workflow: str) -> list[dict]:
    pages = gh_json([f"repos/{repo}/actions/workflows/{workflow}/runs", "--method", "GET", "-f", "per_page=100", "--paginate", "--slurp"])
    runs: list[dict] = []
    for page in pages:
        runs.extend(page.get("workflow_runs", []))
    return runs


def dispatch(repo: str, workflow: str, values: dict[str, str]) -> None:
    command = ["gh", "workflow", "run", workflow, "--repo", repo, "--ref", "main"]
    for key, value in values.items():
        command.extend(["-f", f"{key}={value}"])
    subprocess.run(command, check=True)


def batch_start_from_plan_artifact(repo: str, run_id: int) -> int | None:
    """Read the campaign's authoritative batch_plan.json when available."""
    with tempfile.TemporaryDirectory() as tmp:
        result = subprocess.run(
            [
                "gh", "run", "download", str(run_id), "--repo", repo,
                "--name", f"mfl-batch-plan-{run_id}", "--dir", tmp,
            ],
            capture_output=True, text=True,
        )
        if result.returncode != 0:
            return None
        plan_path = Path(tmp) / "batch_plan.json"
        if not plan_path.is_file():
            return None
        try:
            plan = json.loads(plan_path.read_text(encoding="utf-8"))
            return int(plan["batch_start"])
        except (OSError, KeyError, TypeError, ValueError, json.JSONDecodeError):
            return None

def main() -> int:
    repo = os.environ.get("MFL_REPO", "league-history-workers/mfl-league-fetcher")
    campaign_workflow = os.environ.get("MFL_CAMPAIGN_WORKFLOW", "mfl_register_batch_campaign.yml")
    cooldown = timedelta(minutes=int(os.environ.get("MFL_COOLDOWN_MINUTES", "40")))
    epoch = parse_time(os.environ.get("MFL_SCHEDULER_EPOCH", "2026-08-19T03:10:00Z"))
    now = datetime.now(timezone.utc)
    campaigns = workflow_runs(repo, campaign_workflow)
    # Failed prepare/control runs did not consume their ledger slots.
    unusable = {"failure", "cancelled", "timed_out", "startup_failure", "action_required"}
    direct = [
        r for r in campaigns
        if r.get("created_at")
        and parse_time(r["created_at"]) >= epoch
        and r.get("conclusion") not in unusable
    ]
    direct.sort(key=lambda r: (r.get("created_at", ""), int(r.get("id", 0))))
    queued_direct = [r for r in direct if r.get("status") == "queued"]
    queue_grace = timedelta(minutes=int(os.environ.get("MFL_QUEUE_GRACE_MINUTES", "30")))
    stale_queued = [
        r for r in queued_direct
        if r.get("created_at") and now - parse_time(r["created_at"]) >= queue_grace
    ]
    fresh_queued = [r for r in queued_direct if r not in stale_queued]
    latest_direct = parse_time(direct[-1]["created_at"]) if direct else None
    quiet = latest_direct is None or now - latest_direct >= cooldown

    manifest_path = os.environ.get("MFL_ORDERED_MANIFEST", "derived/mfl_register/quota_planning/mfl_ordered_quota_manifest.json")
    batch_size = int(os.environ.get("MFL_ORDERED_BATCH_SIZE", "2"))
    batches_per_campaign = int(os.environ.get("MFL_BATCHES_PER_CAMPAIGN", "250"))
    campaigns_per_wave = int(os.environ.get("MFL_CAMPAIGNS_PER_WAVE", "3"))
    total_rows = int(os.environ.get("MFL_ORDERED_TOTAL_ROWS", "128940"))
    total_batch_slots = (total_rows + batch_size - 1) // batch_size
    cursor_starts = [
        start for start in (
            batch_start_from_plan_artifact(repo, int(r["id"])) for r in direct
        )
        if start is not None and start % (batches_per_campaign) == 0
    ]
    # Artifacts are authoritative; run count is only a bootstrap fallback for
    # legacy runs that never produced a batch plan.
    next_slot = max((start // batches_per_campaign for start in cursor_starts), default=len(direct) - 1) + 1
    exhausted = next_slot * batches_per_campaign >= total_batch_slots
    decision = {
        "mode": "ordered_manifest_cursor", "repo": repo,
        "checked_at": now.isoformat().replace("+00:00", "Z"),
        "scheduler_epoch": epoch.isoformat().replace("+00:00", "Z"),
        "cooldown_minutes": cooldown.total_seconds() / 60,
        "manifest_path": manifest_path, "direct_campaigns_seen": len(direct), "cursor_batch_starts": sorted(set(cursor_starts)), "excluded_control_runs": len([r for r in campaigns if r.get("created_at") and parse_time(r["created_at"]) >= epoch and r.get("conclusion") in unusable]),
        "latest_direct_campaign_created_at": latest_direct.isoformat().replace("+00:00", "Z") if latest_direct else None,
        "quiet_period_elapsed": quiet, "queue_grace_minutes": queue_grace.total_seconds() / 60,
        "queued_direct_runs": [int(r["id"] ) for r in queued_direct],
        "stale_queued_runs": [int(r["id"] ) for r in stale_queued],
        "fresh_queued_runs": [int(r["id"] ) for r in fresh_queued],
        "next_batch_start": next_slot * batches_per_campaign,
        "batch_size": batch_size, "batches_per_campaign": batches_per_campaign,
        "campaigns_per_wave": campaigns_per_wave, "total_rows": total_rows,
        "total_batch_slots": total_batch_slots, "exhausted": exhausted, "dispatched": [],
    }
    if exhausted:
        decision["reason"] = "ordered_manifest_exhausted"
    elif fresh_queued:
        decision["reason"] = "queued_campaigns_pending"
    elif not quiet:
        decision["reason"] = "40_minute_cooldown_not_elapsed"
    else:
        remaining = total_batch_slots - next_slot * batches_per_campaign
        dispatch_count = min(campaigns_per_wave, (remaining + batches_per_campaign - 1) // batches_per_campaign)
        for slot in range(next_slot, next_slot + dispatch_count):
            values = {
                "manifest_path": manifest_path,
                "yahoo_oauth_ref": os.environ.get("MFL_YAHOO_OAUTH_REF", "codex/mfl-gates"),
                "batch_start": str(slot * batches_per_campaign), "batch_count": str(batches_per_campaign),
                "batch_size": str(batch_size), "max_parallel": os.environ.get("MFL_MAX_PARALLEL", "60"),
                "cache_namespace": os.environ.get("MFL_CACHE_NAMESPACE", "mfl-ordered-schedule-v1"),
                "canonical_namespace": os.environ.get("MFL_CANONICAL_NAMESPACE", "mfl-ordered-schedule-canonical-v1"),
                "attempt_suffix": f"-ordered-slot-{slot}", "continue_population": "false",
                "collect_all": "true", "storage_mode": "chunked",
                "target_years": os.environ.get("MFL_TARGET_YEARS", "2000,2001,2002,2003,2004,2005,2006,2007,2008,2009,2010,2011,2012,2013,2014,2015,2016,2017,2018"),
            }
            if os.environ.get("MFL_DRY_RUN", "false").lower() != "true":
                dispatch(repo, campaign_workflow, values)
            decision["dispatched"].append({"slot": slot, "batch_start": int(values["batch_start"]), "batch_count": batches_per_campaign, "attempt_suffix": values["attempt_suffix"]})
        decision["reason"] = "three_fresh_ordered_campaigns_dispatched" if dispatch_count == campaigns_per_wave else "final_ordered_campaigns_dispatched"
    print(json.dumps(decision, sort_keys=True))
    return 0


if __name__ == "__main__":
    sys.exit(main())
