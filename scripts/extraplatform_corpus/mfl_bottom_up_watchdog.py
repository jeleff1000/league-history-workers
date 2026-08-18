#!/usr/bin/env python3
"""Dispatch the next reserved MFL wave after a quiet period."""
from __future__ import annotations

import json
import os
import subprocess
import sys
from datetime import datetime, timedelta, timezone

ACTIVE = {"queued", "in_progress", "waiting", "requested", "pending"}


def gh_json(args: list[str]) -> dict:
    result = subprocess.run(["gh", "api", *args], check=True, capture_output=True, text=True)
    return json.loads(result.stdout)


def parse_time(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def workflow_runs(repo: str, workflow: str) -> list[dict]:
    payload = gh_json([f"repos/{repo}/actions/workflows/{workflow}/runs", "--method", "GET", "-f", "per_page=100"])
    return payload.get("workflow_runs", [])


def dispatch(repo: str, workflow: str, values: dict[str, str]) -> None:
    command = ["gh", "workflow", "run", workflow, "--repo", repo, "--ref", "main"]
    for key, value in values.items():
        command.extend(["-f", f"{key}={value}"])
    subprocess.run(command, check=True)


def main() -> int:
    repo = os.environ.get("MFL_REPO", "league-history-workers/mfl-league-fetcher")
    campaign_workflow = os.environ.get("MFL_CAMPAIGN_WORKFLOW", "mfl_register_batch_campaign.yml")
    planner_workflow = os.environ.get("MFL_PLANNER_WORKFLOW", "mfl_plan_next_wave.yml")
    cooldown = timedelta(minutes=int(os.environ.get("MFL_COOLDOWN_MINUTES", "30")))
    stale_after = timedelta(minutes=int(os.environ.get("MFL_STALE_CAMPAIGN_MINUTES", "180")))
    now = datetime.now(timezone.utc)
    campaigns = workflow_runs(repo, campaign_workflow)
    planners = workflow_runs(repo, planner_workflow)
    active_campaigns = [r for r in campaigns if r.get("status") in ACTIVE]
    stale_active_campaigns = [
        r for r in active_campaigns
        if r.get("created_at") and now - parse_time(r["created_at"]) >= stale_after
    ]
    blocking_active_campaigns = [r for r in active_campaigns if r not in stale_active_campaigns]
    active_planners = [r for r in planners if r.get("status") in ACTIVE]
    created = [parse_time(r["created_at"]) for r in campaigns if r.get("created_at")]
    latest_campaign = max(created, default=None)
    quiet = latest_campaign is None or now - latest_campaign >= cooldown
    decision = {
        "repo": repo,
        "checked_at": now.isoformat().replace("+00:00", "Z"),
        "cooldown_minutes": cooldown.total_seconds() / 60,
        "stale_campaign_minutes": stale_after.total_seconds() / 60,
        "latest_campaign_created_at": latest_campaign.isoformat().replace("+00:00", "Z") if latest_campaign else None,
        "active_campaigns": [r.get("id") for r in active_campaigns],
        "stale_active_campaigns": [r.get("id") for r in stale_active_campaigns],
        "blocking_active_campaigns": [r.get("id") for r in blocking_active_campaigns],
        "active_planners": [r.get("id") for r in active_planners],
        "quiet_period_elapsed": quiet,
        "dispatched": False,
    }
    if blocking_active_campaigns:
        decision["reason"] = "campaign_runs_active_or_queued"
    elif active_planners:
        decision["reason"] = "planner_run_active_or_queued"
    elif not quiet:
        decision["reason"] = "campaign_cooldown_not_elapsed"
    else:
        values = {
            "campaign_count": "3",
            "batches_per_campaign": "250",
            "batch_size": "2",
            "max_parallel": "75",
            "target_years": "2004,2005,2006,2007,2008,2009,2010,2011,2012,2013,2014,2015,2016,2017,2018",
            "campaign_cutoff": "2000-01-01T00:00:00Z",
        }
        decision["dispatch_inputs"] = values
        if os.environ.get("MFL_DRY_RUN", "false").lower() == "true":
            decision["reason"] = "eligible_dry_run"
        else:
            dispatch(repo, planner_workflow, values)
            decision["dispatched"] = True
            decision["reason"] = "three_campaign_planner_dispatched"
    print(json.dumps(decision, sort_keys=True))
    return 0


if __name__ == "__main__":
    sys.exit(main())
