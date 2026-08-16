"""Build one durable status ledger for MFL batch plans and landed league-years."""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def _read_json(path: Path, default: Any) -> Any:
    if not path.is_file():
        return default
    return json.loads(path.read_text(encoding="utf-8"))


def _run_rows(root: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for path in sorted(root.glob("*.json")):
        payload = _read_json(path, [])
        if isinstance(payload, dict):
            payload = payload.get("runs", [])
        for row in payload:
            row = dict(row)
            row.setdefault("workflow", path.stem)
            row["run_id"] = str(row.get("databaseId", row.get("run_id", "")))
            rows.append(row)
    return rows


def _cache_map(path: Path) -> dict[str, dict[str, Any]]:
    payload = _read_json(path, [])
    if isinstance(payload, dict):
        payload = payload.get("caches", [])
    return {str(row.get("key", "")): dict(row) for row in payload if row.get("key")}


def _key(row: dict[str, Any]) -> tuple[int, str]:
    return int(row["season"]), str(row["league_id"])


def build(*, plans_root: Path, wave_plans_root: Path, runs_root: Path, caches_path: Path, canonical_index: Path, output: Path) -> dict[str, Any]:
    runs = _run_rows(runs_root)
    by_run = {str(row["run_id"]): row for row in runs if row.get("run_id")}
    caches = _cache_map(caches_path)
    index = _read_json(canonical_index, {})
    landed_rows = index.get("leagues", []) if isinstance(index, dict) else []
    landed = {_key(row) for row in landed_rows}

    plans: list[dict[str, Any]] = []
    planned_run_ids: set[str] = set()
    for path in sorted(plans_root.glob("*/batch_plan.json")):
        plan = dict(_read_json(path, {}))
        if not plan:
            continue
        run_id = str(plan.get("source_run_id", path.parent.name))
        planned_run_ids.add(run_id)
        run = by_run.get(run_id, {})
        source_keys = dict(plan.get("source_cache_keys", {}))
        present = {name: key in caches for name, key in source_keys.items()}
        run_status = str(run.get("status", "unknown"))
        conclusion = str(run.get("conclusion", ""))
        if conclusion == "success":
            batch_state = "source_validated" if present.get("chunk") and present.get("research_overlay") else "completed_no_source_cache"
        elif conclusion == "cancelled":
            batch_state = "cancelled"
        elif conclusion == "failure":
            batch_state = "failed"
        elif run_status in {"queued", "in_progress"}:
            batch_state = "in_flight"
        else:
            batch_state = "planned"
        planned = [{"season": int(row["season"]), "league_id": str(row["league_id"])} for row in plan.get("planned_league_years", [])]
        planned_keys = {_key(row) for row in planned}
        plans.append({
            **plan,
            "run_status": run_status,
            "conclusion": conclusion,
            "batch_state": batch_state,
            "source_cache_present": present,
            "planned_count": len(planned),
            "landed_count": len(planned_keys & landed),
            "not_landed_count": len(planned_keys - landed),
        })

    future_batches: list[dict[str, Any]] = []
    for path in sorted(wave_plans_root.glob("*/next_wave_plan.json")):
        wave = dict(_read_json(path, {}))
        if not wave:
            continue
        planner_id = str(wave.get("source_run_id", path.parent.name))
        campaign_count = int(wave.get("campaign_count", 0))
        batches_per_campaign = int(wave.get("batches_per_campaign", 0))
        batch_size = int(wave.get("batch_size", 0))
        rows = [{"season": int(row["season"]), "league_id": str(row["league_id"])}
                for row in wave.get("planned_league_years", [])]
        span = batches_per_campaign * batch_size
        for campaign in range(campaign_count):
            assigned = rows[campaign * span:(campaign + 1) * span]
            future_batches.append({
                "source_run_id": f"future:{planner_id}:c{campaign}",
                "planner_run_id": planner_id,
                "campaign_slot": campaign,
                "batch_state": "planned",
                "planned_count": len(assigned),
                "landed_count": sum(_key(row) in landed for row in assigned),
                "not_landed_count": sum(_key(row) not in landed for row in assigned),
                "planned_league_years": assigned,
            })

    league_status: dict[tuple[int, str], dict[str, Any]] = {
        key: {"season": key[0], "league_id": key[1], "status": "landed"} for key in landed
    }
    actual_planned_keys = {
        _key(row)
        for plan in plans
        for row in plan.get("planned_league_years", [])
    }
    for plan in future_batches:
        for row in plan["planned_league_years"]:
            key = _key(row)
            if key in landed or key in actual_planned_keys:
                continue
            league_status[key] = {
                "season": key[0], "league_id": key[1], "status": "planned",
                "planner_run_id": plan["planner_run_id"],
                "campaign_slot": plan["campaign_slot"],
            }
    for plan in plans:
        for row in plan.get("planned_league_years", []):
            key = _key(row)
            if key in landed:
                continue
            state = plan["batch_state"]
            status = {
                "source_validated": "source_validated_not_promoted",
                "failed": "failed",
                "cancelled": "cancelled",
                "in_flight": "in_flight",
            }.get(state, "planned")
            league_status[key] = {
                "season": key[0], "league_id": key[1], "status": status,
                "source_run_id": plan.get("source_run_id"),
            }

    accepted_by_year = index.get("accepted_by_year", {}) if isinstance(index, dict) else {}
    unplanned_campaign_runs = [
        {
            "run_id": row["run_id"],
            "status": row.get("status", "unknown"),
            "conclusion": row.get("conclusion", ""),
            "created_at": row.get("createdAt"),
            "updated_at": row.get("updatedAt"),
            "note": "Run predates durable batch-plan artifacts; batch status is tracked, but exact planned league-years are not reconstructed.",
        }
        for row in runs
        if row.get("workflow") == "mfl_register_batch_campaign"
        and row.get("run_id") not in planned_run_ids
    ]

    result = {
        "schema_version": "mfl_batch_ledger_v1",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "canonical": {
            "league_count": len(landed_rows),
            "accepted_by_year": accepted_by_year,
            "index_cache_key": next((key for key in caches if "-index-" in key and key.startswith("mfl-registered-")), None),
            "schema_unchanged_required": True,
            "new_lineage_required": False,
        },
        "runs_observed": len(runs),
        "plans_observed": len(plans),
        "future_wave_plans_observed": len({row["planner_run_id"] for row in future_batches}),
        "unplanned_campaign_runs": unplanned_campaign_runs,
        "batches": plans,
        "future_batches": future_batches,
        "league_years": sorted(league_status.values(), key=lambda row: (row["season"], row["league_id"])),
        "coverage": {
            "landed": sum(1 for row in league_status.values() if row["status"] == "landed"),
            "source_validated_not_promoted": sum(1 for row in league_status.values() if row["status"] == "source_validated_not_promoted"),
            "in_flight": sum(1 for row in league_status.values() if row["status"] == "in_flight"),
            "planned": sum(1 for row in league_status.values() if row["status"] == "planned"),
            "failed": sum(1 for row in league_status.values() if row["status"] == "failed"),
            "cancelled": sum(1 for row in league_status.values() if row["status"] == "cancelled"),
            "unplanned_campaign_runs": len(unplanned_campaign_runs),
        },
    }
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(result, indent=2, sort_keys=True), encoding="utf-8")
    return result


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--plans-root", type=Path, required=True)
    parser.add_argument("--wave-plans-root", type=Path, required=True)
    parser.add_argument("--runs-root", type=Path, required=True)
    parser.add_argument("--caches", type=Path, required=True)
    parser.add_argument("--canonical-index", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    result = build(plans_root=args.plans_root, wave_plans_root=args.wave_plans_root, runs_root=args.runs_root, caches_path=args.caches, canonical_index=args.canonical_index, output=args.output)
    print(json.dumps({"plans_observed": result["plans_observed"], "coverage": result["coverage"]}, sort_keys=True))


if __name__ == "__main__":
    main()
