#!/usr/bin/env python3
"""Plan and execute private Yahoo corpus pilots on a GitHub Actions worker."""

from __future__ import annotations

import argparse
import importlib.util
import json
import os
import sys
import time
from collections import Counter, defaultdict
from pathlib import Path
from types import ModuleType
from typing import Iterable

from scripts.yahoo_corpus.inventory import Grant, YahooGrantAdapter, discover_candidates, load_grants
from scripts.yahoo_corpus.planner import Plan, build_plan
from scripts.yahoo_corpus.runner import _atomic_json, execute_import_task, run_plan
from scripts.yahoo_corpus.scheduler import Candidate, ERAS


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", choices=("cross-era", "spacing-resume"), default="cross-era")
    parser.add_argument("--task-limit", type=int, default=12)
    parser.add_argument("--pipeline-root", type=Path, default=Path("code"))
    parser.add_argument("--output", type=Path, default=Path("corpus/yahoo-pilot"))
    parser.add_argument("--plan-only", action="store_true")
    parser.add_argument("--resume", action="store_true")
    parser.add_argument("--stop-after", type=int)
    parser.add_argument("--delay-ms", type=int, default=150)
    parser.add_argument("--global-delay-ms", type=int, default=1000)
    parser.add_argument("--request-budget", type=int, default=10_000)
    return parser


def write_plan(path: Path, plan: Plan) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    _atomic_json(path, plan.to_dict())


def load_plan(path: Path) -> Plan:
    payload = json.loads(path.read_text(encoding="utf-8"))
    tasks = tuple(
        Candidate(
            task_id=row["task_id"],
            grant_id=row["grant_id"],
            league_key=row["league_key"],
            season=int(row["season"]),
            cohort_slug=row["cohort_slug"],
            lineage_id=row["lineage_id"],
        )
        for row in payload.get("tasks", [])
    )
    return Plan(mode=payload["mode"], requested=int(payload["requested"]), tasks=tasks)


def pilot_ready(mode: str, candidates: Iterable[Candidate], limit: int) -> bool:
    rows = list(candidates)
    if mode == "cross-era":
        base, remainder = divmod(limit, len(ERAS))
        era_targets = {
            label: base + (1 if index < remainder else 0)
            for index, (label, _, _) in enumerate(ERAS)
        }
        counts = Counter(row.era for row in rows)
        return (
            len({row.grant_id for row in rows}) >= limit
            and all(counts[label] >= target for label, target in era_targets.items())
        )
    if mode == "spacing-resume":
        per_grant: dict[str, set[int]] = defaultdict(set)
        for row in rows:
            per_grant[row.grant_id].add(row.season)
        needed_grants = max(1, limit // 3)
        return sum(len(years) >= 3 for years in per_grant.values()) >= needed_grants
    raise ValueError(f"Unknown pilot mode: {mode}")


def _load_module(path: Path, name: str) -> ModuleType:
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Cannot load private runtime module: {path.name}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


def _encryption_keys() -> list[str]:
    return [
        value
        for value in (
            os.environ.get("CREDENTIAL_ENCRYPTION_KEY"),
            os.environ.get("CREDENTIAL_ENCRYPTION_KEY_NEW"),
        )
        if value
    ]


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    pipeline_root = args.pipeline_root.resolve()
    fantasy_root = pipeline_root / "fantasy_football_data_scripts"
    for path in (pipeline_root, fantasy_root):
        if str(path) not in sys.path:
            sys.path.insert(0, str(path))

    from multi_league.core.readers.fly_reader import FlyReader
    from multi_league.utils.credential_store import decrypt_token_with_key_rotation

    keys = _encryption_keys()
    if not keys:
        raise RuntimeError("CREDENTIAL_ENCRYPTION_KEY is not set")
    client_id = os.environ.get("YAHOO_CLIENT_ID", "")
    client_secret = os.environ.get("YAHOO_CLIENT_SECRET", "")
    if not client_id or not client_secret:
        raise RuntimeError("Yahoo application credentials are not set")

    grants = load_grants(
        FlyReader(),
        keys,
        decryptor=decrypt_token_with_key_rotation,
    )
    grant_map = {row.grant_id: row for row in grants}
    args.output.mkdir(parents=True, exist_ok=True)
    plan_path = args.output / "plan.json"

    if args.resume and plan_path.exists():
        plan = load_plan(plan_path)
    else:
        client_module = _load_module(
            pipeline_root / "scripts" / "yahoo_corpus" / "client.py",
            "worker_private_yahoo_client",
        )
        parser_module = _load_module(
            pipeline_root / "scripts" / "yahoo_corpus" / "settings_parser.py",
            "worker_private_yahoo_settings_parser",
        )

        def adapter_factory(grant: Grant) -> YahooGrantAdapter:
            client = client_module.YahooCensusClient(
                client_id,
                client_secret,
                grant.refresh_token,
                request_budget=args.request_budget,
                delay_ms=args.delay_ms,
            )
            return YahooGrantAdapter(
                grant,
                client,
                parse_xml=parser_module.parse_settings_xml,
                classify=parser_module.classify_settings,
                renewal_keys=parser_module.extract_renewal_keys,
            )

        candidates, _ = discover_candidates(
            grants,
            adapter_factory=adapter_factory,
            stop_when=lambda rows: pilot_ready(args.mode, rows, args.task_limit),
        )
        plan = build_plan(args.mode, candidates, limit=args.task_limit)
        write_plan(plan_path, plan)

    print(
        json.dumps(
            {
                "mode": plan.mode,
                "requested": plan.requested,
                "planned": len(plan.tasks),
                "shortfall": plan.shortfall,
                "eras": dict(Counter(row.era for row in plan.tasks)),
                "distinct_grants": len({row.grant_id for row in plan.tasks}),
            },
            sort_keys=True,
        )
    )
    if args.plan_only:
        return 0 if plan.tasks else 1
    if not plan.tasks:
        return 1

    private_root = args.output / ".private"
    snapshot_path = args.output / "yahoo_corpus_slice.duckdb"
    sanitized_log = args.output / "driver.log"

    def executor(task: Candidate, grant: Grant):
        outcome = execute_import_task(
            task,
            grant,
            pipeline_root=pipeline_root,
            work_root=private_root,
            snapshot_path=snapshot_path,
            sanitized_log=sanitized_log,
            client_id=client_id,
            client_secret=client_secret,
        )
        if args.global_delay_ms > 0:
            time.sleep(args.global_delay_ms / 1000)
        return outcome

    report = run_plan(
        plan,
        grant_map,
        args.output,
        execute_task=executor,
        stop_after=args.stop_after,
        resume=args.resume,
    )
    shutil_target = private_root
    if shutil_target.exists():
        import shutil

        shutil.rmtree(shutil_target, ignore_errors=True)
    if report["successes"] == 0:
        return 1
    if report["failures"] > report["successes"]:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
