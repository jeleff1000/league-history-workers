"""One-year Yahoo corpus import helpers and privacy gates."""

from __future__ import annotations

import os
import re
import json
import shutil
import subprocess
import sys
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Callable, Mapping

from scripts.yahoo_corpus.inventory import Grant
from scripts.yahoo_corpus.planner import Plan
from scripts.yahoo_corpus.scheduler import Candidate, CredentialScheduler
from scripts.yahoo_corpus.validation import SourceValidationError, validate_source
from scripts.sleeper_corpus.build_corpus_snapshot import fold_database, open_snapshot


FLY_SECRET_KEYS = {
    "DATABASE_SERVER_URL",
    "DATABASE_READ_TOKEN",
    "DATABASE_WRITE_TOKEN",
    "DATABASE_ADMIN_TOKEN",
    "FLY_API_TOKEN",
    "FLY_PRIMARY_MACHINE_ID",
    "MOTHERDUCK_TOKEN",
}
FORBIDDEN_KEYS = {
    "access_token",
    "refresh_token",
    "oauth_credentials",
    "consumer_key",
    "consumer_secret",
    "client_secret",
    "manager",
    "manager_name",
    "manager_guid",
    "team_name",
    "league_name",
    "source_databases",
}
TOKEN_VALUE = re.compile(r"(?i)\b(?:bearer\s+)?[a-z0-9_-]{32,}\b")
EXCEPTION_CLASS = re.compile(
    r"(?m)^([A-Za-z_][A-Za-z0-9_.]*(?:Error|Exception)):\s*",
)
SAFE_FAILURE_MARKERS = (
    ("flyreader is disabled in corpus mode", "CorpusFlyReadAttempt"),
    ("flytarget is disabled in corpus mode", "CorpusFlyWriteAttempt"),
    ("flywriter is disabled in corpus mode", "CorpusFlyWriteAttempt"),
    ("failed to flatten league_settings", "SettingsCanonicalization"),
    ("fetched league settings but produced 0 canonical rows", "EmptyCanonicalSettings"),
    ("local duckdb failed pre-upload validation", "PreUploadValidation"),
)


class ForbiddenArtifactData(ValueError):
    """Raised when a public artifact payload contains private data."""


@dataclass(frozen=True)
class TaskOutcome:
    task_id: str
    status: str
    stage: str
    error_class: str | None


def _safe_db_name(task_id: str) -> str:
    suffix = task_id.removeprefix("yahoo-")
    suffix = re.sub(r"[^a-zA-Z0-9_]", "_", suffix).strip("_").lower()
    return f"smpl_yahoo_{suffix or 'task'}"


def build_context(
    task: Candidate,
    grant: Grant,
    data_dir: Path,
    *,
    client_id: str,
    client_secret: str,
) -> dict[str, Any]:
    """Build an ephemeral one-year full-import LeagueContext payload."""
    resolved = data_dir.resolve()
    return {
        "league_id": task.league_key,
        "league_name": task.task_id,
        "oauth_file_path": None,
        "oauth_credentials": {
            "access_token": "expired",
            "refresh_token": grant.refresh_token,
            "consumer_key": client_id,
            "consumer_secret": client_secret,
            "token_time": 0,
            "token_type": "bearer",
        },
        "game_code": "nfl",
        "start_year": task.season,
        "end_year": task.season,
        "league_ids": {str(task.season): task.league_key},
        "data_directory": str(resolved),
        "database_name": _safe_db_name(task.task_id),
        "import_mode": "full",
        "require_oauth": True,
        "rate_limit_per_sec": 1.0,
        "max_workers": 1,
    }


def build_subprocess_env(base_env: Mapping[str, str] | None = None) -> dict[str, str]:
    """Construct an offline import environment with every Fly credential removed."""
    env = dict(os.environ if base_env is None else base_env)
    for key in FLY_SECRET_KEYS:
        env.pop(key, None)
    env["CORPUS_MODE"] = "1"
    return env


def sanitize_payload(value: Any) -> Any:
    """Return a JSON-safe payload after recursively enforcing the artifact allow boundary."""
    if isinstance(value, Mapping):
        sanitized: dict[str, Any] = {}
        for raw_key, item in value.items():
            key = str(raw_key)
            if key.lower() in FORBIDDEN_KEYS:
                raise ForbiddenArtifactData(f"forbidden artifact key: {key}")
            sanitized[key] = sanitize_payload(item)
        return sanitized
    if isinstance(value, (list, tuple)):
        return [sanitize_payload(item) for item in value]
    if isinstance(value, str) and TOKEN_VALUE.search(value):
        raise ForbiddenArtifactData("artifact value resembles an OAuth token")
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    return str(value)


def classify_import_failure(raw_text: str) -> tuple[str, str]:
    """Classify a private import log while returning no provider/message content."""
    lowered = raw_text.lower()
    if any(marker in lowered for marker in ("rate limit", "rate_limited", "http 429", " 429")):
        return "rate_limited", "YahooRateLimit"
    if any(marker in lowered for marker in ("invalid_grant", "token expired", "http 401")):
        return "failure", "YahooAuthentication"
    for marker, category in SAFE_FAILURE_MARKERS:
        if marker in lowered:
            return "failure", category
    matches = EXCEPTION_CLASS.findall(raw_text)
    if matches:
        return "failure", matches[-1].rsplit(".", 1)[-1]
    return "failure", "ImportSubprocessFailure"


def _atomic_json(path: Path, payload: Any) -> None:
    clean = sanitize_payload(payload)
    temp = path.with_suffix(path.suffix + ".tmp")
    temp.write_text(json.dumps(clean, indent=2, sort_keys=True), encoding="utf-8")
    temp.replace(path)


def _report(plan: Plan, outcomes: list[dict[str, Any]], scheduler: CredentialScheduler) -> dict[str, Any]:
    successes = sum(row.get("status") == "success" for row in outcomes)
    failures = sum(row.get("status") == "failure" for row in outcomes)
    rate_limits = sum(row.get("status") == "rate_limited" for row in outcomes)
    return {
        "mode": plan.mode,
        "requested": plan.requested,
        "planned": len(plan.tasks),
        "successes": successes,
        "failures": failures,
        "rate_limits": rate_limits,
        "dispatch_trace": list(scheduler.dispatch_trace),
        "outcomes": outcomes,
    }


def run_plan(
    plan: Plan,
    grants: Mapping[str, Grant],
    output_dir: Path,
    *,
    execute_task: Callable[[Candidate, Grant], TaskOutcome],
    stop_after: int | None = None,
    resume: bool = False,
    clock: Callable[[], float] = time.monotonic,
) -> dict[str, Any]:
    """Execute a credential-aware plan and persist only redacted resumable state."""
    output_dir.mkdir(parents=True, exist_ok=True)
    ledger_path = output_dir / "ledger.json"
    previous: dict[str, Any] = {}
    if resume and ledger_path.exists():
        previous = json.loads(ledger_path.read_text(encoding="utf-8"))
    scheduler = CredentialScheduler(plan.tasks, state=previous.get("scheduler"))
    outcomes = list(previous.get("outcomes", []))
    completed_this_run = 0
    while True:
        now = clock()
        candidate = scheduler.next(now)
        if candidate is None:
            break
        grant = grants.get(candidate.grant_id)
        if grant is None:
            outcome = TaskOutcome(candidate.task_id, "failure", "credential", "MissingGrant")
        else:
            outcome = execute_task(candidate, grant)
        outcome_row = {
            **asdict(outcome),
            "grant_id": candidate.grant_id,
            "season": candidate.season,
            "era": candidate.era,
            "cohort_slug": candidate.cohort_slug,
        }
        outcomes.append(sanitize_payload(outcome_row))
        if outcome.status == "success":
            scheduler.complete(candidate.task_id)
            completed_this_run += 1
        elif outcome.status == "rate_limited":
            scheduler.rate_limit(candidate.task_id, now=now, cooldown_seconds=120)
        else:
            scheduler.fail(candidate.task_id, outcome.error_class or "TaskFailure")
        ledger = {"scheduler": scheduler.to_dict(), "outcomes": outcomes}
        _atomic_json(ledger_path, ledger)
        _atomic_json(output_dir / "report.json", _report(plan, outcomes, scheduler))
        if stop_after is not None and completed_this_run >= stop_after:
            break
    report = _report(plan, outcomes, scheduler)
    _atomic_json(output_dir / "report.json", report)
    return report


def _append_sanitized_log(path: Path, outcome: TaskOutcome, task: Candidate) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    error = outcome.error_class or "none"
    with path.open("a", encoding="utf-8") as handle:
        handle.write(
            f"[{task.task_id}] season={task.season} status={outcome.status} "
            f"stage={outcome.stage} error_class={error}\n"
        )


def execute_import_task(
    task: Candidate,
    grant: Grant,
    *,
    pipeline_root: Path,
    work_root: Path,
    snapshot_path: Path,
    sanitized_log: Path,
    client_id: str,
    client_secret: str,
    run_command: Callable[..., subprocess.CompletedProcess[Any]] = subprocess.run,
    base_env: Mapping[str, str] | None = None,
) -> TaskOutcome:
    """Run, validate, fold, and erase one private Yahoo league-year import."""
    task_dir = work_root / task.task_id
    if task_dir.exists():
        shutil.rmtree(task_dir)
    task_dir.mkdir(parents=True, exist_ok=True)
    context_path = task_dir / "league_context.json"
    private_log = task_dir / "import.log"
    context = build_context(
        task,
        grant,
        task_dir,
        client_id=client_id,
        client_secret=client_secret,
    )
    context_path.write_text(json.dumps(context), encoding="utf-8")
    db_name = str(context["database_name"])
    command = [
        sys.executable,
        str(pipeline_root / "fantasy_football_data_scripts" / "initial_import_v3.py"),
        "--context",
        str(context_path),
        "--data-dir",
        str(task_dir),
        "--skip-track-1",
        "--skip-track-2-upload",
    ]
    outcome = TaskOutcome(task.task_id, "failure", "import", "ImportFailure")
    try:
        with private_log.open("w", encoding="utf-8") as raw_log:
            result = run_command(
                command,
                cwd=str(pipeline_root),
                env=build_subprocess_env(base_env),
                stdout=raw_log,
                stderr=subprocess.STDOUT,
                text=True,
                timeout=10_800,
                check=False,
            )
        if result.returncode != 0:
            raw_text = private_log.read_text(encoding="utf-8", errors="replace")
            status, error_class = classify_import_failure(raw_text)
            stage = "acquisition" if status == "rate_limited" else "authentication" if error_class == "YahooAuthentication" else "import"
            outcome = TaskOutcome(task.task_id, status, stage, error_class)
            return outcome
        db_path = task_dir / f"{db_name}.duckdb"
        validate_source(db_path, task)
        snapshot = open_snapshot(snapshot_path)
        try:
            ok, message = fold_database(
                snapshot,
                db_path,
                db_name,
                anonymize_identity=True,
            )
        finally:
            snapshot.close()
        if not ok:
            outcome = TaskOutcome(task.task_id, "failure", "fold", "FoldFailure")
            return outcome
        outcome = TaskOutcome(task.task_id, "success", "complete", None)
        return outcome
    except subprocess.TimeoutExpired:
        outcome = TaskOutcome(task.task_id, "failure", "import", "ImportTimeout")
        return outcome
    except SourceValidationError:
        outcome = TaskOutcome(task.task_id, "failure", "validation", "SourceValidationError")
        return outcome
    except Exception as exc:
        outcome = TaskOutcome(task.task_id, "failure", "runtime", type(exc).__name__)
        return outcome
    finally:
        _append_sanitized_log(sanitized_log, outcome, task)
        shutil.rmtree(task_dir, ignore_errors=True)
