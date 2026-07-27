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


def validate_playoff_clutch_materialization(db_path: Path, season: int) -> None:
    """Require the corpus contract's playoff and clutch outputs before folding."""
    import duckdb

    con = duckdb.connect(str(db_path), read_only=True)
    try:
        playoff_rows = int(
            con.execute(
                "SELECT COUNT(*) FROM public.matchup WHERE year = ? "
                "AND (COALESCE(CAST(is_playoffs AS INTEGER), 0) = 1 "
                "OR COALESCE(CAST(champion AS INTEGER), 0) = 1)",
                [season],
            ).fetchone()[0]
        )
        if playoff_rows <= 0:
            raise SourceValidationError("playoff rows were not materialized", code="PlayoffEmpty")
        clutch_rows = int(
            con.execute(
                "SELECT COUNT(*) FROM public.player_fantasy WHERE year = ? "
                "AND clutch_equity IS NOT NULL",
                [season],
            ).fetchone()[0]
        )
        if clutch_rows <= 0:
            raise SourceValidationError("clutch equity was not materialized", code="ClutchEmpty")
    finally:
        con.close()


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
TRACEBACK_SITE = re.compile(
    r'(?m)^\s*File "[^"\r\n]*[\\/](?P<file>[A-Za-z0-9_]+)\.py", line (?P<line>\d+)',
)
SAFE_FAILURE_MARKERS = (
    ("flyreader is disabled in corpus mode", "CorpusFlyReadAttempt"),
    ("flytarget is disabled in corpus mode", "CorpusFlyWriteAttempt"),
    ("flywriter is disabled in corpus mode", "CorpusFlyWriteAttempt"),
    ("failed to flatten league_settings", "SettingsCanonicalization"),
    ("fetched league settings but produced 0 canonical rows", "EmptyCanonicalSettings"),
    ("local duckdb failed pre-upload validation", "PreUploadValidation"),
    ("no oauth credentials available in context", "OAuthMaterialization"),
    ("could not create shared oauth session", "SharedOAuthCreateFailed"),
    ("error creating oauth from context", "OAuthContextMaterialization"),
    ("no leagues found for", "YahooLeagueLookup"),
    ("failed to fetch league_ids for", "YahooLeagueLookup"),
    ("failed to create league object", "YahooLeagueObject"),
    ("no matchup data found", "YahooMatchupEmpty"),
    ("conn is required - all pipeline work uses local duckdb files", "LocalDatabaseContract"),
    ("get_pipeline_connection() requires data_dir", "LocalDatabaseContract"),
    ("sql enrichments failed", "SqlEnrichmentFailure"),
    ("fantasy aggregation failed after sql enrichments", "FantasyAggregationFailure"),
    ("yahoo_fantasy_api not available", "YahooDependencyMissing"),
    ("runlogger not available or not callable", "PipelineDependencyMissing"),
    # Catch-all for the pipeline's zero-settings hard fail. Kept LAST so any
    # more specific cause logged earlier (denied, auth, lookup) wins.
    ("league settings unavailable for every requested year", "SettingsUnavailable"),
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
    """Build an ephemeral one-year quick-import LeagueContext payload."""
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
        # Match the production Yahoo quick-import contract: one season, only
        # the league's rostered data, and the same playoff/clutch follow-up
        # materialization.  This keeps the fleet runner fast while preserving
        # the complete season-level record we fold into the local lake.
        "import_mode": "quick",
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
    if "request denied" in lowered:
        # Yahoo 999 "Request Denied" is throttling, not a permanent failure --
        # cool the grant down and retry rather than burning the task.
        return "rate_limited", "YahooRequestDenied"
    if any(
        marker in lowered
        for marker in (
            "schedule shell with no played matchups",
            "no played yahoo matchups found",
        )
    ):
        # Renewed shell whose season never happened: the import correctly
        # produces an empty database. Not a failure and never retryable.
        return "skipped", "UnplayedSeason"
    if any(marker in lowered for marker in ("invalid_grant", "token expired", "http 401", "401 client", "unauthorized", "access denied")):
        return "failure", "YahooAuthentication"
    if any(marker in lowered for marker in ("http 403", "403 client", "forbidden")):
        return "failure", "YahooAuthorization"
    if any(marker in lowered for marker in ("http 404", "404 client", "not found")):
        return "failure", "YahooNotFound"
    for marker, category in SAFE_FAILURE_MARKERS:
        if marker in lowered:
            return "failure", category
    matches = EXCEPTION_CLASS.findall(raw_text)
    if matches:
        exception_class = matches[-1].rsplit(".", 1)[-1]
        if exception_class == "RuntimeError":
            sites = list(TRACEBACK_SITE.finditer(raw_text))
            if sites:
                site = sites[-1]
                return (
                    "failure",
                    f"RuntimeSite-{site.group('file')}-L{site.group('line')}",
                )
        return "failure", exception_class
    return "failure", "ImportSubprocessFailure"


def _atomic_json(path: Path, payload: Any) -> None:
    clean = sanitize_payload(payload)
    temp = path.with_suffix(path.suffix + ".tmp")
    temp.write_text(json.dumps(clean, indent=2, sort_keys=True), encoding="utf-8")
    temp.replace(path)


def _report(
    plan: Plan,
    outcomes: list[dict[str, Any]],
    scheduler: CredentialScheduler,
    *,
    completed_this_run: int = 0,
) -> dict[str, Any]:
    successes = sum(row.get("status") == "success" for row in outcomes)
    failures = sum(row.get("status") == "failure" for row in outcomes)
    rate_limits = sum(row.get("status") == "rate_limited" for row in outcomes)
    skips = sum(row.get("status") == "skipped" for row in outcomes)
    pending = sum(
        row.task_id not in scheduler.completed and row.task_id not in scheduler.failed
        for row in scheduler.candidates.values()
    )
    return {
        "mode": plan.mode,
        "requested": plan.requested,
        "skipped": skips,
        "planned": len(plan.tasks),
        "successes": successes,
        "failures": failures,
        "rate_limits": rate_limits,
        "pending": pending,
        "completed_this_run": completed_this_run,
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
    sleeper: Callable[[float], None] = time.sleep,
    max_rate_limit_retries: int = 4,
    time_budget_seconds: float | None = None,
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
    rate_limit_counts: dict[str, int] = {}
    started_at = clock()
    while True:
        now = clock()
        if time_budget_seconds is not None and now - started_at >= time_budget_seconds:
            # Stop dispatching so the job ends cleanly inside its CI timeout;
            # pending tasks resume on the next run via the landed list/ledger.
            break
        candidate = scheduler.next(now)
        if candidate is None:
            # Tasks blocked only by a grant cooldown are retryable: wait out
            # the earliest expiry instead of abandoning them mid-run.
            wake_at = scheduler.next_ready_time(now)
            if wake_at is None:
                break
            if time_budget_seconds is not None and wake_at - started_at >= time_budget_seconds:
                break
            sleeper(max(0.0, wake_at - now) + 1.0)
            continue
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
            retries = rate_limit_counts.get(candidate.task_id, 0) + 1
            rate_limit_counts[candidate.task_id] = retries
            if retries > max_rate_limit_retries:
                scheduler.fail(candidate.task_id, outcome.error_class or "RateLimitExhausted")
            else:
                # Escalate: 120s, 240s, 480s, 960s -- Yahoo throttling windows
                # outlast a flat two-minute wait after a heavy discovery phase.
                scheduler.rate_limit(
                    candidate.task_id, now=now, cooldown_seconds=120 * (2 ** (retries - 1))
                )
        else:
            scheduler.fail(candidate.task_id, outcome.error_class or "TaskFailure")
        ledger = {"scheduler": scheduler.to_dict(), "outcomes": outcomes}
        _atomic_json(ledger_path, ledger)
        _atomic_json(
            output_dir / "report.json",
            _report(plan, outcomes, scheduler, completed_this_run=completed_this_run),
        )
        if stop_after is not None and completed_this_run >= stop_after:
            break
    report = _report(plan, outcomes, scheduler, completed_this_run=completed_this_run)
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
        "--quick",
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
            if status == "rate_limited":
                stage = "acquisition"
            elif status == "skipped":
                stage = "acquisition"
            elif error_class == "YahooAuthentication":
                stage = "authentication"
            else:
                stage = "import"
            outcome = TaskOutcome(task.task_id, status, stage, error_class)
            return outcome
        # Keep the same canonical post-import sequence as the production quick
        # worker, locally and idempotently: playoff odds first, aggregation
        # second, and clutch materialization last.  The quick importer may
        # already have produced some of these outputs; rerunning the shared
        # steps makes the lake contract deterministic without touching Fly.
        engine_scripts = (
            ("playoff_sim", pipeline_root / "fantasy_football_data_scripts" / "multi_league" / "transformations" / "matchup" / "playoff_odds_import.py"),
            ("aggregate", pipeline_root / "fantasy_football_data_scripts" / "multi_league" / "transformations" / "aggregation" / "aggregate_fantasy_context.py"),
            ("clutch", pipeline_root / "fantasy_football_data_scripts" / "multi_league" / "transformations" / "player" / "clutch_to_player.py"),
        )
        for stage_name, script in engine_scripts:
            engine = run_command(
                [sys.executable, str(script), "--db", db_name, "--data-dir", str(task_dir)],
                cwd=str(pipeline_root),
                env=build_subprocess_env(base_env),
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                timeout=3_600,
                check=False,
            )
            if engine.returncode != 0:
                status, error_class = classify_import_failure(engine.stdout or "")
                outcome = TaskOutcome(task.task_id, "failure", stage_name, error_class)
                return outcome
        db_path = task_dir / f"{db_name}.duckdb"
        validate_source(db_path, task)
        validate_playoff_clutch_materialization(db_path, task.season)
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
    except SourceValidationError as exc:
        outcome = TaskOutcome(task.task_id, "failure", "validation", exc.code)
        return outcome
    except Exception as exc:
        outcome = TaskOutcome(task.task_id, "failure", "runtime", type(exc).__name__)
        return outcome
    finally:
        _append_sanitized_log(sanitized_log, outcome, task)
        shutil.rmtree(task_dir, ignore_errors=True)
