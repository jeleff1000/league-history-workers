from __future__ import annotations

import json
import subprocess
from pathlib import Path

import pytest
import scripts.yahoo_corpus.runner as runner_module

from scripts.yahoo_corpus.inventory import Grant
from scripts.yahoo_corpus.runner import (
    ForbiddenArtifactData,
    TaskOutcome,
    build_context,
    build_subprocess_env,
    classify_import_failure,
    execute_import_task,
    run_plan,
    sanitize_payload,
)
from scripts.yahoo_corpus.planner import Plan
from scripts.yahoo_corpus.scheduler import Candidate
from scripts.yahoo_corpus.validation import SourceValidationError


def task() -> Candidate:
    return Candidate(
        task_id="yahoo-abcd",
        grant_id="grant-1234",
        league_key="423.l.999",
        season=2023,
        cohort_slug="10t_flx_half_4pt",
        lineage_id="423.l.999",
    )


def test_build_context_targets_exactly_one_full_import_year(tmp_path: Path) -> None:
    grant = Grant("grant-1234", "private-refresh", ("423.l.999",), ("customer",))

    context = build_context(
        task(),
        grant,
        tmp_path,
        client_id="client-id",
        client_secret="client-secret",
    )

    assert context["league_id"] == "423.l.999"
    assert context["league_ids"] == {"2023": "423.l.999"}
    assert context["start_year"] == context["end_year"] == 2023
    assert context["import_mode"] == "full"
    assert context["database_name"] == "smpl_yahoo_abcd"
    assert context["oauth_credentials"]["refresh_token"] == "private-refresh"
    assert context["data_directory"] == str(tmp_path.resolve())


def test_subprocess_environment_removes_every_fly_credential() -> None:
    env = build_subprocess_env(
        {
            "DATABASE_SERVER_URL": "server",
            "DATABASE_READ_TOKEN": "read",
            "DATABASE_WRITE_TOKEN": "write",
            "DATABASE_ADMIN_TOKEN": "admin",
            "FLY_API_TOKEN": "fly",
            "MOTHERDUCK_TOKEN": "motherduck",
            "OPS_CACHE_PATH": "/tmp/ops.duckdb",
            "PATH": "/bin",
        }
    )

    assert env["CORPUS_MODE"] == "1"
    assert env["OPS_CACHE_PATH"] == "/tmp/ops.duckdb"
    assert env["PATH"] == "/bin"
    assert not any(key in env for key in (
        "DATABASE_SERVER_URL",
        "DATABASE_READ_TOKEN",
        "DATABASE_WRITE_TOKEN",
        "DATABASE_ADMIN_TOKEN",
        "FLY_API_TOKEN",
        "MOTHERDUCK_TOKEN",
    ))


def test_sanitize_payload_rejects_private_keys_and_token_values() -> None:
    assert sanitize_payload({"task_id": "opaque", "stage": "draft"}) == {
        "task_id": "opaque",
        "stage": "draft",
    }
    with pytest.raises(ForbiddenArtifactData):
        sanitize_payload({"refresh_token": "secret"})
    with pytest.raises(ForbiddenArtifactData):
        sanitize_payload({"error": "Bearer abcdefghijklmnopqrstuvwxyz012345"})


def test_context_is_not_part_of_redacted_plan_json(tmp_path: Path) -> None:
    grant = Grant("grant-1234", "private-refresh", ("423.l.999",), ("customer",))
    context = build_context(task(), grant, tmp_path, client_id="id", client_secret="secret")
    redacted = sanitize_payload({"task_id": task().task_id, "season": task().season})

    assert "private-refresh" in json.dumps(context)
    assert "private-refresh" not in json.dumps(redacted)


def test_run_plan_rotates_grants_and_writes_redacted_ledger(tmp_path: Path) -> None:
    tasks = (
        task(),
        Candidate("yahoo-b", "grant-2", "399.l.2", 2015, "10t_flx_half_4pt", "b"),
        Candidate("yahoo-c", "grant-1234", "350.l.3", 2008, "10t_flx_half_4pt", "c"),
    )
    plan = Plan("cross-era", 3, tasks)
    grants = {
        "grant-1234": Grant("grant-1234", "private-refresh", (), ("customer",)),
        "grant-2": Grant("grant-2", "another-private-refresh", (), ("customer2",)),
    }
    seen: list[str] = []

    def execute(candidate: Candidate, _: Grant) -> TaskOutcome:
        seen.append(candidate.grant_id)
        return TaskOutcome(candidate.task_id, "success", "complete", None)

    report = run_plan(plan, grants, tmp_path, execute_task=execute)

    assert seen == ["grant-1234", "grant-2", "grant-1234"]
    assert report["successes"] == 3
    ledger_text = (tmp_path / "ledger.json").read_text(encoding="utf-8")
    assert "private-refresh" not in ledger_text
    assert "customer" not in ledger_text


def test_run_plan_waits_out_cooldown_and_retries_rate_limited(tmp_path: Path) -> None:
    plan = Plan("cross-era", 1, (task(),))
    grants = {"grant-1234": Grant("grant-1234", "private-refresh", (), ())}
    fake_now = {"t": 0.0}
    sleeps: list[float] = []
    attempts: list[float] = []

    def clock() -> float:
        return fake_now["t"]

    def sleeper(seconds: float) -> None:
        sleeps.append(seconds)
        fake_now["t"] += seconds

    def execute(candidate: Candidate, _: Grant) -> TaskOutcome:
        attempts.append(fake_now["t"])
        if len(attempts) < 3:
            return TaskOutcome(candidate.task_id, "rate_limited", "acquisition", "YahooRequestDenied")
        return TaskOutcome(candidate.task_id, "success", "complete", None)

    report = run_plan(plan, grants, tmp_path, execute_task=execute, clock=clock, sleeper=sleeper)

    assert report["successes"] == 1
    assert len(attempts) == 3
    assert sleeps and sleeps[0] >= 120
    # Second cooldown escalates beyond the first.
    assert sleeps[1] > sleeps[0]


def test_run_plan_fails_rate_limited_task_after_retry_budget(tmp_path: Path) -> None:
    plan = Plan("cross-era", 1, (task(),))
    grants = {"grant-1234": Grant("grant-1234", "private-refresh", (), ())}
    fake_now = {"t": 0.0}
    attempts: list[float] = []

    def clock() -> float:
        return fake_now["t"]

    def sleeper(seconds: float) -> None:
        fake_now["t"] += seconds

    def execute(candidate: Candidate, _: Grant) -> TaskOutcome:
        attempts.append(fake_now["t"])
        return TaskOutcome(candidate.task_id, "rate_limited", "acquisition", "YahooRequestDenied")

    report = run_plan(
        plan,
        grants,
        tmp_path,
        execute_task=execute,
        clock=clock,
        sleeper=sleeper,
        max_rate_limit_retries=2,
    )

    assert report["successes"] == 0
    assert len(attempts) == 3  # initial attempt + two retries, then failed
    ledger = json.loads((tmp_path / "ledger.json").read_text(encoding="utf-8"))
    assert ledger["scheduler"]["failed"] == {"yahoo-abcd": "YahooRequestDenied"}


def test_request_denied_classifies_as_rate_limited() -> None:
    raw = "requests.exceptions.HTTPError: 999 Server Error: Request Denied for url: https://fantasysports.yahooapis.com/..."
    assert classify_import_failure(raw) == ("rate_limited", "YahooRequestDenied")


def test_unplayed_shell_classifies_as_skipped() -> None:
    raw = (
        "[YEAR 2023] Yahoo returned a schedule shell with no played matchups; "
        "skipping remaining fetchers\n"
        "RuntimeError: Local DuckDB failed pre-upload validation with 5 issue(s)"
    )
    assert classify_import_failure(raw) == ("skipped", "UnplayedSeason")


def test_run_plan_stops_dispatching_at_time_budget(tmp_path: Path) -> None:
    tasks = (
        task(),
        Candidate("yahoo-b", "grant-2", "399.l.2", 2015, "10t_flx_half_4pt", "b"),
        Candidate("yahoo-c", "grant-3", "350.l.3", 2008, "10t_flx_half_4pt", "c"),
    )
    plan = Plan("inventory", 3, tasks)
    grants = {row.grant_id: Grant(row.grant_id, f"secret-{row.grant_id}", (), ()) for row in tasks}
    fake_now = {"t": 0.0}
    executed: list[str] = []

    def clock() -> float:
        return fake_now["t"]

    def execute(candidate: Candidate, _: Grant) -> TaskOutcome:
        executed.append(candidate.task_id)
        fake_now["t"] += 400.0  # each import takes ~6.7 minutes
        return TaskOutcome(candidate.task_id, "success", "complete", None)

    report = run_plan(
        plan,
        grants,
        tmp_path,
        execute_task=execute,
        clock=clock,
        time_budget_seconds=600,
    )

    # First import starts at t=0, second at t=400; t=800 exceeds the budget so
    # the third task stays pending for the next run.
    assert len(executed) == 2
    assert report["successes"] == 2


def test_run_plan_resumes_after_controlled_checkpoint(tmp_path: Path) -> None:
    tasks = (
        task(),
        Candidate("yahoo-b", "grant-2", "399.l.2", 2015, "10t_flx_half_4pt", "b"),
        Candidate("yahoo-c", "grant-3", "350.l.3", 2008, "10t_flx_half_4pt", "c"),
    )
    plan = Plan("spacing-resume", 3, tasks)
    grants = {
        row.grant_id: Grant(row.grant_id, f"secret-{row.grant_id}", (), ()) for row in tasks
    }
    first_seen: list[str] = []
    run_plan(
        plan,
        grants,
        tmp_path,
        execute_task=lambda row, _: (
            first_seen.append(row.task_id)
            or TaskOutcome(row.task_id, "success", "complete", None)
        ),
        stop_after=2,
    )
    resumed_seen: list[str] = []
    report = run_plan(
        plan,
        grants,
        tmp_path,
        execute_task=lambda row, _: (
            resumed_seen.append(row.task_id)
            or TaskOutcome(row.task_id, "success", "complete", None)
        ),
        resume=True,
    )

    assert len(first_seen) == 2
    assert resumed_seen == ["yahoo-c"]
    assert report["successes"] == 3


def test_execute_import_classifies_rate_limit_and_removes_private_task_dir(tmp_path: Path) -> None:
    grant = Grant("grant-1234", "private-refresh", (), ())

    def fake_run(command, **kwargs):
        # Transformations must run: the fold contract reads transformed columns
        # (is_rostered, champion, manager_lamar, ...) that raw fetches lack.
        assert "--skip-transformations" not in command
        kwargs["stdout"].write("Yahoo API 429 rate limit for private manager\n")
        kwargs["stdout"].flush()
        return subprocess.CompletedProcess(command, 1)

    outcome = execute_import_task(
        task(),
        grant,
        pipeline_root=tmp_path / "code",
        work_root=tmp_path / "private",
        snapshot_path=tmp_path / "slice.duckdb",
        sanitized_log=tmp_path / "driver.log",
        client_id="id",
        client_secret="secret",
        run_command=fake_run,
    )

    assert outcome.status == "rate_limited"
    assert outcome.error_class == "YahooRateLimit"
    assert not (tmp_path / "private" / task().task_id).exists()
    log = (tmp_path / "driver.log").read_text(encoding="utf-8")
    assert "private manager" not in log
    assert "private-refresh" not in log


def test_execute_import_reports_safe_validation_code(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    grant = Grant("grant-1234", "private-refresh", (), ())

    def reject_source(*_args, **_kwargs):
        raise SourceValidationError("private details", code="ChampionCount")

    monkeypatch.setattr(runner_module, "validate_source", reject_source)
    outcome = execute_import_task(
        task(),
        grant,
        pipeline_root=tmp_path / "code",
        work_root=tmp_path / "private",
        snapshot_path=tmp_path / "slice.duckdb",
        sanitized_log=tmp_path / "driver.log",
        client_id="id",
        client_secret="secret",
        run_command=lambda command, **kwargs: subprocess.CompletedProcess(command, 0),
    )

    assert outcome.stage == "validation"
    assert outcome.error_class == "ChampionCount"


def test_import_failure_classifier_extracts_exception_class_without_message() -> None:
    raw = "Traceback (most recent call last):\n  private details\nModuleNotFoundError: No module named private_manager\n"

    assert classify_import_failure(raw) == ("failure", "ModuleNotFoundError")


def test_runtime_failure_classifier_reports_only_safe_final_call_site() -> None:
    raw = """Traceback (most recent call last):
  File "/home/runner/private/initial_import_v3.py", line 100, in main
    private_details()
  File "/home/runner/private/yahoo_matchups.py", line 740, in fetch
    raise RuntimeError(private_manager)
RuntimeError: secret league details
"""

    assert classify_import_failure(raw) == (
        "failure",
        "RuntimeSite-yahoo_matchups-L740",
    )


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("RuntimeError: FlyReader is disabled in corpus mode", "CorpusFlyReadAttempt"),
        ("RuntimeError: FlyTarget is disabled in corpus mode", "CorpusFlyWriteAttempt"),
        ("RuntimeError: Failed to flatten league_settings for year(s): private", "SettingsCanonicalization"),
        ("RuntimeError: Fetched league settings but produced 0 canonical rows", "EmptyCanonicalSettings"),
        ("RuntimeError: Local DuckDB failed pre-upload validation with 2 issue(s)", "PreUploadValidation"),
        ("RuntimeError: No OAuth credentials available in context", "OAuthMaterialization"),
        ("RuntimeError: No leagues found for 2009", "YahooLeagueLookup"),
        ("RuntimeError: Failed to fetch league_ids for 2009: private", "YahooLeagueLookup"),
        ("RuntimeError: Failed to create league object for private after 3 attempts", "YahooLeagueObject"),
        ("RuntimeError: No matchup data found", "YahooMatchupEmpty"),
        ("RuntimeError: conn is required - all pipeline work uses local DuckDB files.", "LocalDatabaseContract"),
        ("RuntimeError: get_pipeline_connection() requires data_dir.", "LocalDatabaseContract"),
        ("RuntimeError: SQL enrichments failed: private", "SqlEnrichmentFailure"),
        ("RuntimeError: Fantasy aggregation failed after SQL enrichments", "FantasyAggregationFailure"),
        ("RuntimeError: yahoo_fantasy_api not available.", "YahooDependencyMissing"),
        ("RuntimeError: RunLogger not available or not callable", "PipelineDependencyMissing"),
    ],
)
def test_import_failure_classifier_allowlists_safe_runtime_categories(
    raw: str,
    expected: str,
) -> None:
    assert classify_import_failure(raw) == ("failure", expected)
