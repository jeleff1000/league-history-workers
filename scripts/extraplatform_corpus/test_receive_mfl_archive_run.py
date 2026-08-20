from __future__ import annotations

import json

from receive_mfl_archive_run import wait_for_successful_run


def test_wait_for_successful_run_only_returns_after_actions_reports_success() -> None:
    responses = iter(
        [
            {"status": "in_progress", "conclusion": None},
            {"status": "completed", "conclusion": "success"},
        ]
    )
    commands: list[list[str]] = []
    sleeps: list[int] = []

    def run_command(command: list[str]) -> str:
        commands.append(command)
        return json.dumps(next(responses))

    result = wait_for_successful_run(
        repo="league-history-workers/mfl-league-fetcher",
        run_id=456,
        poll_seconds=30,
        run_command=run_command,
        sleep=sleeps.append,
    )

    assert result == {"status": "completed", "conclusion": "success"}
    assert sleeps == [30]
    assert commands == [
        [
            "gh",
            "run",
            "view",
            "456",
            "--repo",
            "league-history-workers/mfl-league-fetcher",
            "--json",
            "status,conclusion",
        ],
        [
            "gh",
            "run",
            "view",
            "456",
            "--repo",
            "league-history-workers/mfl-league-fetcher",
            "--json",
            "status,conclusion",
        ],
    ]


def test_wait_for_successful_run_fails_closed_when_actions_fails() -> None:
    try:
        wait_for_successful_run(
            repo="league-history-workers/mfl-league-fetcher",
            run_id=456,
            poll_seconds=30,
            run_command=lambda _command: '{"status":"completed","conclusion":"failure"}',
            sleep=lambda _seconds: None,
        )
    except RuntimeError as exc:
        assert "failure" in str(exc)
    else:
        raise AssertionError("a failed Actions run was allowed to reach the D: receiver")
