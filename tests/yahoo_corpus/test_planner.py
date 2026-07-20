from __future__ import annotations

import json

from scripts.yahoo_corpus.planner import build_plan
from scripts.yahoo_corpus.scheduler import Candidate


def row(task: str, grant: str, year: int) -> Candidate:
    return Candidate(
        task_id=task,
        grant_id=grant,
        league_key=f"{year}.l.{task}",
        season=year,
        cohort_slug="10t_flx_half_4pt",
        lineage_id=task,
    )


def test_cross_era_plan_is_redacted_and_reports_shortfall() -> None:
    candidates = [row("old", "g1", 2004), row("new", "g2", 2024)]

    plan = build_plan("cross-era", candidates, limit=4)
    payload = plan.to_dict()
    serialized = json.dumps(payload)

    assert len(payload["tasks"]) == 2
    assert payload["requested"] == 4
    assert payload["shortfall"] == 2
    assert "refresh_token" not in serialized
    assert "league_name" not in serialized
    assert set(payload["tasks"][0]) == {
        "task_id",
        "grant_id",
        "league_key",
        "season",
        "cohort_slug",
        "lineage_id",
        "identity_id",
        "era",
    }


def test_spacing_plan_selects_three_years_from_four_grants() -> None:
    candidates = [
        row(f"{grant}-{year}", grant, year)
        for grant in ("g1", "g2", "g3", "g4")
        for year in (2002, 2014, 2025)
    ]

    plan = build_plan("spacing-resume", candidates, limit=12)

    assert len(plan.tasks) == 12
    assert len({task.grant_id for task in plan.tasks}) == 4


def test_inventory_plan_keeps_every_unique_task() -> None:
    rows = [
        Candidate("yahoo-a", "grant-1", "423.l.1", 2023, "10t_flx_half_4pt", "a"),
        Candidate("yahoo-b", "grant-1", "406.l.1", 2021, "10t_flx_half_4pt", "a"),
        Candidate("yahoo-a", "grant-1", "423.l.1", 2023, "10t_flx_half_4pt", "a"),  # dupe
        Candidate("yahoo-c", "grant-2", "399.l.2", 2015, "12t_flx_std_6pt", "b"),
    ]
    plan = build_plan("inventory", rows, limit=0)
    assert [row.task_id for row in plan.tasks] == ["yahoo-a", "yahoo-b", "yahoo-c"]
    assert plan.requested == 3
    assert plan.shortfall == 0

    capped = build_plan("inventory", rows, limit=2)
    assert len(capped.tasks) == 2


def test_plan_rejects_unknown_mode() -> None:
    try:
        build_plan("unknown", [], limit=12)
    except ValueError as exc:
        assert "unknown" in str(exc)
    else:
        raise AssertionError("unknown plan mode was accepted")
