from __future__ import annotations

from pathlib import Path

from scripts.yahoo_corpus.cli import build_parser, load_plan, pilot_ready, write_plan
from scripts.yahoo_corpus.planner import Plan
from scripts.yahoo_corpus.scheduler import Candidate


def test_cli_defaults_to_twelve_cross_era_tasks() -> None:
    args = build_parser().parse_args([])

    assert args.mode == "cross-era"
    assert args.task_limit == 12
    assert args.pipeline_root == Path("code")
    assert args.output == Path("corpus/yahoo-pilot")


def test_plan_round_trip_preserves_candidates_without_private_fields(tmp_path: Path) -> None:
    plan = Plan(
        "cross-era",
        1,
        (
            Candidate(
                "yahoo-a",
                "grant-a",
                "423.l.1",
                2023,
                "10t_flx_half_4pt",
                "423.l.1",
            ),
        ),
    )
    path = tmp_path / "plan.json"

    write_plan(path, plan)
    loaded = load_plan(path)

    assert loaded == plan
    text = path.read_text(encoding="utf-8")
    assert "refresh_token" not in text
    assert "league_name" not in text


def test_cross_era_readiness_requires_all_four_eras() -> None:
    recent = [
        Candidate(f"t{i}", f"g{i}", f"449.l.{i}", 2024, "10t_flx_half_4pt", f"l{i}")
        for i in range(12)
    ]
    balanced = [
        Candidate(f"b{i}", f"gb{i}", f"{year}.l.{i}", year, "10t_flx_half_4pt", f"lb{i}")
        for i, year in enumerate((2002, 2004, 2008, 2011, 2014, 2017, 2018, 2020, 2022, 2023, 2024, 2025))
    ]

    assert pilot_ready("cross-era", recent, 12) is False
    assert pilot_ready("cross-era", balanced, 12) is True


def test_cross_era_readiness_requires_distinct_grants_inside_each_era() -> None:
    years = (2002, 2004, 2008, 2011, 2014, 2017, 2018, 2020, 2022, 2023, 2024, 2025)
    candidates = [
        Candidate(
            f"t{i}",
            "shared-early" if year <= 2009 else f"g{i}",
            f"{year}.l.{i}",
            year,
            "10t_flx_half_4pt",
            f"l{i}",
        )
        for i, year in enumerate(years)
    ]

    assert pilot_ready("cross-era", candidates, 12) is False


def test_assign_identities_groups_grants_by_league_set() -> None:
    """One person, two grants, same leagues -> one identity."""
    from scripts.yahoo_corpus.cli import assign_identities
    from scripts.yahoo_corpus.planner import Plan
    from scripts.yahoo_corpus.scheduler import Candidate

    tasks = (
        # Mike authorized twice; both grants see leagues A and B.
        Candidate("t1", "grant-mike-1", "449.l.A", 2021, "", "449.l.A"),
        Candidate("t2", "grant-mike-1", "449.l.B", 2021, "", "449.l.B"),
        Candidate("t3", "grant-mike-2", "449.l.A", 2020, "", "449.l.A"),
        Candidate("t4", "grant-mike-2", "449.l.B", 2020, "", "449.l.B"),
        # Ann sees a different league set.
        Candidate("t5", "grant-ann", "449.l.C", 2021, "", "449.l.C"),
    )
    plan = assign_identities(Plan("inventory", 5, tasks))
    by_task = {row.task_id: row.identity_id for row in plan.tasks}

    assert by_task["t1"] == by_task["t3"], "same league set must share one identity"
    assert by_task["t1"] != by_task["t5"], "different people must differ"
    assert all(value for value in by_task.values())


def test_select_shard_keeps_each_account_whole() -> None:
    """No account may span two runners -- that would race one rate-limit bucket."""
    from scripts.yahoo_corpus.cli import assign_identities, select_shard
    from scripts.yahoo_corpus.planner import Plan
    from scripts.yahoo_corpus.scheduler import Candidate

    tasks = tuple(
        Candidate(f"t{i}", f"grant-{i % 7}", f"449.l.{i % 7}", 2000 + i, "", f"449.l.{i % 7}")
        for i in range(40)
    )
    plan = assign_identities(Plan("inventory", 40, tasks))

    shard_count = 5
    seen: dict[str, int] = {}
    total = 0
    for index in range(shard_count):
        shard = select_shard(plan, index, shard_count)
        total += len(shard.tasks)
        for row in shard.tasks:
            assert seen.setdefault(row.spacing_key, index) == index, "account split across shards"

    # Every task is covered exactly once.
    assert total == len(plan.tasks)
