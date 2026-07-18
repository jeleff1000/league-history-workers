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
