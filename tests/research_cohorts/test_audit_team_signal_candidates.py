import json
import sys
from pathlib import Path

import duckdb
import pandas as pd


def _base_cache(path: Path) -> None:
    from scripts.research_cohorts.canonical_player_schema import (
        BASE_PLAYER_COLUMNS,
        EXPECTED_COHORT_COLUMNS,
    )

    values = {
        "db_name": "league",
        "year": 2024,
        "week": 15,
        "NFL_player_id": "p1",
        "is_started": 1,
        "is_rostered": 1,
        "fantasy_points": 10.0,
        "win": 1,
        "champion": None,
        "clutch_equity": 0.0,
        "manager_lamar": 0.0,
        "manager": "mgr",
        "team_points": 100.0,
        "final_playoff_seed": None,
        "is_playoffs": 0,
        "has_po_signal": 1,
        "player": "Player One",
        "position": "RB",
        "fantasy_position": "RB",
        "platform": "mfl",
        "team_key": "team",
        "team_name": "Team",
        "nfl_team_api": "NYJ",
        "yahoo_player_id": None,
        "sleeper_player_id": None,
        "espn_player_id": None,
        "fleaflicker_player_id": None,
        "mfl_player_id": "1",
        "made_playoffs": None,
    }
    values.update({column: None for column in EXPECTED_COHORT_COLUMNS})
    assert set(values) == set(BASE_PLAYER_COLUMNS) | set(EXPECTED_COHORT_COLUMNS)
    con = duckdb.connect(str(path))
    con.register("source", pd.DataFrame([values]))
    con.execute("CREATE SCHEMA public")
    con.execute("CREATE TABLE public.player_fantasy AS SELECT * FROM source")
    con.close()


def _write_parquet(path: Path, rows: list[dict]) -> None:
    con = duckdb.connect()
    con.register("rows", pd.DataFrame(rows))
    con.execute("COPY rows TO ? (FORMAT PARQUET)", [str(path)])
    con.close()


def test_candidate_audit_does_not_fill_champion_from_a_non_championship_week(
    tmp_path: Path, monkeypatch,
) -> None:
    """Removing the championship-week guard would create an invalid champion=0 fill."""
    from scripts.research_cohorts.audit_team_signal_candidates import main

    base = tmp_path / "base.duckdb"
    _base_cache(base)
    candidates = tmp_path / "candidates"
    candidates.mkdir()
    _write_parquet(candidates / "source.parquet", [{
        "db_name": "league", "year": 2024, "week": 15,
        "team_key": "team", "manager": "mgr", "team_name": "Team",
        "champion": 1, "is_championship": None,
    }])
    out = tmp_path / "out"
    monkeypatch.setattr(sys, "argv", [
        "audit_team_signal_candidates.py",
        "--base", str(base), "--candidates", str(candidates), "--out", str(out),
    ])

    main()

    con = duckdb.connect()
    emitted = con.execute(
        "SELECT COUNT(*) FROM read_parquet(?)", [str(out / "team_promotable_player_delta.parquet")]
    ).fetchone()[0]
    con.close()
    assert emitted == 0


def test_candidate_audit_accepts_manager_only_source_without_team_key(
    tmp_path: Path, monkeypatch,
) -> None:
    """A source that lacks the team-key column can still use its unique manager-week bridge."""
    from scripts.research_cohorts.audit_team_signal_candidates import main

    base = tmp_path / "base.duckdb"
    _base_cache(base)
    con = duckdb.connect(str(base))
    con.execute("UPDATE public.player_fantasy SET team_key=NULL, is_playoffs=NULL")
    con.close()
    candidates = tmp_path / "candidates"
    candidates.mkdir()
    _write_parquet(candidates / "manager-only.parquet", [{
        "db_name": "league", "year": 2024, "week": 15,
        "manager": "mgr", "is_playoffs": 1,
    }])
    out = tmp_path / "out"
    monkeypatch.setattr(sys, "argv", [
        "audit_team_signal_candidates.py",
        "--base", str(base), "--candidates", str(candidates), "--out", str(out),
    ])

    main()

    con = duckdb.connect()
    emitted = con.execute(
        "SELECT COUNT(*) FROM read_parquet(?)", [str(out / "team_promotable_player_delta.parquet")]
    ).fetchone()[0]
    con.close()
    assert emitted == 1
    report = json.loads((out / "team_signal_candidate_report.json").read_text(encoding="utf-8"))
    assert report["unmatched_source_team_keys"] == 0
    assert report["candidate_file_inventory"] == [{
        "path": str(candidates / "manager-only.parquet"),
        "columns": ["db_name", "is_playoffs", "manager", "week", "year"],
        "team_candidate": True,
        "player_bridge": False,
    }]


def test_candidate_audit_emits_direct_mfl_win_replacement_separately(
    tmp_path: Path, monkeypatch,
) -> None:
    from scripts.research_cohorts.audit_team_signal_candidates import main

    base = tmp_path / "base.duckdb"
    _base_cache(base)
    con = duckdb.connect(str(base))
    con.execute("UPDATE public.player_fantasy SET win=0")
    con.close()
    candidates = tmp_path / "candidates"
    candidates.mkdir()
    _write_parquet(candidates / "source.parquet", [{
        "db_name": "league", "year": 2024, "week": 15,
        "team_key": "team", "manager": "mgr", "team_name": "Team",
        "win": 1,
    }])
    out = tmp_path / "out"
    monkeypatch.setattr(sys, "argv", [
        "audit_team_signal_candidates.py",
        "--base", str(base), "--candidates", str(candidates), "--out", str(out),
        "--emit-direct-mfl-win-replacements",
    ])

    main()

    con = duckdb.connect()
    emitted = con.execute(
        "SELECT COUNT(*) FROM read_parquet(?)",
        [str(out / "team_direct_mfl_win_replacement_delta.parquet")],
    ).fetchone()[0]
    con.close()
    assert emitted == 1
    report = json.loads((out / "team_signal_candidate_report.json").read_text(encoding="utf-8"))
    assert report["direct_mfl_win_replacement_rows"] == 1


def test_candidate_audit_blocks_player_bridge_when_cache_player_week_is_duplicate(
    tmp_path: Path, monkeypatch,
) -> None:
    """One source roster team must never be fanned onto anonymous duplicate cache rows."""
    from scripts.research_cohorts.audit_team_signal_candidates import main

    base = tmp_path / "base.duckdb"
    _base_cache(base)
    con = duckdb.connect(str(base))
    con.execute("""
        UPDATE public.player_fantasy
        SET manager=NULL, team_key=NULL, team_name=NULL, is_playoffs=NULL
    """)
    con.execute("INSERT INTO public.player_fantasy SELECT * FROM public.player_fantasy")
    con.execute("""
        INSERT INTO public.player_fantasy
        SELECT * REPLACE ('unrelated' AS NFL_player_id)
        FROM public.player_fantasy
        LIMIT 1
    """)
    con.close()
    candidates = tmp_path / "candidates"
    candidates.mkdir()
    _write_parquet(candidates / "team_signal.parquet", [{
        "db_name": "league", "year": 2024, "week": 15,
        "team_key": "source-team", "manager": "source manager", "team_name": "Source Team",
        "is_playoffs": 1,
    }])
    _write_parquet(candidates / "player_bridge.parquet", [{
        "db_name": "league", "year": 2024, "week": 15, "NFL_player_id": "p1",
        "team_key": "source-team", "manager": "source manager", "team_name": "Source Team",
    }])
    out = tmp_path / "out"
    monkeypatch.setattr(sys, "argv", [
        "audit_team_signal_candidates.py",
        "--base", str(base), "--candidates", str(candidates), "--out", str(out),
    ])

    main()

    con = duckdb.connect()
    emitted = con.execute(
        "SELECT COUNT(*) FROM read_parquet(?)", [str(out / "team_promotable_player_delta.parquet")]
    ).fetchone()[0]
    con.close()
    assert emitted == 0
    report = json.loads((out / "team_signal_candidate_report.json").read_text(encoding="utf-8"))
    assert report["bridge_blocked_duplicate_cache_player_weeks"] == 1


def test_exact_mfl_duplicate_builder_pairs_matching_roster_copies_and_ignores_bye_outcome(
    tmp_path: Path,
) -> None:
    """A synthetic MFL BYE row must not replace a real opponent's loss."""
    from scripts.research_cohorts.build_exact_mfl_duplicate_bridge_candidate import build

    base = tmp_path / "base.duckdb"
    _base_cache(base)
    con = duckdb.connect(str(base))
    con.execute("""
        UPDATE public.player_fantasy
        SET manager=NULL, team_key=NULL, team_name=NULL, team_points=NULL,
            win=NULL
    """)
    con.execute("INSERT INTO public.player_fantasy SELECT * FROM public.player_fantasy")
    con.close()

    memberships = tmp_path / "memberships.parquet"
    _write_parquet(memberships, [
        {
            "db_name": "league", "year": 2024, "week": 15,
            "source_year": 2024, "source_franchise_id": "0001",
            "source_manager": "Alpha", "mfl_player_id": "m1",
            "is_started": 1, "fantasy_points": 10.0,
        },
        {
            "db_name": "league", "year": 2024, "week": 15,
            "source_year": 2024, "source_franchise_id": "0002",
            "source_manager": "Bravo", "mfl_player_id": "m2",
            "is_started": 1, "fantasy_points": 10.0,
        },
    ])
    crosswalk = tmp_path / "crosswalk.parquet"
    _write_parquet(crosswalk, [
        {"source_year": 2024, "mfl_player_id": "m1", "NFL_player_id": "p1"},
        {"source_year": 2024, "mfl_player_id": "m2", "NFL_player_id": "p1"},
    ])
    source = tmp_path / "source.parquet"
    _write_parquet(source, [
        {
            "db_name": "league", "year": 2024, "week": 15,
            "team_key": "0001", "manager": "Alpha", "team_name": "Alpha Team",
            "opponent_franchise_id": "mfl_team_1_0002", "win": 0, "loss": 1, "tie": 0,
            "team_points": 90.0, "is_playoffs": 1,
        },
        {
            "db_name": "league", "year": 2024, "week": 15,
            "team_key": "0001", "manager": "Alpha", "team_name": "Alpha Team",
            "opponent_franchise_id": "mfl_team_1_BYE", "win": 1, "loss": 0, "tie": 0,
            "team_points": 90.0, "is_playoffs": 1,
        },
        {
            "db_name": "league", "year": 2024, "week": 15,
            "team_key": "0002", "manager": "Bravo", "team_name": "Bravo Team",
            "opponent_franchise_id": "mfl_team_1_0001", "win": 1, "loss": 0, "tie": 0,
            "team_points": 100.0, "is_playoffs": 1,
        },
    ])

    out = tmp_path / "out"
    build(base=base, memberships=[memberships], crosswalks=[crosswalk], source=source, out=out)

    con = duckdb.connect()
    rows = con.execute("""
        SELECT source_team_key, source_win, source_loss, source_team_points, source_manager
        FROM read_parquet(?) ORDER BY source_team_key
    """, [str(out / "mfl_exact_duplicate_candidate.parquet")]).fetchall()
    con.close()
    assert rows == [
        ("0001", 0, 1, 90.0, "Alpha"),
        ("0002", 1, 0, 100.0, "Bravo"),
    ]
    report = json.loads((out / "mfl_exact_duplicate_candidate_report.json").read_text(encoding="utf-8"))
    assert report["scoped_cache_rows"] == 2
