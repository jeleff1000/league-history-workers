from __future__ import annotations

import json
from pathlib import Path

import duckdb
import pandas as pd


def _parquet(path: Path, rows: list[dict]) -> None:
    con = duckdb.connect()
    con.register("rows", pd.DataFrame(rows))
    con.execute("COPY rows TO ? (FORMAT PARQUET)", [str(path)])
    con.close()


def test_bridge_requires_unique_raw_team_crosswalk_and_canonical_recipient(tmp_path: Path) -> None:
    from scripts.research_cohorts.build_mfl_roster_bridge_receipt import build

    base = tmp_path / "base.duckdb"
    con = duckdb.connect(str(base))
    con.execute("CREATE SCHEMA public")
    con.execute("""
        CREATE TABLE public.player_fantasy (
          db_name VARCHAR, year INTEGER, week INTEGER, NFL_player_id VARCHAR,
          manager VARCHAR, platform VARCHAR
        )
    """)
    con.execute("""
        INSERT INTO public.player_fantasy VALUES
          ('league', 2024, 1, 'p1', 'Owner One', 'mfl'),
          ('league', 2024, 1, 'p2', 'Owner One', 'mfl'),
          ('league', 2024, 1, 'p3', 'Owner One', 'mfl'),
          ('league', 2024, 1, 'p3', 'Owner One', 'mfl'),
          ('league', 2024, 1, 'p4', 'Owner Two', 'mfl')
    """)
    before_schema = con.execute("DESCRIBE public.player_fantasy").fetchall()
    before_rows = con.execute("SELECT COUNT(*) FROM public.player_fantasy").fetchone()[0]
    con.close()

    memberships = tmp_path / "memberships.parquet"
    _parquet(memberships, [
        {"db_name": "league", "year": 2024, "week": 1, "source_year": 2024,
         "source_franchise_id": "0001", "source_manager": " Owner  One ",
         "source_manager_origin": "mfl_franchise_owner_name", "mfl_player_id": "10"},
        {"db_name": "league", "year": 2024, "week": 1, "source_year": 2024,
         "source_franchise_id": "0001", "source_manager": "Owner One",
         "source_manager_origin": "mfl_franchise_owner_name", "mfl_player_id": "12"},
        {"db_name": "league", "year": 2024, "week": 1, "source_year": 2024,
         "source_franchise_id": "0001", "source_manager": "Owner One",
         "source_manager_origin": "mfl_franchise_owner_name", "mfl_player_id": "13"},
        {"db_name": "league", "year": 2024, "week": 1, "source_year": 2024,
         "source_franchise_id": "0002", "source_manager": "Owner Two",
         "source_manager_origin": "mfl_franchise_owner_name", "mfl_player_id": "14"},
        {"db_name": "league", "year": 2024, "week": 1, "source_year": 2024,
         "source_franchise_id": "0004", "source_manager": "Owner Two",
         "source_manager_origin": "mfl_franchise_owner_name", "mfl_player_id": "16"},
        {"db_name": "league", "year": 2024, "week": 1, "source_year": 2024,
         "source_franchise_id": "0003", "source_manager": None,
         "source_manager_origin": "mfl_franchise_owner_name", "mfl_player_id": "15"},
        {"db_name": "league", "year": 2024, "week": 1, "source_year": 2024,
         "source_franchise_id": "0005", "source_manager": "Owner Three",
         "source_manager_origin": "target_inventory_manager", "mfl_player_id": "17"},
    ])
    crosswalk = tmp_path / "crosswalk.parquet"
    _parquet(crosswalk, [
        {"source_year": 2024, "mfl_player_id": "10", "NFL_player_id": "p1"},
        {"source_year": 2024, "mfl_player_id": "11", "NFL_player_id": "p2"},
        {"source_year": 2024, "mfl_player_id": "12", "NFL_player_id": "p3"},
        {"source_year": 2024, "mfl_player_id": "13", "NFL_player_id": "p1"},
        {"source_year": 2024, "mfl_player_id": "13", "NFL_player_id": "p2"},
        {"source_year": 2024, "mfl_player_id": "14", "NFL_player_id": "p4"},
    ])

    out = tmp_path / "bridge.parquet"
    report = tmp_path / "report.json"
    result = build(base=base, memberships=memberships, crosswalk=crosswalk, out=out, report=report)

    assert result["read_only"] is True
    assert result["cache_mutated"] is False
    assert result["new_lineage"] is False
    assert result["input_memberships"] == 7
    assert result["status_counts"] == {
        "ambiguous_canonical_recipient": 1,
        "ambiguous_player_crosswalk": 1,
        "ambiguous_source_franchise_manager": 2,
        "invalid_source_manager_provenance": 1,
        "missing_source_manager": 1,
        "resolved_exact_player_team": 1,
    }

    check = duckdb.connect()
    resolved = check.execute(
        "SELECT mfl_player_id, NFL_player_id, canonical_manager FROM read_parquet(?)",
        [str(out)],
    ).fetchall()
    check.close()
    assert resolved == [("10", "p1", "Owner One")]

    after = duckdb.connect(str(base), read_only=True)
    assert after.execute("DESCRIBE public.player_fantasy").fetchall() == before_schema
    assert after.execute("SELECT COUNT(*) FROM public.player_fantasy").fetchone()[0] == before_rows
    after.close()
    assert json.loads(report.read_text())["status_counts"] == result["status_counts"]
