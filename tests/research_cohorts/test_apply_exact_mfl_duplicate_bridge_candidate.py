import duckdb
import pandas as pd

from scripts.research_cohorts.canonical_player_schema import (
    BASE_PLAYER_COLUMNS,
    EXPECTED_COHORT_COLUMNS,
)


def _base(path):
    columns = BASE_PLAYER_COLUMNS + sorted(EXPECTED_COHORT_COLUMNS) + ["loss", "tie"]
    con = duckdb.connect(str(path))
    con.execute("CREATE SCHEMA public")
    con.execute(
        "CREATE TABLE public.player_fantasy ("
        + ", ".join(f'"{column}" VARCHAR' for column in columns)
        + ")"
    )
    values = {column: None for column in columns}
    values.update({
        "db_name": "league-a", "year": "2024", "week": "15", "NFL_player_id": "nfl-a",
        "is_rostered": "1", "is_started": "1", "fantasy_points": "10.0",
        "manager": None, "team_key": None, "team_name": None, "mfl_player_id": None,
        "win": "0", "loss": "1", "tie": "0", "team_points": "100.0",
        "is_playoffs": "0", "has_po_signal": None, "platform": "mfl",
    })
    names = ", ".join(f'"{column}"' for column in columns)
    placeholders = ", ".join("?" for _ in columns)
    con.execute(f"INSERT INTO public.player_fantasy ({names}) VALUES ({placeholders})", list(values.values()))
    con.close()


def _candidate(path):
    con = duckdb.connect()
    con.register("candidate", pd.DataFrame([{
        "player_rowid": 0,
        "db_name": "league-a", "year": 2024, "week": 15, "NFL_player_id": "nfl-a",
        "canonical_manager": None, "canonical_team_key": None, "canonical_team_name": None,
        "canonical_mfl_player_id": None, "canonical_win": 0, "canonical_loss": 1,
        "canonical_tie": 0, "canonical_team_points": 100.0, "canonical_is_playoffs": 0,
        "source_team_key": "0001", "source_manager": "Alpha", "source_team_name": "Alpha Team",
        "source_mfl_player_id": "mfl-1", "source_win": 1, "source_loss": 0,
        "source_tie": 0, "source_team_points": 140.0, "source_is_playoffs": 1,
    }]))
    con.execute("COPY candidate TO ? (FORMAT PARQUET)", [str(path)])
    con.close()


def test_apply_exact_mfl_candidate_replaces_only_exact_snapshot_rows(tmp_path):
    from scripts.research_cohorts.apply_exact_mfl_duplicate_bridge_candidate import apply

    base = tmp_path / "base.duckdb"
    candidate = tmp_path / "candidate.parquet"
    report = tmp_path / "report.json"
    _base(base)
    _candidate(candidate)

    result = apply(base=base, candidate=candidate, report=report)

    assert result["cache_mutated"] is True
    assert result["new_lineage"] is False
    assert result["candidate_rows"] == result["recipient_rows"] == 1
    assert result["source_replacements"] == {
        "win": 1, "loss": 1, "tie": 0, "team_points": 1, "is_playoffs": 1,
    }
    con = duckdb.connect(str(base), read_only=True)
    row = con.execute("""
        SELECT manager,team_key,team_name,mfl_player_id,win,loss,tie,team_points,is_playoffs,has_po_signal
        FROM public.player_fantasy
    """).fetchone()
    con.close()
    assert row == ("Alpha", "0001", "Alpha Team", "mfl-1", "1", "0", "0", "140.0", "1", "1")
