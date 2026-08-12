from __future__ import annotations

import duckdb
import pytest


KEY = ("db_name", "year", "week", "NFL_player_id", "manager")
SCHEMA = {
    "db_name": "VARCHAR",
    "year": "INTEGER",
    "week": "INTEGER",
    "NFL_player_id": "VARCHAR",
    "manager": "VARCHAR",
    "is_started": "INTEGER",
    "is_rostered": "INTEGER",
    "fantasy_points": "DOUBLE",
    "platform": "VARCHAR",
    "cohort_teams": "VARCHAR",
}


def _make_base(path) -> None:
    con = duckdb.connect(str(path))
    con.execute("CREATE SCHEMA public")
    con.execute(
        "CREATE TABLE public.player_fantasy ("
        + ", ".join(f'\"{name}\" {kind}' for name, kind in SCHEMA.items())
        + ")"
    )
    con.execute(
        "INSERT INTO public.player_fantasy VALUES "
        "('league', 2012, 1, 'existing', 'Manager', 1, 1, 10.0, 'mfl', 'ALL')"
    )
    con.close()


def _write_candidates(path, rows) -> None:
    con = duckdb.connect()
    con.execute(
        "CREATE TABLE candidates ("
        + ", ".join(f'\"{name}\" {kind}' for name, kind in SCHEMA.items())
        + ")"
    )
    con.executemany(
        "INSERT INTO candidates VALUES (" + ", ".join("?" for _ in SCHEMA) + ")",
        [[row[name] for name in SCHEMA] for row in rows],
    )
    con.execute("COPY candidates TO ? (FORMAT PARQUET)", [str(path)])
    con.close()


def test_in_place_add_rejects_any_existing_candidate_key_and_leaves_cache_unchanged(tmp_path) -> None:
    from scripts.research_cohorts.apply_exact_player_row_additions_in_place import apply

    base = tmp_path / "base.duckdb"
    candidates = tmp_path / "candidates.parquet"
    report = tmp_path / "report.json"
    _make_base(base)
    _write_candidates(candidates, [
        {
            "db_name": "league", "year": 2012, "week": 1,
            "NFL_player_id": "existing", "manager": "Manager",
            "is_started": 1, "is_rostered": 1, "fantasy_points": 10.0,
            "platform": "mfl", "cohort_teams": "ALL",
        },
        {
            "db_name": "league", "year": 2012, "week": 1,
            "NFL_player_id": "missing", "manager": "Manager",
            "is_started": 1, "is_rostered": 1, "fantasy_points": 11.0,
            "platform": "mfl", "cohort_teams": "ALL",
        },
    ])

    with pytest.raises(ValueError, match="already exist"):
        apply(base=base, candidates=candidates, report=report)

    check = duckdb.connect(str(base), read_only=True)
    assert check.execute("SELECT COUNT(*) FROM public.player_fantasy").fetchone()[0] == 1
    assert check.execute("SELECT COUNT(*) FROM public.player_fantasy WHERE NFL_player_id='missing'").fetchone()[0] == 0
    check.close()


def test_in_place_add_inserts_only_absent_exact_schema_rows_and_reads_them_back(tmp_path) -> None:
    from scripts.research_cohorts.apply_exact_player_row_additions_in_place import apply

    base = tmp_path / "base.duckdb"
    candidates = tmp_path / "candidates.parquet"
    report = tmp_path / "report.json"
    _make_base(base)
    _write_candidates(candidates, [{
        "db_name": "league", "year": 2012, "week": 1,
        "NFL_player_id": "missing", "manager": "Manager",
        "is_started": 1, "is_rostered": 1, "fantasy_points": 11.0,
        "platform": "mfl", "cohort_teams": "ALL",
    }])

    result = apply(base=base, candidates=candidates, report=report)

    assert result["candidate_rows"] == 1
    assert result["inserted_rows"] == 1
    assert result["readback_missing_keys"] == 0
    assert result["schema_unchanged"] is True
    check = duckdb.connect(str(base), read_only=True)
    assert check.execute("SELECT COUNT(*) FROM public.player_fantasy").fetchone()[0] == 2
    assert check.execute("SELECT fantasy_points FROM public.player_fantasy WHERE NFL_player_id='missing'").fetchone()[0] == 11.0
    assert [row[0] for row in check.execute("DESCRIBE public.player_fantasy").fetchall()] == list(SCHEMA)
    check.close()
