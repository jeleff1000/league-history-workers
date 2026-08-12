from __future__ import annotations

import duckdb


def _parquet(path, table, columns, rows) -> None:
    con = duckdb.connect()
    con.execute("CREATE TABLE source (" + ", ".join(f'\"{name}\" {kind}' for name, kind in columns) + ")")
    con.executemany(
        "INSERT INTO source VALUES (" + ", ".join("?" for _ in columns) + ")",
        rows,
    )
    con.execute("COPY source TO ? (FORMAT PARQUET)", [str(path)])
    con.close()


def test_builds_one_complete_exact_schema_mfl_row_from_source_membership_and_team_template(tmp_path) -> None:
    from scripts.research_cohorts.build_exact_mfl_missing_player_rows import build

    base = tmp_path / "base.duckdb"
    con = duckdb.connect(str(base))
    con.execute("CREATE SCHEMA public")
    con.execute("""
      CREATE TABLE public.player_fantasy (
        db_name VARCHAR, year INTEGER, week INTEGER, NFL_player_id VARCHAR,
        is_started INTEGER, is_rostered INTEGER, fantasy_points DOUBLE, win INTEGER,
        champion INTEGER, clutch_equity DOUBLE, manager_lamar DOUBLE, manager VARCHAR,
        team_points DOUBLE, final_playoff_seed INTEGER, is_playoffs INTEGER,
        has_po_signal INTEGER, player VARCHAR, position VARCHAR, fantasy_position VARCHAR,
        platform VARCHAR, team_key VARCHAR, team_name VARCHAR, nfl_team_api VARCHAR,
        yahoo_player_id VARCHAR, sleeper_player_id VARCHAR, espn_player_id VARCHAR,
        fleaflicker_player_id VARCHAR, mfl_player_id VARCHAR, cohort_teams VARCHAR,
        cohort_roster VARCHAR, cohort_scoring VARCHAR, cohort_pass_td VARCHAR,
        cohort_playoff_teams VARCHAR, cohort_dynasty VARCHAR, cohort_best_ball VARCHAR,
        cohort_position_eligible INTEGER, made_playoffs INTEGER
      )
    """)
    con.execute("""
      INSERT INTO public.player_fantasy VALUES
      ('league', 2012, 1, 'existing', 1, NULL, NULL, 1, 0, .25, 4.0, 'Manager',
       101.5, NULL, 0, NULL, 'Existing', 'RB', 'RB', 'mfl', NULL, NULL, NULL,
       NULL, NULL, NULL, NULL, NULL, '12t', 'idp', 'ppr', '6pt', '4po',
       'redraft', 'managed', 1, NULL)
    """)
    con.execute("CREATE TABLE public.league_settings (db_name VARCHAR, year INTEGER)")
    con.execute("INSERT INTO public.league_settings VALUES ('league', 2012)")
    con.close()

    ops = tmp_path / "ops.duckdb"
    con = duckdb.connect(str(ops))
    con.execute("CREATE SCHEMA nfl_historical")
    con.execute("""
      CREATE TABLE nfl_historical.nfl_player_stats_all (
        NFL_player_id VARCHAR, year INTEGER, week INTEGER, player VARCHAR,
        position VARCHAR, lamar_12t_idp_ppr_6pt DOUBLE
      )
    """)
    con.execute("INSERT INTO nfl_historical.nfl_player_stats_all VALUES ('nfl-missing', 2012, 1, 'Missing Player', 'WR', 7.5)")
    con.close()

    memberships = tmp_path / "memberships.parquet"
    _parquet(memberships, "source", [
        ("db_name", "VARCHAR"), ("year", "INTEGER"), ("week", "INTEGER"),
        ("source_year", "INTEGER"), ("source_franchise_id", "VARCHAR"), ("source_manager", "VARCHAR"),
        ("mfl_player_id", "VARCHAR"), ("is_started", "INTEGER"), ("fantasy_points", "DOUBLE"),
    ], [
        ("league", 2012, 1, 2012, "0001", "Manager", "100", 1, 12.5),
        ("league", 2012, 1, 2012, "0001", "Manager", "101", 1, 8.0),
    ])
    directory = tmp_path / "directory.parquet"
    _parquet(directory, "source", [
        ("source_year", "INTEGER"), ("mfl_player_id", "VARCHAR"),
        ("espn_id", "VARCHAR"), ("raw_team", "VARCHAR"),
    ], [(2012, "100", "999", "NEP"), (2012, "101", "998", "NEP")])
    crosswalk = tmp_path / "crosswalk.parquet"
    _parquet(crosswalk, "source", [
        ("source_year", "INTEGER"), ("mfl_player_id", "VARCHAR"), ("NFL_player_id", "VARCHAR"),
    ], [(2012, "100", "nfl-missing"), (2012, "101", "existing")])
    targets = tmp_path / "targets.parquet"
    _parquet(targets, "source", [
        ("db_name", "VARCHAR"), ("year", "INTEGER"), ("week", "INTEGER"),
        ("source_year", "INTEGER"), ("source_franchise_id", "VARCHAR"), ("source_manager", "VARCHAR"),
        ("mfl_player_id", "VARCHAR"), ("bridge_status", "VARCHAR"),
        ("canonical_player_week_count", "INTEGER"),
    ], [("league", 2012, 1, 2012, "0001", "Manager", "100", "no_canonical_recipient", 0)])

    out = tmp_path / "candidate.parquet"
    report = tmp_path / "report.json"
    result = build(
        base=base, ops=ops, memberships=memberships, directory=directory,
        crosswalk=crosswalk, targets=targets, out=out, report=report, expected_rows=1,
    )

    assert result["candidate_rows"] == 1
    assert result["cache_mutated"] is False
    check = duckdb.connect()
    row = check.execute("SELECT * FROM read_parquet(?)", [str(out)]).fetchone()
    columns = [item[0] for item in check.execute("DESCRIBE SELECT * FROM read_parquet(?)", [str(out)]).fetchall()]
    check.close()
    values = dict(zip(columns, row))
    assert values["NFL_player_id"] == "nfl-missing"
    assert values["is_rostered"] == 1
    assert values["is_started"] == 1
    assert values["fantasy_points"] == 12.5
    assert values["manager_lamar"] == 7.5
    assert values["fantasy_position"] == "WR"
    assert values["win"] == 1
    assert values["team_points"] == 101.5
    assert values["cohort_roster"] == "idp"
