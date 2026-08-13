import json
from pathlib import Path

import duckdb


def _base(path: Path) -> None:
    con = duckdb.connect(str(path))
    con.execute("CREATE SCHEMA public")
    con.execute(
        """
        CREATE TABLE public.player_fantasy (
          db_name VARCHAR, year INTEGER, week INTEGER, is_playoffs INTEGER,
          champion INTEGER, platform VARCHAR, manager VARCHAR, team_key VARCHAR,
          team_name VARCHAR, fleaflicker_player_id VARCHAR
        )
        """
    )
    con.execute(
        """
        CREATE TABLE public.matchup (
          db_name VARCHAR, year INTEGER, week INTEGER, team_key VARCHAR,
          is_playoffs INTEGER, champion INTEGER, is_championship INTEGER
        )
        """
    )
    con.execute(
        """
        INSERT INTO public.player_fantasy VALUES
          ('smpl_ffl_100365', 2010, 15, NULL, NULL, 'fleaflicker', NULL, NULL, NULL, '101'),
          ('smpl_ffl_100365', 2010, 15, NULL, NULL, 'fleaflicker', 'known', '712338', 'Known Team', '102'),
          ('smpl_ffl_100365', 2010, 15, NULL, NULL, 'sleeper', NULL, NULL, NULL, '101')
        """
    )
    con.execute(
        """
        INSERT INTO public.matchup VALUES
          ('smpl_ffl_100365', 2010, 15, '712337', 1, 1, 1),
          ('smpl_ffl_100365', 2010, 15, '712338', NULL, 1, 0)
        """
    )
    con.close()


def test_targets_only_unbridged_fleaflicker_team_signals_and_preserves_cache(tmp_path: Path) -> None:
    from scripts.research_cohorts.build_fleaflicker_team_signal_targets import build

    base = tmp_path / "lake.duckdb"
    _base(base)
    before = base.read_bytes()
    out = tmp_path / "targets.parquet"
    report = tmp_path / "report.json"

    result = build(base=base, out=out, report=report)

    assert base.read_bytes() == before
    assert result["read_only"] is True
    assert result["cache_mutated"] is False
    assert result["new_lineage"] is False
    assert result["target_team_weeks"] == 1
    assert result["championship_winner_team_weeks"] == 1
    con = duckdb.connect()
    row = con.execute("SELECT * FROM read_parquet(?)", [str(out)]).fetchone()
    columns = [item[0] for item in con.execute("DESCRIBE SELECT * FROM read_parquet(?)", [str(out)]).fetchall()]
    target = dict(zip(columns, row))
    assert target["team_key"] == "712337"
    assert target["source_league_id"] == "100365"
    assert target["source_is_playoffs"] == 1
    assert target["source_champion"] == 1
    con.close()
    assert json.loads(report.read_text())["champion_contract"] == "is_championship=1 AND champion=1 only"
