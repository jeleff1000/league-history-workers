import json

import duckdb

from canonical_player_schema import BASE_PLAYER_COLUMNS, EXPECTED_COHORT_COLUMNS
from build_exact_sleeper_playoff_champ_candidate import build


def _base(path, *, duplicate_week14=False):
    columns = BASE_PLAYER_COLUMNS + sorted(EXPECTED_COHORT_COLUMNS) + ["loss", "tie"]
    con = duckdb.connect(str(path))
    con.execute("CREATE SCHEMA public")
    con.execute(
        "CREATE TABLE public.player_fantasy ("
        + ", ".join(f'"{column}" VARCHAR' for column in columns)
        + ")"
    )

    def add(*, week, nfl_id, sleeper_id, team_key, manager):
        values = {column: None for column in columns}
        values.update({
            "db_name": "smpl_1257417276856999936",
            "year": "2017",
            "week": str(week),
            "NFL_player_id": nfl_id,
            "sleeper_player_id": sleeper_id,
            "team_key": team_key,
            "team_name": f"Team {team_key}",
            "manager": manager,
            "platform": "sleeper",
            "is_started": "1",
        })
        names = ", ".join(f'"{column}"' for column in columns)
        con.execute(
            f"INSERT INTO public.player_fantasy ({names}) VALUES ({','.join('?' for _ in columns)})",
            list(values.values()),
        )

    add(week=14, nfl_id="nfl-3", sleeper_id="sp-3", team_key="league_3", manager="Third")
    if duplicate_week14:
        add(week=14, nfl_id="nfl-3-duplicate", sleeper_id="sp-3", team_key="league_3", manager="Third")
    add(week=15, nfl_id="nfl-5", sleeper_id="sp-5", team_key="league_5", manager="Fifth")
    con.close()


def _source(path):
    payload = {
        "db_name": "smpl_1257417276856999936",
        "year": 2017,
        "league": {
            "season": "2017",
            # Sleeper's 2017 API omits the value; omission is its documented
            # single-week default for this historical league.
            "settings": {"playoff_week_start": 14, "playoff_round_type": None},
        },
        "winners_bracket": [
            {"r": 1, "p": 1, "t1": 3, "t2": 5, "w": 5},
            {"r": 1, "p": 1, "t1": 6, "t2": 4, "w": 6},
            {"r": 2, "p": 1, "t1": 5, "t2": 6, "w": 5},
            {"r": 2, "p": 3, "t1": 3, "t2": 4, "w": 3},
        ],
        "matchups_by_week": {
            "14": [
                {"roster_id": 3, "matchup_id": 1, "starters": ["sp-3"]},
                {"roster_id": 5, "matchup_id": 1, "starters": ["sp-5"]},
                {"roster_id": 6, "matchup_id": 2, "starters": ["sp-6"]},
                {"roster_id": 4, "matchup_id": 2, "starters": ["sp-4"]},
            ],
            "15": [
                # Sleeper's historical final-week matchup ids do not preserve
                # the winner-bracket pairing. The bracket, not this id, proves
                # that rosters 5/6 played the title game.
                {"roster_id": 5, "matchup_id": 4, "starters": ["sp-5"]},
                {"roster_id": 6, "matchup_id": 1, "starters": ["sp-6"]},
                {"roster_id": 3, "matchup_id": 4, "starters": ["sp-3"]},
                {"roster_id": 4, "matchup_id": 4, "starters": ["sp-4"]},
            ],
        },
    }
    path.write_text(json.dumps(payload), encoding="utf-8")


def test_emits_only_actual_playoff_starters_and_title_winner(tmp_path):
    base = tmp_path / "base.duckdb"
    source = tmp_path / "source.json"
    out = tmp_path / "delta.parquet"
    report = tmp_path / "report.json"
    _base(base)
    _source(source)

    result = build(base=base, source=source, out=out, report=report)

    assert result["source_playoff_starter_events"] == 6
    assert result["source_championship_starter_events"] == 1
    assert result["candidate_rows"] == 2
    con = duckdb.connect()
    rows = con.execute(
        "SELECT week, NFL_player_id, manager, team_key, source_playoffs, source_champion "
        "FROM read_parquet(?) ORDER BY week", [str(out)]
    ).fetchall()
    con.close()
    assert rows == [
        (14, "nfl-3", "Third", "league_3", 1, None),
        (15, "nfl-5", "Fifth", "league_5", 1, 1),
    ]


def test_blocks_ambiguous_cache_recipient_instead_of_fanning_out(tmp_path):
    base = tmp_path / "base.duckdb"
    source = tmp_path / "source.json"
    out = tmp_path / "delta.parquet"
    report = tmp_path / "report.json"
    _base(base, duplicate_week14=True)
    _source(source)

    result = build(base=base, source=source, out=out, report=report)

    assert result["ambiguous_recipient_events"] == 1
    assert result["candidate_rows"] == 1
    con = duckdb.connect()
    rows = con.execute(
        "SELECT week, NFL_player_id, source_champion FROM read_parquet(?)", [str(out)]
    ).fetchall()
    con.close()
    assert rows == [(15, "nfl-5", 1)]
