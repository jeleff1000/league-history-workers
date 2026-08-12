from __future__ import annotations


class _FakeMFL:
    def __init__(self) -> None:
        self.calls: list[tuple[str, int, int | None]] = []

    def fetch_league(self, league_id: str, year: int):
        self.calls.append((league_id, year, None))
        if (league_id, year) != ("456", 2004):
            return None
        return {
            "franchises": {
                "franchise": [
                    {"id": "0001", "owner_name": "Owner One", "name": "The One"},
                    {"id": "0002", "name": "Named Franchise"},
                ]
            }
        }

    def fetch_weekly_results(self, league_id: str, year: int, week: int):
        self.calls.append((league_id, year, week))
        if (league_id, year, week) != ("456", 2004, 14):
            return None
        return {
            "matchup": [{"franchise": [
                {"id": "0001", "player": [
                    {"id": "10", "status": "starter", "score": "21.5"},
                    {"id": "11", "status": "nonstarter", "score": "5.2"},
                ]},
            ]}],
            "franchise": [{"id": "0002", "player": [{"id": "12", "status": "starter"}]}],
        }

    def fetch_players(self, league_id: str, year: int):
        self.calls.append((league_id, year, -1))
        if (league_id, year) != ("456", 2004):
            return {}
        return {
            "10": {"id": "10", "espn_id": "100", "name": "One, Player", "position": "RB", "team": "NEP"},
            "11": {"id": "11", "espn_id": "101", "name": "Two, Player", "position": "WR", "team": "KCC"},
            "12": {"id": "12", "name": "Team, DST", "position": "Def", "team": "GBP"},
        }


def test_resolver_tries_historical_candidates_and_preserves_raw_franchise_provenance() -> None:
    from scripts.research_cohorts.extract_mfl_target_roster_memberships import extract_group

    client = _FakeMFL()
    result = extract_group({
        "db_name": "smpl_mfl_2015_456", "year": 2004, "week": 14, "source_id": "123",
    }, client)

    assert result["status"] == "resolved"
    assert result["source_id_used"] == "456"
    assert result["source_year_used"] == 2004
    assert result["candidate_provenance"] == "seed_id_target_year"
    assert result["attempted_candidates"] == [
        {"source_id": "123", "source_year": 2004, "status": "no_league"},
        {"source_id": "456", "source_year": 2004, "status": "resolved"},
    ]
    assert result["memberships"] == [
        {
            "db_name": "smpl_mfl_2015_456", "year": 2004, "week": 14,
            "source_id": "456", "source_year": 2004, "source_franchise_id": "0001",
            "source_manager": "Owner One", "source_manager_origin": "mfl_franchise_owner_name",
            "mfl_player_id": "10", "is_started": 1,
        },
        {
            "db_name": "smpl_mfl_2015_456", "year": 2004, "week": 14,
            "source_id": "456", "source_year": 2004, "source_franchise_id": "0001",
            "source_manager": "Owner One", "source_manager_origin": "mfl_franchise_owner_name",
            "mfl_player_id": "11", "is_started": 0,
        },
        {
            "db_name": "smpl_mfl_2015_456", "year": 2004, "week": 14,
            "source_id": "456", "source_year": 2004, "source_franchise_id": "0002",
            "source_manager": "Named Franchise", "source_manager_origin": "mfl_franchise_name",
            "mfl_player_id": "12", "is_started": 1,
        },
    ]
    assert result["player_directory"] == [
        {"source_year": 2004, "mfl_player_id": "10", "espn_id": "100", "raw_name": "One, Player", "raw_position": "RB", "raw_team": "NEP"},
        {"source_year": 2004, "mfl_player_id": "11", "espn_id": "101", "raw_name": "Two, Player", "raw_position": "WR", "raw_team": "KCC"},
        {"source_year": 2004, "mfl_player_id": "12", "espn_id": None, "raw_name": "Team, DST", "raw_position": "Def", "raw_team": "GBP"},
    ]
