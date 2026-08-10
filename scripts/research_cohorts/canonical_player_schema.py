"""Validation for the frozen research player schema."""
from __future__ import annotations

BASE_PLAYER_COLUMNS = [
    "db_name", "year", "week", "NFL_player_id", "is_started", "is_rostered",
    "fantasy_points", "win", "champion", "clutch_equity", "manager_lamar",
    "manager", "team_points", "final_playoff_seed", "is_playoffs",
    "has_po_signal", "player", "position", "fantasy_position", "platform",
    "team_key", "team_name", "nfl_team_api", "yahoo_player_id",
    "sleeper_player_id", "espn_player_id", "fleaflicker_player_id",
    "mfl_player_id", "made_playoffs",
]
OPTIONAL_PLAYER_COLUMNS = {"loss", "tie"}
FORBIDDEN_PLAYER_COLUMNS = {
    "opponent_points", "is_championship", "is_active", "is_playoffs_bf",
    "made_po_bf", "made_po",
}
COHORT_DIMENSIONS = (
    "teams", "roster", "scoring", "pass_td", "playoff_teams", "dynasty",
    "best_ball", "position_eligible",
)
COHORT_GROUPS = tuple(str(i) for i in range(1, 13)) + ("",)
EXPECTED_COHORT_COLUMNS = {
    f"cohort_{dimension}{'_' + group if group else ''}"
    for group in COHORT_GROUPS
    for dimension in COHORT_DIMENSIONS
}


def validate_player_schema(columns: list[str], *, allow_test_fixture: bool = False) -> tuple[list[str], list[str]]:
    """Return cohort and optional columns, rejecting schema drift."""
    pcols = list(columns)
    missing = sorted(set(BASE_PLAYER_COLUMNS) - set(pcols))
    optional = [c for c in pcols if c in OPTIONAL_PLAYER_COLUMNS]
    cohorts = [c for c in pcols if c not in BASE_PLAYER_COLUMNS and c not in OPTIONAL_PLAYER_COLUMNS]
    forbidden = sorted(FORBIDDEN_PLAYER_COLUMNS & set(pcols))
    synthetic = {f"cohort_{c}" for c in "abcdefg"}
    valid_cohorts = set(cohorts) == EXPECTED_COHORT_COLUMNS or (allow_test_fixture and set(cohorts) == synthetic)
    if missing or not valid_cohorts or forbidden:
        raise ValueError(
            f"canonical schema mismatch: missing={missing} cohorts={cohorts} "
            f"optional={optional} forbidden={forbidden}"
        )
    return cohorts, optional
