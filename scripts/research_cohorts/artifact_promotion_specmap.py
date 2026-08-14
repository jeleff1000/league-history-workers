"""Fixed source-grain contracts for the approved research cache.

This module is deliberately small and pure: it does not open a cache, change
the ledger, download an artifact, or choose source precedence.  It answers one
question consistently for every artifact schema: what canonical grain, if any,
can this source safely target?
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable


PLAYER_KEY = ("db_name", "year", "week", "NFL_player_id")
TEAM_KEY = ("db_name", "year", "week", "team_key")
MANAGER_TEAM_KEY = ("db_name", "year", "week", "manager")
LEAGUE_YEAR_KEY = ("db_name", "year")

PLAYER_COLUMNS = (
    "is_rostered", "is_started", "fantasy_points", "win", "loss", "tie",
    "team_points", "is_playoffs", "champion", "clutch_equity",
    "final_playoff_seed", "made_playoffs", "has_po_signal",
)
TEAM_SIGNAL_COLUMNS = (
    "win", "loss", "tie", "team_points", "is_playoffs", "champion",
    "final_playoff_seed", "made_playoffs", "has_po_signal",
)
# Source setting names deliberately differ from canonical cache columns.  These
# are the seven requested league-year dimensions; their canonical targets are
# the unsuffixed cohort columns that later pooling expands deterministically.
SETTING_SOURCE_TO_TARGET = (
    ("team_count", "cohort_teams"),
    ("ppr", "cohort_scoring"),
    ("pass_td", "cohort_pass_td"),
    ("playoff_teams", "cohort_playoff_teams"),
    ("roster_config", "cohort_roster"),
    ("format", "cohort_best_ball"),
    ("draft_style", "cohort_dynasty"),
)


@dataclass(frozen=True)
class PromotionContract:
    """The only permitted source-to-canonical promotion contracts."""

    grain: str
    source_key: tuple[str, ...]
    cache_key: tuple[str, ...]
    allowed_target_columns: tuple[str, ...]
    may_insert_missing_player_rows: bool = False
    terminal_reason: str = ""


@dataclass(frozen=True)
class BoundPromotionContract:
    """A source contract constrained by the restored canonical schema."""

    contract: PromotionContract
    writable_target_columns: tuple[str, ...]
    blocked_source_columns: tuple[str, ...]
    cache_key_present: bool


def _present(columns: set[str], ordered: tuple[str, ...]) -> tuple[str, ...]:
    return tuple(column for column in ordered if column in columns)


def classify_source_schema(columns: Iterable[str]) -> PromotionContract:
    """Classify an artifact's actual columns without guessing a recipient.

    A source is only a player-row insertion candidate when it proves both
    player identity and a roster/start state.  A team-week source can enrich
    existing player rows but can never create a player row by outcome alone.
    """
    present = {str(column) for column in columns}

    if set(PLAYER_KEY) <= present:
        allowed = _present(present, PLAYER_COLUMNS)
        has_roster_payload = "is_rostered" in present or "is_started" in present
        return PromotionContract(
            grain="player_week",
            source_key=PLAYER_KEY,
            cache_key=PLAYER_KEY,
            allowed_target_columns=allowed,
            may_insert_missing_player_rows=has_roster_payload,
        )

    if set(TEAM_KEY) <= present:
        return PromotionContract(
            grain="team_week",
            source_key=TEAM_KEY,
            cache_key=TEAM_KEY,
            allowed_target_columns=_present(present, TEAM_SIGNAL_COLUMNS),
        )

    # Manager is the valid alternate fantasy-team identity.  It fans a single
    # team-week signal to the cache's player rows for that manager/week; it is
    # not a player identity and therefore never authorizes row insertion.
    if set(MANAGER_TEAM_KEY) <= present:
        return PromotionContract(
            grain="team_week_manager",
            source_key=MANAGER_TEAM_KEY,
            cache_key=MANAGER_TEAM_KEY,
            allowed_target_columns=_present(present, TEAM_SIGNAL_COLUMNS),
        )

    if set(LEAGUE_YEAR_KEY) <= present and any(
        source in present for source, _ in SETTING_SOURCE_TO_TARGET
    ):
        return PromotionContract(
            grain="league_year_settings",
            source_key=LEAGUE_YEAR_KEY,
            cache_key=LEAGUE_YEAR_KEY,
            allowed_target_columns=tuple(
                target for source, target in SETTING_SOURCE_TO_TARGET if source in present
            ),
        )

    if {"db_name", "year", "week"} <= present and any(
        column in present for column in TEAM_SIGNAL_COLUMNS
    ):
        return PromotionContract(
            grain="unattributable_team_signal",
            source_key=(),
            cache_key=(),
            allowed_target_columns=(),
            terminal_reason="missing_team_identity",
        )

    return PromotionContract(
        grain="aggregate_only",
        source_key=(),
        cache_key=(),
        allowed_target_columns=(),
        terminal_reason="no_supported_row_level_contract",
    )


def bind_to_canonical_schema(
    contract: PromotionContract, canonical_columns: Iterable[str],
) -> BoundPromotionContract:
    """Restrict a contract to fields that exist in this cache restore.

    This prevents a receipt or a stale workflow from silently treating a
    historically available source field as a writable canonical field.
    """
    present = {str(column) for column in canonical_columns}
    writable = tuple(
        column for column in contract.allowed_target_columns if column in present
    )
    blocked = tuple(
        column for column in contract.allowed_target_columns if column not in present
    )
    return BoundPromotionContract(
        contract=contract,
        writable_target_columns=writable,
        blocked_source_columns=blocked,
        cache_key_present=all(column in present for column in contract.cache_key),
    )
