"""Validation gates for one-year Yahoo corpus imports."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import duckdb

from scripts.yahoo_corpus.scheduler import Candidate


REQUIRED_TABLES = {"league_settings", "draft", "transactions", "player_fantasy", "matchup"}


class SourceValidationError(RuntimeError):
    """A sanitized corpus source validation failure."""

    def __init__(self, message: str, *, code: str) -> None:
        super().__init__(message)
        self.code = code


def _cohort_slug(row: dict[str, Any]) -> str:
    teams = int(row.get("num_teams") or 0)
    idp_columns = ("roster_IDP", "roster_DL", "roster_LB", "roster_DB", "roster_DB_LB", "roster_DL_LB")
    if sum(int(row.get(column) or 0) for column in idp_columns) > 0:
        roster = "idp"
    elif int(row.get("roster_SUPER_FLEX") or 0) > 0:
        roster = "sflx"
    else:
        roster = "flx"
    reception = float(row.get("scoring_rec") or 0)
    ppr = "ppr" if reception >= 0.75 else "half" if reception >= 0.25 else "std"
    passing = "6pt" if float(row.get("scoring_pass_td") or 0) >= 5 else "4pt"
    return f"{teams}t_{roster}_{ppr}_{passing}"


def validate_source(db_path: Path | str, task: Candidate) -> dict[str, Any]:
    """Validate source completeness, champion uniqueness, and settings cohort."""
    con = duckdb.connect(str(db_path), read_only=True)
    try:
        tables = {
            row[0]
            for row in con.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_schema='public'"
            ).fetchall()
        }
        missing = sorted(REQUIRED_TABLES - tables)
        if missing:
            # Table names are fixed constants from REQUIRED_TABLES, so they are
            # safe to surface in the sanitized error class. Dot separators keep
            # each segment short enough to pass the artifact token screen.
            raise SourceValidationError(
                f"missing required tables: {','.join(missing)}",
                code="MissingTables." + ".".join(missing),
            )
        settings_rows = con.execute(
            "SELECT * FROM public.league_settings WHERE year = ?", [task.season]
        ).fetchdf()
        if len(settings_rows) != 1:
            raise SourceValidationError(
                "league_settings year row count is not one",
                code="SettingsRowCount",
            )
        actual_cohort = _cohort_slug(settings_rows.iloc[0].to_dict())
        if actual_cohort != task.cohort_slug:
            raise SourceValidationError(
                f"cohort mismatch: expected {task.cohort_slug}, got {actual_cohort}",
                code="CohortMismatch",
            )
        rostered_rows = int(
            con.execute(
                "SELECT COUNT(*) FROM public.player_fantasy "
                "WHERE year = ? AND COALESCE(CAST(is_rostered AS INTEGER), 0) = 1",
                [task.season],
            ).fetchone()[0]
        )
        if rostered_rows <= 0:
            raise SourceValidationError(
                "rostered player population is empty",
                code="EmptyRosterPopulation",
            )
        champions = int(
            con.execute(
                "SELECT COUNT(DISTINCT franchise_id) FROM public.matchup "
                "WHERE year = ? AND COALESCE(CAST(champion AS INTEGER), 0) = 1",
                [task.season],
            ).fetchone()[0]
        )
        if champions != 1:
            raise SourceValidationError(
                f"champion franchise count is {champions}, expected one",
                code="ChampionCount",
            )
        return {
            "cohort_slug": actual_cohort,
            "rostered_rows": rostered_rows,
            "champions": champions,
        }
    finally:
        con.close()
