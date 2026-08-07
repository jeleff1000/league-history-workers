"""Player-table-only gap inventory for the canonical research lake."""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import duckdb


def _first(candidates: tuple[str, ...], available: set[str]) -> str | None:
    return next((c for c in candidates if c in available), None)


def _qi(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def audit(snapshot: Path) -> dict[str, object]:
    con = duckdb.connect(str(snapshot), read_only=True)
    tables = {r[0] for r in con.execute("SELECT table_name FROM duckdb_tables() WHERE schema_name='public'").fetchall()}
    if "player_fantasy" not in tables:
        raise SystemExit("canonical lake has no public.player_fantasy table")
    available = {r[0] for r in con.execute("DESCRIBE public.player_fantasy").fetchall()}
    missing = {"db_name", "year", "week", "is_started"} - available
    if missing:
        raise SystemExit(f"player_fantasy missing required columns: {sorted(missing)}")

    win = _first(("win", "is_win", "won"), available)
    loss = _first(("loss", "is_loss", "lost"), available)
    tie = _first(("tie", "is_tie", "tied"), available)
    team_points = _first(("team_points", "fantasy_team_points", "manager_points"), available)
    opponent_points = _first(("opponent_points", "opp_points"), available)
    playoff = _first(("is_playoffs", "is_playoff", "playoff", "playoff_start", "started_playoffs"), available)
    championship = _first(("is_championship", "is_champ", "championship", "champ_start", "started_championship"), available)
    clutch = _first(("clutch_equity", "clutch", "clutch_value"), available)
    # *_bf is the populated player-level playoff flag in the canonical lake.
    # has_po_signal and made_po are present in the schema but null in this
    # lineage, so they must not win candidate selection.
    po_signal = _first(("is_playoffs_bf", "is_playoffs", "is_playoff", "playoff"), available)
    playoff_flag = _first(("is_playoffs_bf", "is_playoffs", "is_playoff", "playoff"), available)
    made_po = _first(("made_po",), available)
    playoff_seed = _first(("final_playoff_seed", "playoff_seed"), available)
    champion_marker = _first(("champion", "is_champion"), available)

    outcome_terms = []
    if win:
        # The canonical player table stores win as a 0/1 result.  A zero is a
        # confirmed loss, so only NULL is an unresolved outcome.
        outcome_terms.append(f"{_qi(win)} IS NOT NULL")
    elif loss:
        tie_term = f" OR {_qi(tie)} IS NOT NULL" if tie else ""
        outcome_terms.append(f"({_qi(loss)} IS NOT NULL{tie_term})")
    if not win and team_points and opponent_points:
        outcome_terms.append(f"({_qi(team_points)} IS NOT NULL AND {_qi(opponent_points)} IS NOT NULL)")
    outcome_present = " OR ".join(outcome_terms) if outcome_terms else "FALSE"

    def null_signal(col: str | None) -> str:
        return f"{_qi(col)} IS NULL" if col else "FALSE"

    select = [
        "CAST(db_name AS VARCHAR) AS db_name",
        "CAST(year AS INTEGER) AS year",
        "COUNT(*) AS player_rows",
        "COUNT(*) FILTER (WHERE CAST(is_started AS INTEGER)=1) AS started_rows",
        "COUNT(*) FILTER (WHERE is_started IS NULL) AS null_start_rows",
        f"COUNT(*) FILTER (WHERE CAST(is_started AS INTEGER)=1 AND NOT ({outcome_present})) AS started_missing_outcome",
        f"COUNT(*) FILTER (WHERE CAST(is_started AS INTEGER)=1 AND {null_signal(playoff)}) AS started_missing_playoff",
        f"COUNT(*) FILTER (WHERE CAST(is_started AS INTEGER)=1 AND {null_signal(championship)}) AS started_missing_championship",
        f"COUNT(*) FILTER (WHERE CAST(is_started AS INTEGER)=1 AND {null_signal(clutch)}) AS started_missing_clutch",
    ]
    if team_points and opponent_points:
        select.append(f"COUNT(*) FILTER (WHERE CAST(is_started AS INTEGER)=1 AND ({_qi(team_points)} IS NULL OR {_qi(opponent_points)} IS NULL)) AS started_missing_team_game_denominator")
    else:
        select.append("NULL::BIGINT AS started_missing_team_game_denominator")

    rows = con.execute("SELECT " + ", ".join(select) + " FROM public.player_fantasy GROUP BY 1,2 ORDER BY 1,2").fetchall()
    names = [d[0] for d in con.description]
    totals = con.execute("SELECT " + ", ".join(select[2:]) + " FROM public.player_fantasy").fetchone()
    signal_select = [
        "CAST(db_name AS VARCHAR) AS db_name",
        "CAST(year AS INTEGER) AS year",
        "COUNT(*) AS player_rows",
        "COUNT(*) FILTER (WHERE CAST(is_started AS INTEGER)=1) AS started_rows",
    ]
    for alias, field in (("playoff_flag_rows", playoff_flag), ("made_po_rows", made_po), ("po_signal_rows", po_signal)):
        signal_select.append(f"COUNT(*) FILTER (WHERE CAST({_qi(field)} AS INTEGER)=1) AS {alias}" if field else f"0 AS {alias}")
    if playoff_seed:
        signal_select.append(f"COUNT(*) FILTER (WHERE {_qi(playoff_seed)} IS NOT NULL) AS playoff_seed_rows")
    else:
        signal_select.append("0 AS playoff_seed_rows")
    if champion_marker:
        signal_select.append(f"COUNT(*) FILTER (WHERE CAST({_qi(champion_marker)} AS INTEGER)=1) AS champion_marked_rows")
    else:
        signal_select.append("0 AS champion_marked_rows")
    signal_rows_raw = con.execute("SELECT " + ", ".join(signal_select) + " FROM public.player_fantasy GROUP BY 1,2 ORDER BY 1,2").fetchall()
    signal_names = [d[0] for d in con.description]
    signal_rows = [dict(zip(signal_names, row)) for row in signal_rows_raw]
    settings_cols = {r[0] for r in con.execute("DESCRIBE public.league_settings").fetchall()} if "league_settings" in tables else set()
    settings_bracket = _first(("playoff_teams", "num_playoff_teams", "playoff_team_count", "playoff_bracket_size", "playoff_slots"), settings_cols)
    if settings_bracket:
        configured = con.execute(f"SELECT CAST(db_name AS VARCHAR), CAST(year AS INTEGER), MAX(TRY_CAST({_qi(settings_bracket)} AS DOUBLE)) FROM public.league_settings GROUP BY 1,2").fetchall()
        configured_map = {(r[0], r[1]): r[2] for r in configured}
    else:
        configured_map = {}
    for row in signal_rows:
        row["configured_playoff_teams"] = configured_map.get((row["db_name"], row["year"]))
        row["player_playoff_signal"] = bool(row["playoff_flag_rows"] or row["made_po_rows"] or row["po_signal_rows"] or row["playoff_seed_rows"])
        row["player_champion_signal"] = bool(row["champion_marked_rows"])
        if not settings_bracket:
            row["playoff_signal_vs_settings"] = "no_settings_bracket_field"
        else:
            expected = row["configured_playoff_teams"] is not None and row["configured_playoff_teams"] > 0
            row["playoff_signal_vs_settings"] = "consistent_presence" if expected == row["player_playoff_signal"] else "signal_settings_mismatch"
    signal_distributions = {}
    for field in ("is_playoffs", "is_playoffs_bf", "has_po_signal", "made_po", "final_playoff_seed", "champion"):
        if field in available:
            values = con.execute(f"SELECT CAST({_qi(field)} AS VARCHAR) AS value, COUNT(*) AS rows FROM public.player_fantasy GROUP BY 1 ORDER BY rows DESC LIMIT 20").fetchall()
            signal_distributions[field] = [{"value": value, "rows": int(count)} for value, count in values]
    result = {
        "population_source": "public.player_fantasy only",
        "player_rows": int(con.execute("SELECT COUNT(*) FROM public.player_fantasy").fetchone()[0]),
        "player_league_years": int(con.execute("SELECT COUNT(*) FROM (SELECT db_name, year FROM public.player_fantasy GROUP BY 1,2)").fetchone()[0]),
        "resolved_columns": {"win": win, "loss": loss, "tie": tie, "team_points": team_points, "opponent_points": opponent_points, "playoff": playoff, "championship": championship, "clutch": clutch, "player_playoff_signal": po_signal, "playoff_seed": playoff_seed, "player_champion_marker": champion_marker, "settings_playoff_teams": settings_bracket},
        "unavailable_player_fields": [name for name, value in {"loss": loss, "tie": tie, "opponent_points": opponent_points, "championship_start": championship}.items() if value is None],
        "available_columns": sorted(available),
        "totals": dict(zip(names[2:], totals)),
        "league_season_signal_totals": {
            "player_league_years": len(signal_rows),
            "with_player_playoff_signal": sum(r["player_playoff_signal"] for r in signal_rows),
            "with_player_champion_signal": sum(r["player_champion_signal"] for r in signal_rows),
            "signal_settings_mismatch": sum(r["playoff_signal_vs_settings"] == "signal_settings_mismatch" for r in signal_rows),
        },
        "player_signal_distributions": signal_distributions,
        "league_season_signal_rows": signal_rows,
        "league_season_rows": [dict(zip(names, row)) for row in rows],
    }
    con.close()
    return result


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--snapshot", type=Path, required=True)
    ap.add_argument("--out", type=Path, required=True)
    args = ap.parse_args()
    result = audit(args.snapshot)
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(result, indent=2, default=str), encoding="utf-8")
    print(json.dumps({"player_rows": result["player_rows"], "player_league_years": result["player_league_years"], "totals": result["totals"], "resolved_columns": result["resolved_columns"]}, indent=2))


if __name__ == "__main__":
    main()
