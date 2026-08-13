"""Build a NULL-fill-only player delta from one strict Sleeper playoff snapshot.

This is deliberately not a general corpus fetcher.  It accepts a saved raw
Sleeper league/bracket/matchup snapshot for one known season, verifies the
single-week bracket structure, and follows only actual winner-bracket matchup
pairs.  A source starter receives playoff credit only when its roster played
that pair; championship credit additionally requires the title-pair winner.

The output contains existing canonical player keys only.  It cannot insert
players, change schema, create a lineage, or overwrite a non-NULL cache value.
"""
from __future__ import annotations

import argparse
import json
from collections import defaultdict
from pathlib import Path
from typing import Any

import duckdb

try:
    from .canonical_player_schema import validate_player_schema
except ImportError:  # direct script execution in Actions
    from canonical_player_schema import validate_player_schema


def _write_json(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _integer(value: Any, *, field: str) -> int:
    try:
        return int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"invalid {field}: {value!r}") from exc


def _roster_id(value: Any) -> str:
    return str(_integer(value, field="roster_id"))


def _team_key_roster_id(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    suffix = text.rsplit("_", 1)[-1]
    try:
        return str(int(suffix))
    except ValueError:
        return None


def _winner_pair(matchup: dict[str, Any]) -> tuple[str, str]:
    if matchup.get("t1") is None or matchup.get("t2") is None:
        raise ValueError(f"winner-bracket matchup lacks both rosters: {matchup!r}")
    return tuple(sorted((_roster_id(matchup["t1"]), _roster_id(matchup["t2"]))))


def _is_title_path(matchup: dict[str, Any]) -> bool:
    placement = matchup.get("p")
    return placement is None or _integer(placement, field="bracket placement") == 1


def _source_events(payload: dict[str, Any]) -> tuple[list[dict[str, Any]], dict[str, int]]:
    league = payload.get("league") or {}
    settings = league.get("settings") or {}
    year = _integer(payload.get("year"), field="payload year")
    if _integer(league.get("season"), field="league season") != year:
        raise ValueError("payload year and source league season differ")
    raw_round_type = settings.get("playoff_round_type")
    round_type = 0 if raw_round_type is None else _integer(raw_round_type, field="playoff_round_type")
    if round_type != 0:
        raise ValueError("exact candidate only accepts Sleeper single-week playoff rounds")
    start_week = _integer(settings.get("playoff_week_start"), field="playoff_week_start")
    if start_week < 1:
        raise ValueError("playoff_week_start must be positive")

    title_path = [item for item in (payload.get("winners_bracket") or []) if _is_title_path(item)]
    if not title_path:
        raise ValueError("winners bracket has no title-path matchups")
    title_round = max(_integer(item.get("r"), field="bracket round") for item in title_path)
    title_matches = [
        item for item in title_path
        if _integer(item.get("r"), field="bracket round") == title_round
    ]
    if len(title_matches) != 1:
        raise ValueError(f"expected one title matchup in final round, found {len(title_matches)}")
    title_match = title_matches[0]
    title_pair = _winner_pair(title_match)
    title_winner = _roster_id(title_match.get("w"))
    if title_winner not in title_pair:
        raise ValueError("title winner is not one of the title matchup rosters")

    matchups_by_week = payload.get("matchups_by_week") or {}
    source_players = payload.get("players") or {}
    if not isinstance(source_players, dict):
        raise ValueError("source players directory must be an object")
    source_events: dict[tuple[int, str, str], dict[str, Any]] = {}
    playoff_starter_events = 0
    championship_starter_events = 0

    for bracket_matchup in title_path:
        round_number = _integer(bracket_matchup.get("r"), field="bracket round")
        week = start_week + round_number - 1
        pair = _winner_pair(bracket_matchup)
        weekly_rows = matchups_by_week.get(str(week), matchups_by_week.get(week))
        if not isinstance(weekly_rows, list):
            raise ValueError(f"missing matchup rows for required playoff week {week}")
        # Historical Sleeper final-week matchup ids do not always preserve the
        # winner-bracket opponent pair.  The winners bracket is authoritative
        # for who played the title path; each roster's own starter list is the
        # authoritative evidence for which players started.  Do not invent an
        # opponent relation from a malformed matchup_id.
        by_roster: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for row in weekly_rows:
            if isinstance(row, dict) and row.get("roster_id") is not None:
                by_roster[_roster_id(row["roster_id"])].append(row)
        is_title = round_number == title_round
        for roster in pair:
            roster_rows = by_roster.get(roster, [])
            if len(roster_rows) != 1:
                raise ValueError(
                    f"week {week} has {len(roster_rows)} source rows for winner-bracket roster {roster}"
                )
            row = roster_rows[0]
            starters = row.get("starters") or []
            if not isinstance(starters, list):
                raise ValueError(f"week {week} roster {roster} starters is not a list")
            for sleeper_player_id in starters:
                if sleeper_player_id is None or not str(sleeper_player_id).strip():
                    continue
                playoff_starter_events += 1
                champion = 1 if is_title and roster == title_winner else None
                if champion:
                    championship_starter_events += 1
                key = (week, roster, str(sleeper_player_id))
                source_player = source_players.get(str(sleeper_player_id)) or {}
                if not isinstance(source_player, dict):
                    raise ValueError(f"source player entry is not an object: {sleeper_player_id}")
                espn_player_id = source_player.get("espn_id")
                nfl_player_id = source_player.get("gsis_id")
                current = source_events.setdefault(key, {
                    "week": week,
                    "source_roster_id": roster,
                    "sleeper_player_id": str(sleeper_player_id),
                    "source_espn_player_id": str(espn_player_id).strip() if espn_player_id is not None and str(espn_player_id).strip() else None,
                    "source_nfl_player_id": str(nfl_player_id).strip() if nfl_player_id is not None and str(nfl_player_id).strip() else None,
                    "source_playoffs": 1,
                    "source_champion": None,
                })
                if champion:
                    current["source_champion"] = 1
    return list(source_events.values()), {
        "source_playoff_starter_events": playoff_starter_events,
        "source_championship_starter_events": championship_starter_events,
    }


def build(*, base: Path, source: Path, out: Path, report: Path) -> dict[str, Any]:
    payload = json.loads(source.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("source payload must be a JSON object")
    db_name = str(payload.get("db_name") or "").strip()
    if not db_name:
        raise ValueError("source payload lacks db_name")
    year = _integer(payload.get("year"), field="payload year")
    events, event_counts = _source_events(payload)

    con = duckdb.connect(str(base), read_only=True)
    player_columns = [row[0] for row in con.execute("DESCRIBE public.player_fantasy").fetchall()]
    validate_player_schema(player_columns)
    canonical_player_rows_for_league_year = int(con.execute(
        "SELECT COUNT(*) FROM public.player_fantasy WHERE db_name=? AND CAST(year AS INTEGER)=?",
        [db_name, year],
    ).fetchone()[0])
    required = {"sleeper_player_id", "team_key", "platform", "is_playoffs", "champion", "has_po_signal"}
    if missing := sorted(required - set(player_columns)):
        raise ValueError(f"canonical player schema lacks source-recipient fields: {missing}")

    con.execute("CREATE OR REPLACE TEMP TABLE source_events (week INTEGER, source_roster_id VARCHAR, sleeper_player_id VARCHAR, source_espn_player_id VARCHAR, source_nfl_player_id VARCHAR, source_playoffs INTEGER, source_champion INTEGER)")
    if events:
        con.executemany(
            "INSERT INTO source_events VALUES (?, ?, ?, ?, ?, ?, ?)",
            [
                (event["week"], event["source_roster_id"], event["sleeper_player_id"], event["source_espn_player_id"], event["source_nfl_player_id"], event["source_playoffs"], event["source_champion"])
                for event in events
            ],
        )
    con.execute("""
      CREATE OR REPLACE TEMP TABLE recipient_matches AS
      SELECT e.*, p.rowid AS player_rowid, p.NFL_player_id, p.manager, p.team_key,
             p.team_name, p.platform, p.sleeper_player_id AS canonical_sleeper_player_id,
             p.espn_player_id AS canonical_espn_player_id, p.is_playoffs AS canonical_is_playoffs,
             p.champion AS canonical_champion, p.has_po_signal AS canonical_has_po_signal
      FROM source_events e
      JOIN public.player_fantasy p
        ON p.db_name = ?
       AND CAST(p.year AS INTEGER) = ?
       AND CAST(p.week AS INTEGER) = e.week
       AND LOWER(TRIM(CAST(p.platform AS VARCHAR))) = 'sleeper'
       AND (
            CAST(p.sleeper_player_id AS VARCHAR) = e.sleeper_player_id
            OR (
                e.source_espn_player_id IS NOT NULL
                AND CAST(p.espn_player_id AS VARCHAR) = e.source_espn_player_id
            )
            OR (
                e.source_nfl_player_id IS NOT NULL
                AND CAST(p.NFL_player_id AS VARCHAR) = e.source_nfl_player_id
            )
       )
       AND CASE WHEN p.team_key IS NULL THEN NULL
                WHEN INSTR(TRIM(CAST(p.team_key AS VARCHAR)), '_') > 0
                  THEN REGEXP_EXTRACT(TRIM(CAST(p.team_key AS VARCHAR)), '([^_]+)$', 1)
                ELSE TRIM(CAST(p.team_key AS VARCHAR)) END = e.source_roster_id
    """, [db_name, year])
    con.execute("""
      CREATE OR REPLACE TEMP TABLE recipient_cardinality AS
      SELECT week, source_roster_id, sleeper_player_id, COUNT(*) AS recipient_rows
      FROM recipient_matches
      GROUP BY 1,2,3
    """)
    con.execute("""
      CREATE OR REPLACE TEMP TABLE matched AS
      SELECT r.*, c.recipient_rows
      FROM recipient_matches r
      JOIN recipient_cardinality c USING (week, source_roster_id, sleeper_player_id)
    """)
    unmatched_events = int(con.execute("""
      SELECT COUNT(*) FROM source_events e
      WHERE NOT EXISTS (
        SELECT 1 FROM recipient_cardinality c
        WHERE c.week=e.week AND c.source_roster_id=e.source_roster_id
          AND c.sleeper_player_id=e.sleeper_player_id
      )
    """).fetchone()[0])
    ambiguous_events = int(con.execute("SELECT COUNT(*) FROM recipient_cardinality WHERE recipient_rows > 1").fetchone()[0])
    direct_native_events = int(con.execute("""
      SELECT COUNT(*) FROM recipient_cardinality c
      JOIN source_events e USING (week, source_roster_id, sleeper_player_id)
      JOIN recipient_matches r USING (week, source_roster_id, sleeper_player_id)
      WHERE c.recipient_rows=1 AND CAST(r.canonical_sleeper_player_id AS VARCHAR)=e.sleeper_player_id
    """).fetchone()[0])
    espn_bridge_events = int(con.execute("""
      SELECT COUNT(*) FROM recipient_cardinality c
      JOIN source_events e USING (week, source_roster_id, sleeper_player_id)
      JOIN recipient_matches r USING (week, source_roster_id, sleeper_player_id)
      WHERE c.recipient_rows=1
        AND CAST(r.canonical_sleeper_player_id AS VARCHAR) IS DISTINCT FROM e.sleeper_player_id
        AND e.source_espn_player_id IS NOT NULL
        AND CAST(r.canonical_espn_player_id AS VARCHAR)=e.source_espn_player_id
    """).fetchone()[0])
    gsis_bridge_events = int(con.execute("""
      SELECT COUNT(*) FROM recipient_cardinality c
      JOIN source_events e USING (week, source_roster_id, sleeper_player_id)
      JOIN recipient_matches r USING (week, source_roster_id, sleeper_player_id)
      WHERE c.recipient_rows=1
        AND CAST(r.canonical_sleeper_player_id AS VARCHAR) IS DISTINCT FROM e.sleeper_player_id
        AND (e.source_espn_player_id IS NULL OR CAST(r.canonical_espn_player_id AS VARCHAR) IS DISTINCT FROM e.source_espn_player_id)
        AND e.source_nfl_player_id IS NOT NULL
        AND CAST(r.NFL_player_id AS VARCHAR)=e.source_nfl_player_id
    """).fetchone()[0])
    cache_equal_playoffs = int(con.execute("SELECT COUNT(*) FROM matched WHERE recipient_rows=1 AND canonical_is_playoffs IS NOT NULL").fetchone()[0])
    cache_equal_champions = int(con.execute("SELECT COUNT(*) FROM matched WHERE recipient_rows=1 AND source_champion=1 AND canonical_champion IS NOT NULL").fetchone()[0])
    cache_equal_signal = int(con.execute("SELECT COUNT(*) FROM matched WHERE recipient_rows=1 AND canonical_has_po_signal IS NOT NULL").fetchone()[0])
    candidate_filter = "recipient_rows=1 AND (canonical_is_playoffs IS NULL OR (source_champion=1 AND canonical_champion IS NULL) OR canonical_has_po_signal IS NULL)"
    out.parent.mkdir(parents=True, exist_ok=True)
    escaped = str(out.resolve()).replace("'", "''")
    con.execute(f"""
      COPY (
        SELECT CAST(? AS VARCHAR) AS db_name,
               CAST(? AS INTEGER) AS year,
               CAST(week AS INTEGER) AS week,
               CAST(NFL_player_id AS VARCHAR) AS NFL_player_id,
               CAST(manager AS VARCHAR) AS manager,
               CAST(team_key AS VARCHAR) AS team_key,
               CAST(team_name AS VARCHAR) AS team_name,
               CAST(platform AS VARCHAR) AS platform,
               NULL::INTEGER AS source_win,
               NULL::INTEGER AS source_loss,
               NULL::INTEGER AS source_tie,
               NULL::DOUBLE AS source_team_points,
               CASE WHEN canonical_is_playoffs IS NULL THEN source_playoffs ELSE NULL::INTEGER END AS source_playoffs,
               CASE WHEN canonical_champion IS NULL THEN source_champion ELSE NULL::INTEGER END AS source_champion,
               NULL::INTEGER AS source_final_playoff_seed,
               CASE WHEN canonical_has_po_signal IS NULL THEN 1 ELSE NULL::INTEGER END AS source_has_po_signal,
               NULL::INTEGER AS source_made_playoffs,
               NULL::DOUBLE AS source_clutch_equity,
               CAST(? AS VARCHAR) AS source_file
        FROM matched
        WHERE {candidate_filter}
      ) TO '{escaped}' (FORMAT PARQUET)
    """, [db_name, year, str(source.name)])
    candidate_rows = int(con.execute(f"SELECT COUNT(*) FROM read_parquet('{escaped}')").fetchone()[0])
    duplicate_candidates = int(con.execute(f"SELECT COUNT(*) FROM (SELECT db_name,year,week,NFL_player_id,manager,team_key,team_name,platform FROM read_parquet('{escaped}') GROUP BY ALL HAVING COUNT(*)>1)").fetchone()[0])
    if duplicate_candidates:
        raise RuntimeError(f"candidate contains duplicate canonical keys: {duplicate_candidates}")
    result = {
        "read_only": True,
        "cache_mutated": False,
        "new_lineage": False,
        "schema_unchanged": True,
        "db_name": db_name,
        "year": year,
        "canonical_player_rows_for_league_year": canonical_player_rows_for_league_year,
        **event_counts,
        "source_unique_starter_events": len(events),
        "unmatched_recipient_events": unmatched_events,
        "ambiguous_recipient_events": ambiguous_events,
        "direct_native_recipient_events": direct_native_events,
        "espn_bridge_recipient_events": espn_bridge_events,
        "gsis_bridge_recipient_events": gsis_bridge_events,
        "cache_non_null_playoff_recipients": cache_equal_playoffs,
        "cache_non_null_champion_recipients": cache_equal_champions,
        "cache_non_null_playoff_signal_recipients": cache_equal_signal,
        "candidate_rows": candidate_rows,
    }
    _write_json(report, result)
    con.close()
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base", type=Path, required=True)
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--report", type=Path, required=True)
    args = parser.parse_args()
    print(json.dumps(build(base=args.base, source=args.source, out=args.out, report=args.report), sort_keys=True))


if __name__ == "__main__":
    main()
