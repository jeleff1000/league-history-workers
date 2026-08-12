"""Build exact-schema, source-proven MFL player rows that are absent from cache.

This is deliberately an *addition* builder.  It does not update a cache and it
does not make a new schema.  Every output field is either direct MFL/ops data,
or a single consistent value already carried by the existing team-week.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import duckdb


KEY = ("db_name", "year", "week", "NFL_player_id", "manager")
TEAM_WEEK_FIELDS = (
    "win", "champion", "team_points", "final_playoff_seed", "is_playoffs",
    "has_po_signal", "made_playoffs", "platform", "team_key", "team_name",
    "cohort_teams", "cohort_roster", "cohort_scoring", "cohort_pass_td",
    "cohort_playoff_teams", "cohort_dynasty", "cohort_best_ball",
    "cohort_position_eligible",
)


def _q(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def _path(value: Path) -> str:
    return "'" + str(value.resolve()).replace("\\", "/").replace("'", "''") + "'"


def _columns(con: duckdb.DuckDBPyConnection, relation: str) -> list[tuple[str, str]]:
    return [(str(row[0]), str(row[1])) for row in con.execute(f"DESCRIBE {relation}").fetchall()]


def _slot(position: object, started: object) -> str:
    if not bool(started):
        return "BN"
    normalized = str(position or "").upper().strip()
    if normalized in {"DE", "DT", "DL"}:
        return "DL"
    if normalized in {"CB", "S", "DB"}:
        return "DB"
    if normalized in {"DST", "DEF"}:
        return "DEF"
    return normalized or "BN"


def _single_template(
    con: duckdb.DuckDBPyConnection, *, db_name: str, year: int, week: int, manager: str, columns: set[str]
) -> dict[str, Any]:
    fields = [field for field in TEAM_WEEK_FIELDS if field in columns]
    if not fields:
        raise ValueError("canonical schema has no team-week fields")
    select = ", ".join(
        f"COUNT(DISTINCT {_q(field)}) FILTER (WHERE {_q(field)} IS NOT NULL) AS {_q(field)}"
        for field in fields
    )
    params = [db_name, year, week, manager]
    counts = con.execute(
        "SELECT COUNT(*) AS rows, " + select + " FROM public.player_fantasy "
        "WHERE db_name=? AND year=? AND week=? AND manager=?", params
    ).fetchone()
    if not counts or int(counts[0]) < 1:
        raise ValueError(f"no existing canonical team-week template for {db_name}/{year}/W{week}/{manager}")
    ambiguous = [field for index, field in enumerate(fields, start=1) if int(counts[index]) > 1]
    if ambiguous:
        raise ValueError(
            f"ambiguous canonical team-week fields for {db_name}/{year}/W{week}/{manager}: {ambiguous}"
        )
    templates = con.execute(
        "SELECT " + ", ".join(_q(field) for field in fields) + " FROM public.player_fantasy "
        "WHERE db_name=? AND year=? AND week=? AND manager=? LIMIT 1", params
    ).fetchone()
    return dict(zip(fields, templates))


def build(
    *, base: Path, ops: Path, memberships: Path, directory: Path, crosswalk: Path, targets: Path,
    out: Path, report: Path, expected_rows: int,
) -> dict[str, Any]:
    """Produce a source-only exact-schema addition candidate; never mutate *base*."""
    con = duckdb.connect(str(base), read_only=True)
    try:
        schema = _columns(con, "public.player_fantasy")
        names = [name for name, _ in schema]
        name_set = set(names)
        missing = sorted(set(KEY) - name_set)
        if missing:
            raise ValueError(f"canonical player schema missing key columns: {missing}")
        required = {"is_started", "is_rostered", "fantasy_points", "manager_lamar", "player", "position", "fantasy_position", "mfl_player_id", "espn_player_id"}
        missing = sorted(required - name_set)
        if missing:
            raise ValueError(f"canonical player schema missing required fields: {missing}")
        con.execute(f"ATTACH {_path(ops)} AS ops (READ_ONLY)")
        source_sql = f"""
          SELECT m.db_name, m.year, m.week, m.source_year, m.source_manager,
                 m.mfl_player_id, m.is_started, m.fantasy_points,
                 d.espn_id, d.raw_team, c.NFL_player_id,
                 o.player AS ops_player, o.position AS ops_position,
                 o.lamar_12t_idp_ppr_6pt AS ops_lamar
          FROM read_parquet({_path(memberships)}) m
          JOIN read_parquet({_path(targets)}) t
            ON t.db_name=m.db_name AND t.year=m.year AND t.week=m.week
           AND t.source_year=m.source_year AND t.source_manager=m.source_manager
           AND t.mfl_player_id=m.mfl_player_id
           AND t.bridge_status='no_canonical_recipient'
           AND t.canonical_player_week_count=0
          JOIN read_parquet({_path(directory)}) d
            ON d.source_year=m.source_year AND d.mfl_player_id=m.mfl_player_id
          JOIN read_parquet({_path(crosswalk)}) c
            ON c.source_year=m.source_year AND c.mfl_player_id=m.mfl_player_id
          LEFT JOIN ops.nfl_historical.nfl_player_stats_all o
            ON o.NFL_player_id=c.NFL_player_id AND o.year=m.year AND o.week=m.week
          ORDER BY m.db_name, m.year, m.week, c.NFL_player_id, m.source_manager
        """
        source_columns = [row[0] for row in con.execute(source_sql).description]
        source_rows = [dict(zip(source_columns, row)) for row in con.execute(source_sql).fetchall()]
        if len(source_rows) != expected_rows:
            raise ValueError(f"source rows {len(source_rows)} != expected_rows {expected_rows}")
        seen: set[tuple[Any, ...]] = set()
        candidate_rows: list[list[Any]] = []
        for source in source_rows:
            required_source = ("source_manager", "mfl_player_id", "fantasy_points", "espn_id", "NFL_player_id", "ops_player", "ops_position", "ops_lamar")
            absent = [field for field in required_source if source.get(field) is None]
            if absent:
                raise ValueError(f"source-proven row lacks required fields {absent}: {source}")
            logical_key = (
                source["db_name"], source["year"], source["week"], source["NFL_player_id"], source["source_manager"],
            )
            if logical_key in seen:
                raise ValueError(f"duplicate candidate logical key: {logical_key}")
            seen.add(logical_key)
            existing = int(con.execute(
                "SELECT COUNT(*) FROM public.player_fantasy WHERE db_name=? AND year=? AND week=? "
                "AND NFL_player_id=? AND manager=?", logical_key
            ).fetchone()[0])
            if existing:
                raise ValueError(f"candidate key already exists in canonical cache: {logical_key}")
            template = _single_template(
                con, db_name=str(source["db_name"]), year=int(source["year"]), week=int(source["week"]),
                manager=str(source["source_manager"]), columns=name_set,
            )
            values = {name: None for name in names}
            values.update(template)
            values.update({
                "db_name": source["db_name"],
                "year": source["year"],
                "week": source["week"],
                "NFL_player_id": source["NFL_player_id"],
                "manager": source["source_manager"],
                "is_started": int(bool(source["is_started"])),
                "is_rostered": 1,
                "fantasy_points": float(source["fantasy_points"]),
                "manager_lamar": float(source["ops_lamar"]),
                "player": source["ops_player"],
                "position": source["ops_position"],
                "fantasy_position": _slot(source["ops_position"], source["is_started"]),
                # Clutch is a league-week allocation, never a team-template
                # attribute.  The recovery workflow recomputes it for the full
                # affected league-year after these rows are inserted.
                "clutch_equity": 0.0,
                "nfl_team_api": source["raw_team"],
                "espn_player_id": str(source["espn_id"]),
                "mfl_player_id": str(source["mfl_player_id"]),
                "platform": "mfl",
            })
            candidate_rows.append([values[name] for name in names])

        writable = duckdb.connect()
        try:
            writable.execute("CREATE TABLE candidate (" + ", ".join(f"{_q(name)} {kind}" for name, kind in schema) + ")")
            writable.executemany("INSERT INTO candidate VALUES (" + ", ".join("?" for _ in names) + ")", candidate_rows)
            out.parent.mkdir(parents=True, exist_ok=True)
            writable.execute(f"COPY candidate TO {_path(out)} (FORMAT PARQUET, COMPRESSION ZSTD)")
        finally:
            writable.close()
        result = {
            "cache_mutated": False,
            "new_lineage": False,
            "candidate_rows": len(candidate_rows),
            "expected_rows": expected_rows,
            "logical_key": list(KEY),
            "schema": names,
            "source_fields": ["MFL roster membership", "MFL score", "MFL player directory", "ESPN-to-NFL crosswalk", "ops player/week", "existing canonical team-week"],
        }
        report.parent.mkdir(parents=True, exist_ok=True)
        report.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        return result
    finally:
        con.close()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base", type=Path, required=True)
    parser.add_argument("--ops", type=Path, required=True)
    parser.add_argument("--memberships", type=Path, required=True)
    parser.add_argument("--directory", type=Path, required=True)
    parser.add_argument("--crosswalk", type=Path, required=True)
    parser.add_argument("--targets", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument("--expected-rows", type=int, required=True)
    args = parser.parse_args()
    print(json.dumps(build(**vars(args)), sort_keys=True))


if __name__ == "__main__":
    main()
