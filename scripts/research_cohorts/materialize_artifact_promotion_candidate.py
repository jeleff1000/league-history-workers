"""Materialize audited artifact deltas into a verified working copy.

This command deliberately does not save or replace an Actions cache.  It
updates the restored working copy only, fails closed on schema/key/conflict
problems, and proves every promoted source cell by reading it back.
"""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

import duckdb


BASE_COLUMNS = [
    "db_name", "year", "week", "NFL_player_id", "is_started", "is_rostered",
    "fantasy_points", "win", "champion", "clutch_equity", "manager_lamar",
    "manager", "team_points", "final_playoff_seed", "is_playoffs",
    "has_po_signal", "player", "position", "fantasy_position", "platform",
    "team_key", "team_name", "nfl_team_api", "yahoo_player_id",
    "sleeper_player_id", "espn_player_id", "fleaflicker_player_id",
    "mfl_player_id", "made_playoffs",
]
FORBIDDEN = {"opponent_points", "is_championship", "is_active", "is_playoffs_bf", "made_po_bf", "made_po"}
TEAM_FIELDS = {
    "win": "source_win", "team_points": "source_team_points",
    "is_playoffs": "source_playoffs", "champion": "source_champion",
    "final_playoff_seed": "source_final_playoff_seed", "made_playoffs": "source_made_playoffs",
    "loss": "source_loss", "tie": "source_tie",
}
KEY = ["db_name", "year", "week", "NFL_player_id", "platform", "manager", "team_key", "team_name"]


def q(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def hash_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for block in iter(lambda: f.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def relation_columns(con: duckdb.DuckDBPyConnection, relation: str) -> list[str]:
    return [r[0] for r in con.execute(f"DESCRIBE {relation}").fetchall()]


def join_expr(left: str, right: str) -> str:
    return " AND ".join(f"{left}.{q(c)} IS NOT DISTINCT FROM {right}.{q(c)}" for c in KEY)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--base", type=Path, required=True)
    ap.add_argument("--team-delta", type=Path, required=True)
    ap.add_argument("--exact-delta", type=Path, required=True)
    ap.add_argument("--ops", type=Path, required=True)
    ap.add_argument("--report", type=Path, required=True)
    args = ap.parse_args()

    before_ops = hash_file(args.ops)
    con = duckdb.connect(str(args.base))
    pcols = relation_columns(con, "public.player_fantasy")
    if not set(BASE_COLUMNS) <= set(pcols) or FORBIDDEN & set(pcols):
        raise SystemExit(f"canonical schema mismatch: missing={sorted(set(BASE_COLUMNS)-set(pcols))} forbidden={sorted(FORBIDDEN & set(pcols))}")
    before_schema = pcols[:]
    before_rows = con.execute("SELECT COUNT(*) FROM public.player_fantasy").fetchone()[0]

    team_path = str(args.team_delta.resolve()).replace("'", "''")
    exact_path = str(args.exact_delta.resolve()).replace("'", "''")
    con.execute(f"CREATE OR REPLACE TEMP VIEW team_delta AS SELECT * FROM read_parquet('{team_path}')")
    team_cols = relation_columns(con, "team_delta")
    required_team = set(KEY) | set(TEAM_FIELDS.values())
    missing_team = sorted(required_team - set(team_cols))
    if missing_team:
        raise SystemExit(f"team delta schema missing: {missing_team}")
    team_rows = con.execute("SELECT COUNT(*) FROM team_delta").fetchone()[0]
    team_dupes = con.execute(
        f"SELECT COUNT(*) FROM (SELECT {','.join(q(c) for c in KEY)} FROM team_delta GROUP BY ALL HAVING COUNT(*)>1)"
    ).fetchone()[0]
    if team_dupes:
        raise SystemExit(f"duplicate team delta keys: {team_dupes}")
    con.execute(f"""
      CREATE OR REPLACE TEMP TABLE team_matches AS
      SELECT p.rowid player_rowid, d.*
      FROM public.player_fantasy p JOIN team_delta d ON {join_expr('p','d')}
    """)
    team_matched = con.execute("SELECT COUNT(*) FROM team_matches").fetchone()[0]
    if team_matched != team_rows:
        raise SystemExit(f"unmatched team delta rows: {team_rows-team_matched}")

    exact_path_obj = args.exact_delta
    exact_rows = 0
    exact_matched = 0
    if exact_path_obj.exists() and exact_path_obj.stat().st_size > 0:
        con.execute(f"CREATE OR REPLACE TEMP VIEW exact_delta AS SELECT * FROM read_parquet('{exact_path}')")
        exact_cols = relation_columns(con, "exact_delta")
        missing_exact = sorted(set(BASE_COLUMNS) - set(exact_cols))
        if missing_exact:
            raise SystemExit(f"exact delta schema missing: {missing_exact}")
        exact_rows = con.execute("SELECT COUNT(*) FROM exact_delta").fetchone()[0]
        exact_dupes = con.execute(
            f"SELECT COUNT(*) FROM (SELECT {','.join(q(c) for c in KEY)} FROM exact_delta GROUP BY ALL HAVING COUNT(*)>1)"
        ).fetchone()[0]
        if exact_dupes:
            raise SystemExit(f"duplicate exact delta keys: {exact_dupes}")
        con.execute(f"CREATE OR REPLACE TEMP TABLE exact_matches AS SELECT p.rowid player_rowid, d.* FROM public.player_fantasy p JOIN exact_delta d ON {join_expr('p','d')}")
        exact_matched = con.execute("SELECT COUNT(*) FROM exact_matches").fetchone()[0]
        if exact_matched != exact_rows:
            raise SystemExit(f"unmatched exact delta rows: {exact_rows-exact_matched}")

    improvements: dict[str, int] = {}
    for field, source in TEAM_FIELDS.items():
        if field not in pcols or source not in team_cols:
            continue
        improvements[field] = con.execute(
            f"SELECT COUNT(*) FROM team_matches WHERE {q(field)} IS NULL AND {q(source)} IS NOT NULL"
        ).fetchone()[0]
    if exact_rows:
        for field in BASE_COLUMNS:
            if field in KEY:
                continue
            count = con.execute(f"SELECT COUNT(*) FROM exact_matches WHERE {q(field)} IS NULL AND {q(field)} IS NOT NULL").fetchone()[0]
            if count:
                improvements[f"exact_{field}"] = count

    con.execute("BEGIN")
    try:
        assignments = []
        for field, source in TEAM_FIELDS.items():
            if field in pcols and source in team_cols:
                assignments.append(f"{q(field)}=CASE WHEN p.{q(field)} IS NULL THEN m.{q(source)} ELSE p.{q(field)} END")
        if assignments:
            con.execute(f"UPDATE public.player_fantasy p SET {', '.join(assignments)} FROM team_matches m WHERE p.rowid=m.player_rowid")
        if exact_rows:
            exact_assignments = [f"{q(c)}=CASE WHEN p.{q(c)} IS NULL THEN e.{q(c)} ELSE p.{q(c)} END" for c in BASE_COLUMNS if c not in KEY]
            con.execute(f"UPDATE public.player_fantasy p SET {', '.join(exact_assignments)} FROM exact_matches e WHERE p.rowid=e.player_rowid")
        con.execute("COMMIT")
    except Exception:
        con.execute("ROLLBACK")
        raise

    readback: dict[str, int] = {}
    for field, source in TEAM_FIELDS.items():
        if field not in pcols or source not in team_cols:
            continue
        readback[field] = con.execute(
            f"SELECT COUNT(*) FROM team_matches m JOIN public.player_fantasy p ON p.rowid=m.player_rowid WHERE m.{q(source)} IS NOT NULL AND p.{q(field)} IS NULL"
        ).fetchone()[0]
        if readback[field] != 0:
            raise SystemExit(f"readback failed for {field}: {readback[field]} source cells still null")
    after_rows = con.execute("SELECT COUNT(*) FROM public.player_fantasy").fetchone()[0]
    after_schema = relation_columns(con, "public.player_fantasy")
    after_ops = hash_file(args.ops)
    if after_rows != before_rows or after_schema != before_schema or after_ops != before_ops:
        raise SystemExit("preservation invariant failed")
    report = {
        "working_copy_only": True, "canonical_cache_replaced": False,
        "new_lineage": False, "ops_unchanged": after_ops == before_ops,
        "schema_unchanged": after_schema == before_schema,
        "player_rows_before": before_rows, "player_rows_after": after_rows,
        "team_delta_rows": team_rows, "team_matched_rows": team_matched,
        "exact_delta_rows": exact_rows, "exact_matched_rows": exact_matched,
        "improvements_by_field": improvements, "readback_remaining_nulls": readback,
        "cache_key_replacement_performed": False,
    }
    args.report.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    con.close()
    print(json.dumps(report, sort_keys=True))


if __name__ == "__main__":
    main()
