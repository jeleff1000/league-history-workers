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
EXACT_FIELDS = {
    "win": "source_win", "loss": "source_loss", "tie": "source_tie",
    "team_points": "source_team_points", "is_playoffs": "source_playoffs",
    "has_po_signal": "source_has_po_signal", "champion": "source_champion",
    "final_playoff_seed": "source_final_playoff_seed", "made_playoffs": "source_made_playoffs",
    "clutch_equity": "source_clutch_equity",
}
STRUCTURED_FIELDS = {
    "win": "source_win", "loss": "source_loss", "tie": "source_tie",
    "team_points": "source_team_points", "is_playoffs": "source_is_playoffs",
    "champion": "source_champion", "final_playoff_seed": "source_final_playoff_seed",
    "made_playoffs": "source_made_playoffs", "clutch_equity": "source_clutch_equity",
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
    ap.add_argument("--structured-delta", type=Path, required=True)
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
    raw_team_rows = con.execute("SELECT COUNT(*) FROM team_delta").fetchone()[0]
    team_dupes = con.execute(
        f"SELECT COUNT(*) FROM (SELECT {','.join(q(c) for c in KEY)} FROM team_delta GROUP BY ALL HAVING COUNT(*)>1)"
    ).fetchone()[0]
    if team_dupes:
        # Multiple artifacts can repeat the same source fact.  Collapse only
        # exact duplicates; a value-versus-NULL or value-versus-value conflict
        # is still fatal and remains quarantined for adjudication.
        conflict_terms = ",".join(
            f"COUNT(DISTINCT COALESCE(CAST({q(c)} AS VARCHAR), '<NULL>'))" for c in TEAM_FIELDS.values()
        )
        conflicting_groups = con.execute(
            f"SELECT COUNT(*) FROM (SELECT {','.join(q(c) for c in KEY)} FROM team_delta GROUP BY ALL HAVING {' OR '.join(f'COUNT(DISTINCT COALESCE(CAST({q(c)} AS VARCHAR), \'<NULL>\'))>1' for c in TEAM_FIELDS.values())})"
        ).fetchone()[0]
        if conflicting_groups:
            raise SystemExit(f"conflicting duplicate team delta keys: {conflicting_groups}")
        source_select = ", ".join(f"MAX({q(c)}) AS {q(c)}" for c in TEAM_FIELDS.values())
        con.execute(
            f"CREATE OR REPLACE TEMP TABLE team_delta_unique AS SELECT {','.join(q(c) for c in KEY)}, {source_select} FROM team_delta GROUP BY ALL"
        )
    else:
        con.execute("CREATE OR REPLACE TEMP TABLE team_delta_unique AS SELECT * FROM team_delta")
    team_rows = con.execute("SELECT COUNT(*) FROM team_delta_unique").fetchone()[0]
    con.execute(f"""
      CREATE OR REPLACE TEMP TABLE team_matches AS
      SELECT p.rowid player_rowid, d.*
      FROM public.player_fantasy p JOIN team_delta_unique d ON {join_expr('p','d')}
    """)
    team_matched = con.execute("SELECT COUNT(*) FROM team_matches").fetchone()[0]
    team_unmatched_keys = con.execute(
        f"SELECT COUNT(*) FROM team_delta_unique d WHERE NOT EXISTS (SELECT 1 FROM public.player_fantasy p WHERE {join_expr('p','d')})"
    ).fetchone()[0]
    if team_unmatched_keys:
        raise SystemExit(f"unmatched team delta keys: {team_unmatched_keys}")

    exact_path_obj = args.exact_delta
    exact_rows = 0
    exact_matched = 0
    if exact_path_obj.exists() and exact_path_obj.stat().st_size > 0:
        con.execute(f"CREATE OR REPLACE TEMP VIEW exact_delta AS SELECT * FROM read_parquet('{exact_path}')")
        exact_cols = relation_columns(con, "exact_delta")
        missing_exact = sorted((set(KEY) | set(EXACT_FIELDS.values())) - set(exact_cols))
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
        exact_unmatched_keys = con.execute(
            f"SELECT COUNT(*) FROM exact_delta d WHERE NOT EXISTS (SELECT 1 FROM public.player_fantasy p WHERE {join_expr('p','d')})"
        ).fetchone()[0]
        if exact_unmatched_keys:
            raise SystemExit(f"unmatched exact delta keys: {exact_unmatched_keys}")
    else:
        exact_unmatched_keys = 0

    structured_path = args.structured_delta
    structured_rows = 0
    structured_matched = 0
    structured_cols: list[str] = []
    structured_unmatched_keys = 0
    if structured_path.exists() and structured_path.stat().st_size > 0:
        structured_path_sql = str(structured_path.resolve()).replace("'", "''")
        con.execute(f"CREATE OR REPLACE TEMP VIEW structured_delta AS SELECT * FROM read_parquet('{structured_path_sql}')")
        structured_cols = relation_columns(con, "structured_delta")
        required_structured = {"db_name", "year", "week", "manager"}
        missing_structured = sorted(required_structured - set(structured_cols))
        if missing_structured:
            raise SystemExit(f"structured delta schema missing: {missing_structured}")
        structured_rows = con.execute("SELECT COUNT(*) FROM structured_delta").fetchone()[0]
        structured_dupes = con.execute("SELECT COUNT(*) FROM (SELECT db_name,year,week,manager FROM structured_delta GROUP BY ALL HAVING COUNT(*)>1)").fetchone()[0]
        if structured_dupes:
            raise SystemExit(f"duplicate structured delta keys: {structured_dupes}")
        con.execute("""
          CREATE OR REPLACE TEMP TABLE structured_matches AS
          SELECT p.rowid player_rowid, d.*
          FROM public.player_fantasy p JOIN structured_delta d
            ON d.db_name=p.db_name AND CAST(d.year AS INTEGER)=CAST(p.year AS INTEGER)
           AND CAST(d.week AS INTEGER)=CAST(p.week AS INTEGER)
           AND lower(trim(cast(d.manager AS VARCHAR)))=lower(trim(cast(p.manager AS VARCHAR)))
        """)
        structured_matched = con.execute("SELECT COUNT(*) FROM structured_matches").fetchone()[0]
        structured_unmatched_keys = con.execute("""
          SELECT COUNT(*) FROM structured_delta d
          WHERE NOT EXISTS (SELECT 1 FROM public.player_fantasy p
            WHERE d.db_name=p.db_name AND CAST(d.year AS INTEGER)=CAST(p.year AS INTEGER)
              AND CAST(d.week AS INTEGER)=CAST(p.week AS INTEGER)
              AND lower(trim(cast(d.manager AS VARCHAR)))=lower(trim(cast(p.manager AS VARCHAR))))
        """).fetchone()[0]
        if structured_unmatched_keys:
            raise SystemExit(f"unmatched structured delta keys: {structured_unmatched_keys}")

    improvements: dict[str, int] = {}
    for field, source in TEAM_FIELDS.items():
        if field not in pcols or source not in team_cols:
            continue
        improvements[field] = con.execute(
            f"SELECT COUNT(*) FROM public.player_fantasy p JOIN team_matches m ON p.rowid=m.player_rowid WHERE p.{q(field)} IS NULL AND m.{q(source)} IS NOT NULL"
        ).fetchone()[0]
    if exact_rows:
        for field, source in EXACT_FIELDS.items():
            if field not in pcols or source not in exact_cols:
                continue
            count = con.execute(f"SELECT COUNT(*) FROM public.player_fantasy p JOIN exact_matches m ON p.rowid=m.player_rowid WHERE p.{q(field)} IS NULL AND m.{q(source)} IS NOT NULL").fetchone()[0]
            if count:
                improvements[f"exact_{field}"] = count
    if structured_rows:
        for field, source in STRUCTURED_FIELDS.items():
            if field not in pcols or source not in structured_cols:
                continue
            count = con.execute(f"SELECT COUNT(*) FROM public.player_fantasy p JOIN structured_matches m ON p.rowid=m.player_rowid WHERE p.{q(field)} IS NULL AND m.{q(source)} IS NOT NULL").fetchone()[0]
            if count:
                improvements[f"structured_{field}"] = count

    con.execute("BEGIN")
    try:
        assignments = []
        for field, source in TEAM_FIELDS.items():
            if field in pcols and source in team_cols:
                assignments.append(f"{q(field)}=CASE WHEN p.{q(field)} IS NULL THEN m.{q(source)} ELSE p.{q(field)} END")
        if assignments:
            con.execute(f"UPDATE public.player_fantasy p SET {', '.join(assignments)} FROM team_matches m WHERE p.rowid=m.player_rowid")
        if structured_rows:
            structured_assignments = [f"{q(field)}=CASE WHEN p.{q(field)} IS NULL THEN m.{q(source)} ELSE p.{q(field)} END" for field, source in STRUCTURED_FIELDS.items() if field in pcols and source in structured_cols]
            if structured_assignments:
                con.execute(f"UPDATE public.player_fantasy p SET {', '.join(structured_assignments)} FROM structured_matches m WHERE p.rowid=m.player_rowid")
        if exact_rows:
            exact_assignments = [f"{q(field)}=CASE WHEN p.{q(field)} IS NULL THEN e.{q(source)} ELSE p.{q(field)} END" for field, source in EXACT_FIELDS.items() if field in pcols and source in exact_cols]
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
    for field, source in EXACT_FIELDS.items():
        if exact_rows and field in pcols and source in exact_cols:
            readback[f"exact_{field}"] = con.execute(
                f"SELECT COUNT(*) FROM exact_matches m JOIN public.player_fantasy p ON p.rowid=m.player_rowid WHERE m.{q(source)} IS NOT NULL AND p.{q(field)} IS NULL"
            ).fetchone()[0]
            if readback[f"exact_{field}"] != 0:
                raise SystemExit(f"exact readback failed for {field}: {readback[f'exact_{field}']} source cells still null")
    for field, source in STRUCTURED_FIELDS.items():
        if structured_rows and field in pcols and source in structured_cols:
            readback[f"structured_{field}"] = con.execute(
                f"SELECT COUNT(*) FROM structured_matches m JOIN public.player_fantasy p ON p.rowid=m.player_rowid WHERE m.{q(source)} IS NOT NULL AND p.{q(field)} IS NULL"
            ).fetchone()[0]
            if readback[f"structured_{field}"] != 0:
                raise SystemExit(f"structured readback failed for {field}: {readback[f'structured_{field}']} source cells still null")
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
        "team_delta_rows_raw": raw_team_rows, "team_delta_duplicate_rows_collapsed": raw_team_rows - team_rows,
        "team_delta_rows": team_rows, "team_matched_player_rows": team_matched,
        "team_unmatched_keys": team_unmatched_keys,
        "exact_delta_rows": exact_rows, "exact_matched_player_rows": exact_matched,
        "exact_unmatched_keys": exact_unmatched_keys,
        "structured_delta_rows": structured_rows, "structured_matched_player_rows": structured_matched,
        "structured_unmatched_keys": structured_unmatched_keys,
        "improvements_by_field": improvements, "readback_remaining_nulls": readback,
        "cache_key_replacement_performed": False,
    }
    args.report.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    con.close()
    print(json.dumps(report, sort_keys=True))


if __name__ == "__main__":
    main()
