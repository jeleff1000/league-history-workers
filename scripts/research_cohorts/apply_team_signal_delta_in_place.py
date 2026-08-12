"""Apply an already-audited team fan-out delta to the frozen player table.

This is intentionally narrow: it updates existing player rows only, fills NULLs
only, and refuses schema/duplicate/key violations. It never touches ops tables.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import duckdb

try:
    from .canonical_player_schema import BASE_PLAYER_COLUMNS, OPTIONAL_PLAYER_COLUMNS, validate_player_schema
except ImportError:  # direct script execution in Actions
    from canonical_player_schema import BASE_PLAYER_COLUMNS, OPTIONAL_PLAYER_COLUMNS, validate_player_schema




def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--base", type=Path, required=True)
    ap.add_argument("--delta", type=Path, required=True)
    ap.add_argument("--new-rows", type=Path, default=None)
    ap.add_argument("--report", type=Path, required=True)
    args = ap.parse_args()

    con = duckdb.connect(str(args.base))
    pcols = [r[0] for r in con.execute("DESCRIBE public.player_fantasy").fetchall()]
    cohort_columns, optional = validate_player_schema(pcols)
    loss_expr = "p.loss" if "loss" in pcols else "CAST(NULL AS INTEGER)"
    tie_expr = "p.tie" if "tie" in pcols else "CAST(NULL AS INTEGER)"
    canonical_columns_before = list(pcols)
    dpath = str(args.delta.resolve()).replace("'", "''")
    con.execute(f"CREATE OR REPLACE TEMP VIEW delta_raw AS SELECT * FROM read_parquet('{dpath}')")
    dcols = {r[0] for r in con.execute("DESCRIBE delta_raw").fetchall()}
    required = {"db_name", "year", "week", "NFL_player_id", "manager", "team_key", "team_name", "platform"}
    required |= {"source_win", "source_team_points", "source_playoffs", "source_champion", "source_final_playoff_seed"}
    if not required <= dcols:
        raise SystemExit(f"delta schema missing: {sorted(required-dcols)}")
    new_count = 0
    if args.new_rows is not None:
        npath = str(args.new_rows.resolve()).replace("'", "''")
        con.execute(f"CREATE OR REPLACE TEMP VIEW new_rows_raw AS SELECT * FROM read_parquet('{npath}')")
        ncols = [r[0] for r in con.execute("DESCRIBE new_rows_raw").fetchall()]
        if ncols != canonical_columns_before:
            raise SystemExit(f"new-row schema must exactly equal canonical player schema: {ncols} != {canonical_columns_before}")
        new_count = con.execute("SELECT COUNT(*) FROM new_rows_raw").fetchone()[0]
        new_dup = con.execute("SELECT COUNT(*) FROM (SELECT db_name,year,week,NFL_player_id,manager,team_key,team_name,platform FROM new_rows_raw GROUP BY ALL HAVING COUNT(*)>1)").fetchone()[0]
        if new_dup:
            raise SystemExit(f"duplicate new-row keys: {new_dup}")
        existing_new = con.execute("""
          SELECT COUNT(*) FROM new_rows_raw n JOIN public.player_fantasy p
            ON n.db_name IS NOT DISTINCT FROM p.db_name
           AND CAST(n.year AS INTEGER) IS NOT DISTINCT FROM CAST(p.year AS INTEGER)
           AND CAST(n.week AS INTEGER) IS NOT DISTINCT FROM CAST(p.week AS INTEGER)
           AND n.NFL_player_id IS NOT DISTINCT FROM p.NFL_player_id
           AND n.manager IS NOT DISTINCT FROM p.manager
           AND n.team_key IS NOT DISTINCT FROM p.team_key
           AND n.team_name IS NOT DISTINCT FROM p.team_name
           AND LOWER(n.platform) IS NOT DISTINCT FROM LOWER(p.platform)
        """).fetchone()[0]
        if existing_new:
            raise SystemExit(f"new-row candidates already present in canonical table: {existing_new}")
    key_columns = ["db_name", "year", "week", "NFL_player_id", "manager", "team_key", "team_name", "platform"]
    key = ",".join('"' + column + '"' for column in key_columns)
    raw_delta_rows = int(con.execute("SELECT COUNT(*) FROM delta_raw").fetchone()[0])
    dup = con.execute(f"SELECT COUNT(*) FROM (SELECT {key} FROM delta_raw GROUP BY ALL HAVING COUNT(*)>1)").fetchone()[0]
    value_columns = sorted(
        column for column in dcols
        if column not in key_columns and column not in {"source_file", "source_files"}
    )
    if dup:
        conflicts = con.execute(f"""
            SELECT COUNT(*) FROM (
              SELECT {key} FROM delta_raw GROUP BY ALL HAVING
              {' OR '.join(f"COUNT(DISTINCT COALESCE(CAST(\"{column}\" AS VARCHAR), '<NULL>')) > 1" for column in value_columns)}
            )
        """).fetchone()[0]
        if conflicts:
            raise SystemExit(f"conflicting duplicate delta keys: {conflicts}")
        expressions = [key]
        for column in value_columns:
            expressions.append(f'MAX("{column}") AS "{column}"')
        for column in ("source_file", "source_files"):
            if column in dcols:
                expressions.append(
                    f"STRING_AGG(DISTINCT CAST(\"{column}\" AS VARCHAR), '|') AS \"{column}\""
                )
        con.execute(
            "CREATE OR REPLACE TEMP TABLE delta_unique AS SELECT "
            + ", ".join(expressions)
            + " FROM delta_raw GROUP BY "
            + key
        )
    else:
        con.execute("CREATE OR REPLACE TEMP TABLE delta_unique AS SELECT * FROM delta_raw")
    before_rows = con.execute("SELECT COUNT(*) FROM public.player_fantasy").fetchone()[0]
    core_join = """
        d.db_name=p.db_name
        AND CAST(d.year AS INTEGER)=CAST(p.year AS INTEGER)
        AND CAST(d.week AS INTEGER)=CAST(p.week AS INTEGER)
        AND d.NFL_player_id IS NOT DISTINCT FROM p.NFL_player_id
        AND d.platform IS NOT DISTINCT FROM p.platform
        AND d.manager IS NOT DISTINCT FROM p.manager
    """
    full_join = core_join + """
        AND (p.team_key IS NULL OR d.team_key=p.team_key)
        AND (p.team_name IS NULL OR d.team_name=p.team_name)
    """
    con.execute(f"""
      CREATE OR REPLACE TEMP TABLE matched AS
      SELECT p.rowid player_rowid,
             p.win canonical_win,
             {loss_expr} canonical_loss,
             {tie_expr} canonical_tie,
             p.team_points canonical_team_points,
             p.is_playoffs canonical_is_playoffs,
             p.has_po_signal canonical_has_po_signal,
             p.champion canonical_champion,
             p.clutch_equity canonical_clutch_equity,
             p.final_playoff_seed canonical_final_playoff_seed,
             p.made_playoffs canonical_made_playoffs,
             d.*
      FROM public.player_fantasy p JOIN delta_unique d ON {full_join}
    """)
    matched = con.execute("SELECT COUNT(*) FROM matched").fetchone()[0]
    unmatched_count = con.execute(f"""
      SELECT COUNT(*) FROM delta_unique d
      WHERE NOT EXISTS (SELECT 1 FROM public.player_fantasy p WHERE {full_join})
    """).fetchone()[0]
    if unmatched_count:
        con.execute(f"""
          CREATE OR REPLACE TEMP TABLE unmatched_delta AS
          SELECT d.* FROM delta_unique d
          WHERE NOT EXISTS (SELECT 1 FROM public.player_fantasy p WHERE {full_join})
        """)
        diagnostic = con.execute(f"""
          SELECT
            COUNT(*) AS unmatched_delta_rows,
            COUNT(*) FILTER (WHERE EXISTS (
              SELECT 1 FROM public.player_fantasy p WHERE {core_join}
            )) AS core_identity_matches,
            COUNT(*) FILTER (WHERE EXISTS (
              SELECT 1 FROM public.player_fantasy p WHERE {core_join}
                AND p.team_key IS NOT NULL AND p.team_key IS DISTINCT FROM d.team_key
            )) AS team_key_conflicts,
            COUNT(*) FILTER (WHERE EXISTS (
              SELECT 1 FROM public.player_fantasy p WHERE {core_join}
                AND p.team_name IS NOT NULL AND p.team_name IS DISTINCT FROM d.team_name
            )) AS team_name_conflicts,
            COUNT(*) FILTER (WHERE NFL_player_id IS NULL) AS null_nfl_player_id,
            COUNT(*) FILTER (WHERE platform IS NULL) AS null_platform,
            COUNT(*) FILTER (WHERE manager IS NULL) AS null_manager
          FROM unmatched_delta d
        """).fetchone()
        fields = [column[0] for column in con.description]
        receipt = dict(zip(fields, diagnostic))
        print("unmatched_diagnostic=" + json.dumps(receipt, sort_keys=True), file=sys.stderr)
        raise SystemExit("delta contains unmatched canonical player rows")
    fields = {
        "win": "source_win",
        "loss": "source_loss",
        "tie": "source_tie",
        "team_points": "source_team_points",
        "is_playoffs": "source_playoffs",
        "has_po_signal": "source_has_po_signal",
        "champion": "source_champion",
        "final_playoff_seed": "source_final_playoff_seed",
        "made_playoffs": "source_made_playoffs",
        "clutch_equity": "source_clutch_equity",
    }
    improvements = {}
    for field, src in fields.items():
        if src not in dcols:
            continue
        canonical = f"canonical_{field}"
        improvements[field] = con.execute(f"SELECT COUNT(*) FROM matched WHERE {canonical} IS NULL AND {src} IS NOT NULL").fetchone()[0]
    update_assignments = [
        "win=CASE WHEN p.win IS NULL THEN m.source_win ELSE p.win END",
        "team_points=CASE WHEN p.team_points IS NULL THEN m.source_team_points ELSE p.team_points END",
        "is_playoffs=CASE WHEN p.is_playoffs IS NULL THEN m.source_playoffs ELSE p.is_playoffs END",
        "champion=CASE WHEN p.champion IS NULL THEN m.source_champion ELSE p.champion END",
        "final_playoff_seed=CASE WHEN p.final_playoff_seed IS NULL THEN m.source_final_playoff_seed ELSE p.final_playoff_seed END",
    ]
    if "source_made_playoffs" in dcols:
        update_assignments.append(
            "made_playoffs=CASE WHEN p.made_playoffs IS NULL THEN m.source_made_playoffs ELSE p.made_playoffs END"
        )
    if "loss" in pcols and "source_loss" in dcols:
        update_assignments.insert(1, "loss=CASE WHEN p.loss IS NULL THEN m.source_loss ELSE p.loss END")
    if "tie" in pcols and "source_tie" in dcols:
        update_assignments.insert(2 if "loss" in pcols and "source_loss" in dcols else 1, "tie=CASE WHEN p.tie IS NULL THEN m.source_tie ELSE p.tie END")
    if "source_has_po_signal" in dcols:
        update_assignments.append("has_po_signal=CASE WHEN p.has_po_signal IS NULL THEN m.source_has_po_signal ELSE p.has_po_signal END")
    if "source_clutch_equity" in dcols:
        update_assignments.append("clutch_equity=CASE WHEN p.clutch_equity IS NULL THEN m.source_clutch_equity ELSE p.clutch_equity END")
    update_sql = ",\n            ".join(update_assignments)
    con.execute("BEGIN")
    try:
        con.execute(f"""
          UPDATE public.player_fantasy p SET
            {update_sql}
          FROM matched m WHERE p.rowid=m.player_rowid
        """)
        if args.new_rows is not None and new_count:
            quoted = ",".join('"' + c.replace('"','""') + '"' for c in canonical_columns_before)
            con.execute(f"INSERT INTO public.player_fantasy ({quoted}) SELECT {quoted} FROM new_rows_raw")
        expected_rows = before_rows + int(new_count)
        if con.execute("SELECT COUNT(*) FROM public.player_fantasy").fetchone()[0] != expected_rows:
            raise SystemExit("unexpected player row count after update/insert")
        if [r[0] for r in con.execute("DESCRIBE public.player_fantasy").fetchall()] != canonical_columns_before:
            raise SystemExit("player schema changed")
        con.execute("COMMIT")
    except Exception:
        con.execute("ROLLBACK")
        raise
    report = {
        "in_place": True, "cache_mutated": True, "new_lineage": False,
        "ops_untouched": True, "schema_unchanged": True,
        "delta_rows_raw": raw_delta_rows,
        "delta_duplicate_rows_collapsed": raw_delta_rows - int(con.execute("SELECT COUNT(*) FROM delta_unique").fetchone()[0]),
        "delta_rows": int(con.execute("SELECT COUNT(*) FROM delta_unique").fetchone()[0]),
        "matched_rows": int(matched),
        "inserted_rows": int(new_count),
        "improvements_by_field": {k:int(v) for k,v in improvements.items()},
    }
    if "source_file" in dcols:
        report["source_file_rows"] = {
            str(r[0]): int(r[1]) for r in con.execute(
                "SELECT source_file, COUNT(*) FROM delta_unique GROUP BY 1 ORDER BY 1"
            ).fetchall()
        }
    if "source_files" in dcols:
        report["source_file_rows"] = {
            str(r[0]): int(r[1]) for r in con.execute(
                "SELECT source_files, COUNT(*) FROM delta_unique GROUP BY 1 ORDER BY 1"
            ).fetchall()
        }
    args.report.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(report, sort_keys=True))


if __name__ == "__main__":
    main()
