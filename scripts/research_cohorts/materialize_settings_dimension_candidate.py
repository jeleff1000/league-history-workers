"""Apply consensus settings NULL fills to a working copy only."""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

import duckdb


FIELDS = (
    "scoring_pass_td", "playoff_teams", "roster_FLX",
    "roster_SUPER_FLEX", "roster_IDP", "sleeper_best_ball",
)


def hash_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--base", type=Path, required=True)
    ap.add_argument("--ops", type=Path, required=True)
    ap.add_argument("--delta", type=Path, required=True)
    ap.add_argument("--report", type=Path, required=True)
    args = ap.parse_args()

    con = duckdb.connect(str(args.base))
    before_ops = hash_file(args.ops)
    before_schema = [r[0] for r in con.execute('DESCRIBE public."league_settings"').fetchall()]
    before_rows = int(con.execute('SELECT COUNT(*) FROM public."league_settings"').fetchone()[0])
    delta_path = str(args.delta.resolve()).replace("'", "''")
    con.execute(f"CREATE OR REPLACE TEMP VIEW settings_delta AS SELECT * FROM read_parquet('{delta_path}')")
    dcols = {r[0] for r in con.execute("DESCRIBE settings_delta").fetchall()}
    required = {"db_name", "year", *(f"source_{f}" for f in FIELDS)}
    missing = sorted(required - dcols)
    if missing:
        raise SystemExit(f"settings delta schema missing: {missing}")
    duplicate_keys = int(con.execute('SELECT COUNT(*) FROM (SELECT db_name,year FROM settings_delta GROUP BY ALL HAVING COUNT(*)>1)').fetchone()[0])
    if duplicate_keys:
        raise SystemExit(f"duplicate settings delta keys: {duplicate_keys}")
    unmatched = int(con.execute('''
      SELECT COUNT(*) FROM settings_delta d
      WHERE NOT EXISTS (SELECT 1 FROM public.league_settings p
        WHERE p.db_name=d.db_name AND CAST(p.year AS INTEGER)=CAST(d.year AS INTEGER))
    ''').fetchone()[0])
    if unmatched:
        raise SystemExit(f"unmatched settings delta keys: {unmatched}")

    improvements = {}
    for field in FIELDS:
        improvements[field] = int(con.execute(f'''
          SELECT COUNT(*) FROM settings_delta d JOIN public.league_settings p
            ON p.db_name=d.db_name AND CAST(p.year AS INTEGER)=CAST(d.year AS INTEGER)
          WHERE p."{field}" IS NULL AND d."source_{field}" IS NOT NULL
        ''').fetchone()[0])

    con.execute("BEGIN")
    try:
        assignments = ", ".join(f'"{field}"=CASE WHEN p."{field}" IS NULL THEN d."source_{field}" ELSE p."{field}" END' for field in FIELDS)
        con.execute(f'''
          UPDATE public.league_settings p SET {assignments}
          FROM settings_delta d
          WHERE p.db_name=d.db_name AND CAST(p.year AS INTEGER)=CAST(d.year AS INTEGER)
        ''')
        con.execute("COMMIT")
    except Exception:
        con.execute("ROLLBACK")
        raise

    readback = {}
    for field in FIELDS:
        readback[field] = int(con.execute(f'''
          SELECT COUNT(*) FROM settings_delta d JOIN public.league_settings p
            ON p.db_name=d.db_name AND CAST(p.year AS INTEGER)=CAST(d.year AS INTEGER)
          WHERE d."source_{field}" IS NOT NULL AND p."{field}" IS NULL
        ''').fetchone()[0])
        if readback[field] != 0:
            raise SystemExit(f"settings readback failed for {field}: {readback[field]}")
    after_rows = int(con.execute('SELECT COUNT(*) FROM public."league_settings"').fetchone()[0])
    after_schema = [r[0] for r in con.execute('DESCRIBE public."league_settings"').fetchall()]
    after_ops = hash_file(args.ops)
    if after_rows != before_rows or after_schema != before_schema or after_ops != before_ops:
        raise SystemExit("settings preservation invariant failed")
    report = {
        "working_copy_only": True, "canonical_cache_replaced": False,
        "new_lineage": False, "schema_unchanged": after_schema == before_schema,
        "ops_unchanged": after_ops == before_ops, "rows_before": before_rows,
        "rows_after": after_rows, "delta_rows": int(con.execute('SELECT COUNT(*) FROM settings_delta').fetchone()[0]),
        "unmatched_keys": unmatched, "duplicate_keys": duplicate_keys,
        "improvements_by_field": improvements, "readback_remaining_nulls": readback,
    }
    args.report.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    con.close()
    print(json.dumps(report, sort_keys=True))


if __name__ == "__main__":
    main()
