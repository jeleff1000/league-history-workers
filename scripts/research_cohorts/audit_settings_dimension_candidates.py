"""Audit settings-fill artifacts against the restored canonical cache.

Read-only: this identifies exact NULL fills and conflicts in existing target
columns. It does not infer cohort labels or alter either database.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import duckdb


SOURCE_FIELDS = (
    "pass_td_fill", "playoff_teams_fill", "roster_FLX_fill",
    "roster_SUPER_FLEX_fill", "roster_IDP_fill", "best_ball_fill",
)
TARGET_FIELDS = {
    "pass_td_fill": "scoring_pass_td",
    "playoff_teams_fill": "playoff_teams",
    "roster_FLX_fill": "roster_FLX",
    "roster_SUPER_FLEX_fill": "roster_SUPER_FLEX",
    "roster_IDP_fill": "roster_IDP",
    "best_ball_fill": "sleeper_best_ball",
}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--base", type=Path, required=True)
    ap.add_argument("--candidates", type=Path, required=True)
    ap.add_argument("--out", type=Path, required=True)
    ap.add_argument("--delta-out", type=Path, required=False)
    args = ap.parse_args()

    con = duckdb.connect(str(args.base), read_only=True)
    tables = {r[0] for r in con.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='public'").fetchall()}
    source_files = []
    for path in sorted(args.candidates.rglob("*.parquet")):
        try:
            cols = {r[0] for r in con.execute("DESCRIBE SELECT * FROM read_parquet(?)", [str(path)]).fetchall()}
        except Exception:
            continue
        if {"db_name", "year"} <= cols and set(SOURCE_FIELDS) & cols:
            source_files.append((path, cols))

    report: dict = {
        "cache_mutated": False, "new_lineage": False, "schema_changed": False,
        "source_files": len(source_files), "source_rows": 0,
        "source_conflicting_keys": 0, "source_unique_keys": 0,
        "public_tables": sorted(tables), "target_tables": {},
    }
    if not source_files:
        args.out.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
        return

    paths = ",".join("'" + str(path.resolve()).replace("'", "''") + "'" for path, _ in source_files)
    con.execute(f"CREATE OR REPLACE TEMP VIEW settings_source AS SELECT * FROM read_parquet([{paths}], union_by_name=true, filename=true)")
    report["source_rows"] = int(con.execute("SELECT COUNT(*) FROM settings_source").fetchone()[0])
    present = [f for f in SOURCE_FIELDS if f in {r[0] for r in con.execute("DESCRIBE settings_source").fetchall()}]
    conflict = " OR ".join(f"COUNT(DISTINCT COALESCE(CAST(\"{f}\" AS VARCHAR), '<NULL>')) > 1" for f in present)
    con.execute(f"""
      CREATE OR REPLACE TEMP TABLE settings_unique AS
      SELECT db_name, CAST("year" AS INTEGER) AS "year",
             {', '.join(f'MAX("{f}") AS "{f}"' for f in present)},
             string_agg(DISTINCT filename, '|') AS source_files
      FROM settings_source GROUP BY db_name, CAST("year" AS INTEGER)
    """)
    report["source_conflicting_keys"] = int(con.execute(f"""
      SELECT COUNT(*) FROM (SELECT db_name, CAST("year" AS INTEGER) AS "year" FROM settings_source
      GROUP BY db_name, CAST("year" AS INTEGER) HAVING {conflict})
    """).fetchone()[0]) if conflict else 0
    report["source_unique_keys"] = int(con.execute("SELECT COUNT(*) FROM settings_unique").fetchone()[0])

    for table_name in ("league_settings", "player_fantasy"):
        if table_name not in tables:
            continue
        cols = [r[0] for r in con.execute(f'DESCRIBE public."{table_name}"').fetchall()]
        target = {field: TARGET_FIELDS[field] for field in present if TARGET_FIELDS[field] in cols}
        result = {"columns": cols, "exact_target_columns": target, "null_fill_candidates": {}, "conflicts": {}}
        if target:
            joins = "s.db_name=p.db_name AND CAST(s.year AS INTEGER)=CAST(p.year AS INTEGER)"
            for source, dest in target.items():
                result["null_fill_candidates"][dest] = int(con.execute(f"""
                  SELECT COUNT(*) FROM settings_unique s JOIN public."{table_name}" p ON {joins}
                  WHERE p."{dest}" IS NULL AND s."{source}" IS NOT NULL
                    AND NOT EXISTS (SELECT 1 FROM settings_source c WHERE c.db_name=s.db_name AND CAST(c."year" AS INTEGER)=s."year"
                      GROUP BY c.db_name, CAST(c."year" AS INTEGER) HAVING COUNT(DISTINCT COALESCE(CAST(c."{source}" AS VARCHAR), '<NULL>')) > 1)
                """).fetchone()[0])
                result["conflicts"][dest] = int(con.execute(f"""
                  SELECT COUNT(*) FROM settings_unique s JOIN public."{table_name}" p ON {joins}
                  WHERE p."{dest}" IS NOT NULL AND s."{source}" IS NOT NULL
                    AND CAST(p."{dest}" AS VARCHAR) <> CAST(s."{source}" AS VARCHAR)
                """).fetchone()[0])
        result["cohort_columns"] = [c for c in cols if c.startswith("cohort_")]
        result["cohort_null_rows"] = int(con.execute(f"SELECT COUNT(*) FROM public.\"{table_name}\"").fetchone()[0]) if table_name == "player_fantasy" else None
        report["target_tables"][table_name] = result

    settings_target = report["target_tables"].get("league_settings", {})
    target_columns = set(settings_target.get("columns", []))
    delta_fields = [source for source in present if TARGET_FIELDS[source] in target_columns]
    if args.delta_out and delta_fields:
        # A source key with conflicting values is excluded, never arbitrarily
        # resolved. The materializer performs NULL-only updates on the target.
        select_fields = [f's."{source}" AS "source_{TARGET_FIELDS[source]}"' for source in delta_fields]
        null_terms = [f'p."{TARGET_FIELDS[source]}" IS NULL AND s."{source}" IS NOT NULL' for source in delta_fields]
        conflict_terms = " OR ".join(f'COUNT(DISTINCT COALESCE(CAST(c."{source}" AS VARCHAR), \'<NULL>\')) > 1' for source in delta_fields)
        con.execute(f"""
          COPY (
            SELECT s.db_name, s."year", s.source_files, {', '.join(select_fields)}
            FROM settings_unique s JOIN public.league_settings p
              ON s.db_name=p.db_name AND s."year"=CAST(p."year" AS INTEGER)
            WHERE ({' OR '.join(null_terms)})
              AND NOT EXISTS (
                SELECT 1 FROM settings_source c
                WHERE c.db_name=s.db_name AND CAST(c."year" AS INTEGER)=s."year"
                GROUP BY c.db_name, CAST(c."year" AS INTEGER) HAVING {conflict_terms}
              )
          ) TO '{str(args.delta_out.resolve()).replace("'", "''")}' (FORMAT PARQUET)
        """)
        report["delta_rows"] = int(con.execute(f"SELECT COUNT(*) FROM read_parquet('{str(args.delta_out.resolve()).replace("'", "''")}')").fetchone()[0])
    else:
        report["delta_rows"] = 0

    con.close()
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps({k: report[k] for k in ("source_files", "source_rows", "source_conflicting_keys", "source_unique_keys", "target_tables")}, sort_keys=True))


if __name__ == "__main__":
    main()
