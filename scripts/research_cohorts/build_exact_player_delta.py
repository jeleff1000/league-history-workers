"""Build a read-only, null-fill-only delta from exact player artifacts.

The artifact set is treated as evidence, not as a replacement table.  Rows are
matched at player-week-manager/team grain and only canonical NULL cells are
eligible for promotion.  Conflicting duplicate source keys are excluded rather
than arbitrarily resolved.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Iterable

import duckdb

try:
    from .canonical_player_schema import BASE_PLAYER_COLUMNS, OPTIONAL_PLAYER_COLUMNS, validate_player_schema
except ImportError:  # direct script execution in Actions
    from canonical_player_schema import BASE_PLAYER_COLUMNS, OPTIONAL_PLAYER_COLUMNS, validate_player_schema


OPTIONAL = OPTIONAL_PLAYER_COLUMNS
SIGNAL_TYPES = {
    "win": "INTEGER",
    "loss": "INTEGER",
    "tie": "INTEGER",
    "team_points": "DOUBLE",
    "is_playoffs": "INTEGER",
    "has_po_signal": "INTEGER",
    "champion": "INTEGER",
    "final_playoff_seed": "INTEGER",
    "made_playoffs": "INTEGER",
    "clutch_equity": "DOUBLE",
}
KEY_COLUMNS = ["db_name", "year", "week", "NFL_player_id", "manager", "team_key", "team_name", "platform"]


def _lit(path: Path) -> str:
    return "'" + str(path.resolve()).replace("'", "''") + "'"


def _ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _read_union(con: duckdb.DuckDBPyConnection, paths: Iterable[Path]) -> None:
    values = ",".join(_lit(p) for p in paths)
    con.execute(
        "CREATE OR REPLACE TEMP VIEW source_raw AS "
        f"SELECT * FROM read_parquet([{values}], union_by_name=true, filename=true)"
    )


def build_delta(base: Path, sources: list[Path], out: Path, insert_out: Path | None = None) -> dict:
    con = duckdb.connect(str(base), read_only=True)
    pcols = [r[0] for r in con.execute("DESCRIBE public.player_fantasy").fetchall()]
    cohorts, _ = validate_player_schema(pcols, allow_test_fixture=True)
    if not sources:
        con.close()
        return {"source_files": 0, "source_rows": 0, "matched_rows": 0, "unmatched_rows": 0,
                "insert_candidate_rows": 0, "unresolved_unmatched_rows": 0,
                "source_conflicting_keys": 0, "delta_rows": 0,
                "improvements_by_field": {f: 0 for f in SIGNAL_TYPES}}

    _read_union(con, sources)
    source_cols = {r[0] for r in con.execute("DESCRIBE source_raw").fetchall()}
    required = set(KEY_COLUMNS)
    missing_source = sorted(required - source_cols)
    if missing_source:
        raise ValueError(f"exact source schema missing join fields: {missing_source}")
    source_expr = ["CAST(s.filename AS VARCHAR) AS source_file"] if "filename" in source_cols else ["CAST(NULL AS VARCHAR) AS source_file"]
    for field in KEY_COLUMNS:
        source_expr.append(f"CAST(s.{_ident(field)} AS VARCHAR) AS {_ident(field)}")
    for field, typ in SIGNAL_TYPES.items():
        if field in source_cols:
            source_expr.append(f"TRY_CAST(s.{_ident(field)} AS {typ}) AS source_{field}")
        else:
            source_expr.append(f"CAST(NULL AS {typ}) AS source_{field}")
    con.execute("CREATE OR REPLACE TEMP VIEW source_typed AS SELECT " + ",".join(source_expr) + " FROM source_raw s")
    key_sql = ",".join(_ident(c) for c in KEY_COLUMNS)
    source_rows = int(con.execute("SELECT COUNT(*) FROM source_typed").fetchone()[0])
    conflicting = int(con.execute(f"""
        SELECT COALESCE(SUM(n), 0) FROM (
          SELECT COUNT(*) AS n FROM source_typed
          GROUP BY ALL HAVING COUNT(*) > 1
        )
    """).fetchone()[0])
    con.execute(f"""
        CREATE OR REPLACE TEMP VIEW source_unique AS
        SELECT * FROM source_typed
        QUALIFY COUNT(*) OVER (PARTITION BY {key_sql}) = 1
    """)
    base_key = " AND ".join(
        [
            "s.db_name IS NOT DISTINCT FROM CAST(p.db_name AS VARCHAR)",
            "CAST(s.year AS INTEGER) IS NOT DISTINCT FROM CAST(p.year AS INTEGER)",
            "CAST(s.week AS INTEGER) IS NOT DISTINCT FROM CAST(p.week AS INTEGER)",
            "s.NFL_player_id IS NOT DISTINCT FROM CAST(p.NFL_player_id AS VARCHAR)",
            "s.manager IS NOT DISTINCT FROM CAST(p.manager AS VARCHAR)",
            "s.team_key IS NOT DISTINCT FROM CAST(p.team_key AS VARCHAR)",
            "s.team_name IS NOT DISTINCT FROM CAST(p.team_name AS VARCHAR)",
            "LOWER(s.platform) IS NOT DISTINCT FROM LOWER(CAST(p.platform AS VARCHAR))",
        ]
    )
    con.execute(f"""
        CREATE OR REPLACE TEMP VIEW matched AS
        SELECT p.rowid AS player_rowid, s.source_file, p.*,
               s.source_win, s.source_loss, s.source_tie,
               s.source_team_points, s.source_is_playoffs, s.source_has_po_signal,
               s.source_champion, s.source_final_playoff_seed,
               s.source_made_playoffs, s.source_clutch_equity
        FROM public.player_fantasy p JOIN source_unique s ON {base_key}
    """)
    matched = int(con.execute("SELECT COUNT(*) FROM matched").fetchone()[0])
    unmatched = source_rows - (conflicting + matched)
    filters = []
    improvements: dict[str, int] = {}
    for field in SIGNAL_TYPES:
        canonical = f'm.{_ident(field)}' if field in pcols else 'NULL'
        source = f'm.source_{field}'
        filters.append(f"({canonical} IS NULL AND {source} IS NOT NULL)")
        improvements[field] = int(con.execute(f"SELECT COUNT(*) FROM matched m WHERE {canonical} IS NULL AND {source} IS NOT NULL").fetchone()[0])
    out.parent.mkdir(parents=True, exist_ok=True)
    con.execute(f"""
        COPY (
          SELECT player_rowid, source_file, db_name, year, week, NFL_player_id, manager,
                 team_key, team_name, platform, source_win, source_loss,
                 source_tie, source_team_points, source_is_playoffs AS source_playoffs,
                 source_has_po_signal, source_champion, source_final_playoff_seed,
                 source_made_playoffs, source_clutch_equity
          FROM matched m
          WHERE {' OR '.join(filters)}
        ) TO {_lit(out)} (FORMAT PARQUET)
    """)
    delta_rows = int(con.execute(f"SELECT COUNT(*) FROM read_parquet({_lit(out)})").fetchone()[0])

    # Exact artifacts can contain player rows that are absent from the
    # canonical table entirely.  Those rows are eligible for insertion only
    # when the source carries every canonical column (including the seven
    # cohort dimensions and any optional loss/tie columns).  Partial source
    # rows remain unresolved instead of being padded with invented values.
    insert_candidates = 0
    unresolved_unmatched = unmatched
    insertion_schema_complete = False
    if insert_out is not None:
        source_required = set(BASE_PLAYER_COLUMNS) | (set(OPTIONAL) & set(pcols)) | set(cohorts)
        insertion_schema_complete = source_required <= source_cols
        if insertion_schema_complete and unmatched:
            full_key = " AND ".join(
                [
                    f"s.{_ident('db_name')} IS NOT DISTINCT FROM CAST(p.db_name AS VARCHAR)",
                    f"TRY_CAST(s.{_ident('year')} AS INTEGER) IS NOT DISTINCT FROM CAST(p.year AS INTEGER)",
                    f"TRY_CAST(s.{_ident('week')} AS INTEGER) IS NOT DISTINCT FROM CAST(p.week AS INTEGER)",
                    f"s.{_ident('NFL_player_id')} IS NOT DISTINCT FROM CAST(p.NFL_player_id AS VARCHAR)",
                    f"s.{_ident('manager')} IS NOT DISTINCT FROM CAST(p.manager AS VARCHAR)",
                    f"s.{_ident('team_key')} IS NOT DISTINCT FROM CAST(p.team_key AS VARCHAR)",
                    f"s.{_ident('team_name')} IS NOT DISTINCT FROM CAST(p.team_name AS VARCHAR)",
                    f"LOWER(CAST(s.{_ident('platform')} AS VARCHAR)) IS NOT DISTINCT FROM LOWER(CAST(p.platform AS VARCHAR))",
                ]
            )
            # A source key is insertable only if it occurs once and has no
            # canonical row.  This prevents duplicate source artifacts from
            # silently creating duplicate player rows.
            con.execute(f"""
                CREATE OR REPLACE TEMP VIEW insert_source AS
                SELECT s.*
                FROM source_raw s
                WHERE NOT EXISTS (SELECT 1 FROM public.player_fantasy p WHERE {full_key})
                QUALIFY COUNT(*) OVER (PARTITION BY {key_sql}) = 1
            """)
            insert_candidates = int(con.execute("SELECT COUNT(*) FROM insert_source").fetchone()[0])
            type_map = {r[0]: r[1] for r in con.execute("DESCRIBE public.player_fantasy").fetchall()}
            select_cols = []
            for col in pcols:
                select_cols.append(f"TRY_CAST(s.{_ident(col)} AS {type_map[col]}) AS {_ident(col)}")
            insert_out.parent.mkdir(parents=True, exist_ok=True)
            con.execute(
                "COPY (SELECT " + ",".join(select_cols) +
                " FROM insert_source s) TO " + _lit(insert_out) + " (FORMAT PARQUET)"
            )
            unresolved_unmatched = unmatched - insert_candidates
        elif insert_out is not None:
            insert_out.parent.mkdir(parents=True, exist_ok=True)
            # Emit an empty, schema-correct Parquet file so downstream apply
            # can distinguish "no insert candidates" from a missing output.
            type_map = {r[0]: r[1] for r in con.execute("DESCRIBE public.player_fantasy").fetchall()}
            select_cols = [f"TRY_CAST(NULL AS {type_map[c]}) AS {_ident(c)}" for c in pcols]
            con.execute("COPY (SELECT " + ",".join(select_cols) + " WHERE FALSE) TO " + _lit(insert_out) + " (FORMAT PARQUET)")
    report = {
        "source_files": len(sources), "source_rows": source_rows,
        "matched_rows": matched, "unmatched_rows": int(unmatched),
        "insert_candidate_rows": int(insert_candidates),
        "unresolved_unmatched_rows": int(unresolved_unmatched),
        "insertion_schema_complete": bool(insertion_schema_complete),
        "source_conflicting_keys": conflicting, "delta_rows": delta_rows,
        "improvements_by_field": improvements, "canonical_schema_unchanged": True,
        "cache_mutated": False, "new_lineage": False,
    }
    con.close()
    return report


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--base", type=Path, required=True)
    ap.add_argument("--sources", nargs="+", type=Path, required=True)
    ap.add_argument("--out", type=Path, required=True)
    ap.add_argument("--insert-out", type=Path, default=None)
    ap.add_argument("--report", type=Path, required=True)
    args = ap.parse_args()
    report = build_delta(args.base, args.sources, args.out, args.insert_out)
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(report, sort_keys=True))


if __name__ == "__main__":
    main()
