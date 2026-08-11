"""Build a null-fill-only overlay from MFL player outcome classifications.

The source files are player-indexed outcome evidence, not complete player
snapshots.  This script therefore never inserts rows and never changes the
base database. It only emits exact-key candidate values for canonical NULL
cells in fields supported by the current schema.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import duckdb


KEY = ["db_name", "year", "week", "NFL_player_id", "manager"]
SUPPORTED = {
    "source_win": ("win", "INTEGER"),
    "source_team_points": ("team_points", "DOUBLE"),
    "source_is_playoffs": ("is_playoffs", "INTEGER"),
}
BLOCKED = ("source_loss", "source_tie")


def _q(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _lit(path: Path) -> str:
    return "'" + str(path.resolve()).replace("'", "''") + "'"


def build(*, base: Path, manifest: Path, out: Path, report: Path) -> dict:
    entries = json.loads(manifest.read_text(encoding="utf-8"))
    if not isinstance(entries, list) or not entries:
        raise ValueError("manifest must be a non-empty list")
    con = duckdb.connect(str(base), read_only=True)
    base_columns = {row[0] for row in con.execute("DESCRIBE public.player_fantasy").fetchall()}
    missing = (set(KEY) | {target for target, _ in SUPPORTED.values()}) - base_columns
    if missing:
        raise ValueError(f"canonical player schema missing: {sorted(missing)}")

    selects: list[str] = []
    for entry in entries:
        artifact_id = int(entry["artifact_id"])
        source = Path(entry["path"])
        if not source.exists():
            raise FileNotFoundError(source)
        raw_columns = {r[0] for r in con.execute(f"DESCRIBE SELECT * FROM read_parquet({_lit(source)})").fetchall()}
        required = set(KEY)
        if not required <= raw_columns:
            raise ValueError(f"{artifact_id}: source missing exact key {sorted(required - raw_columns)}")
        terms = [f"{artifact_id}::BIGINT AS artifact_id", *(_q(key) for key in KEY)]
        for source_field, (_, type_name) in SUPPORTED.items():
            expr = f"TRY_CAST({_q(source_field)} AS {type_name})" if source_field in raw_columns else f"NULL::{type_name}"
            terms.append(f"{expr} AS {_q(source_field)}")
        for source_field in BLOCKED:
            expr = f"TRY_CAST({_q(source_field)} AS INTEGER)" if source_field in raw_columns else "NULL::INTEGER"
            terms.append(f"{expr} AS {_q(source_field)}")
        selects.append(f"SELECT {', '.join(terms)} FROM read_parquet({_lit(source)})")
    con.execute("CREATE OR REPLACE TEMP TABLE raw_source AS " + " UNION ALL ".join(selects))
    raw_rows = int(con.execute("SELECT COUNT(*) FROM raw_source").fetchone()[0])

    field_selects = []
    for source_field in [*SUPPORTED, *BLOCKED]:
        field_selects.extend([
            f"COUNT(DISTINCT {_q(source_field)}) FILTER (WHERE {_q(source_field)} IS NOT NULL) AS {_q(source_field + '_variants')}",
            f"MAX({_q(source_field)}) FILTER (WHERE {_q(source_field)} IS NOT NULL) AS {_q(source_field)}",
        ])
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE source_unique AS
        SELECT {', '.join(_q(key) for key in KEY)},
               string_agg(DISTINCT CAST(artifact_id AS VARCHAR), '|' ORDER BY CAST(artifact_id AS VARCHAR)) AS source_artifact_ids,
               {', '.join(field_selects)}
        FROM raw_source
        GROUP BY {', '.join(_q(key) for key in KEY)}
    """)
    source_keys = int(con.execute("SELECT COUNT(*) FROM source_unique").fetchone()[0])
    base_duplicate_keys = int(con.execute(f"""
        SELECT COUNT(*) FROM (
          SELECT {', '.join(_q(key) for key in KEY)}
          FROM public.player_fantasy GROUP BY ALL HAVING COUNT(*) > 1
        )
    """).fetchone()[0])
    if base_duplicate_keys:
        raise RuntimeError(f"canonical player table has {base_duplicate_keys} duplicate exact MFL outcome keys")

    join = " AND ".join(f"p.{_q(key)} IS NOT DISTINCT FROM s.{_q(key)}" for key in KEY)
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE matched AS
        SELECT p.rowid AS player_rowid, p.*, s.* EXCLUDE ({', '.join(_q(key) for key in KEY)})
        FROM source_unique s LEFT JOIN public.player_fantasy p ON {join}
    """)
    unmatched = int(con.execute("SELECT COUNT(*) FROM matched WHERE player_rowid IS NULL").fetchone()[0])
    if unmatched:
        raise RuntimeError(f"{unmatched} exact player source keys have no canonical row")

    report_fields: dict[str, dict[str, int]] = {}
    for source_field, (target, _) in SUPPORTED.items():
        variants = f"{source_field}_variants"
        report_fields[target] = {
            "source_non_null_keys": int(con.execute(f"SELECT COUNT(*) FROM matched WHERE {_q(source_field)} IS NOT NULL").fetchone()[0]),
            "source_conflicting_keys": int(con.execute(f"SELECT COUNT(*) FROM matched WHERE {_q(variants)} > 1").fetchone()[0]),
            "cache_equal": int(con.execute(f"SELECT COUNT(*) FROM matched WHERE {_q(variants)} <= 1 AND {_q(source_field)} IS NOT NULL AND {_q(target)} IS NOT NULL AND {_q(target)} IS NOT DISTINCT FROM {_q(source_field)}").fetchone()[0]),
            "cache_null_candidates": int(con.execute(f"SELECT COUNT(*) FROM matched WHERE {_q(variants)} <= 1 AND {_q(source_field)} IS NOT NULL AND {_q(target)} IS NULL").fetchone()[0]),
            "cache_conflicts": int(con.execute(f"SELECT COUNT(*) FROM matched WHERE {_q(variants)} <= 1 AND {_q(source_field)} IS NOT NULL AND {_q(target)} IS NOT NULL AND {_q(target)} IS DISTINCT FROM {_q(source_field)}").fetchone()[0]),
        }
    blocked = {
        source_field.removeprefix("source_"): {
            "source_non_null_keys": int(con.execute(f"SELECT COUNT(*) FROM matched WHERE {_q(source_field)} IS NOT NULL").fetchone()[0]),
            "source_conflicting_keys": int(con.execute(f"SELECT COUNT(*) FROM matched WHERE {_q(source_field + '_variants')} > 1").fetchone()[0]),
        }
        for source_field in BLOCKED
    }
    filters = [
        f"({_q(source)} IS NOT NULL AND {_q(source + '_variants')} <= 1 AND {_q(target)} IS NULL)"
        for source, (target, _) in SUPPORTED.items()
    ]
    out.parent.mkdir(parents=True, exist_ok=True)
    con.execute(f"""
        COPY (
          SELECT {', '.join(_q(key) for key in KEY)}, source_artifact_ids,
                 {', '.join(_q(source) for source in SUPPORTED)}
          FROM matched WHERE {' OR '.join(filters)}
        ) TO {_lit(out)} (FORMAT PARQUET)
    """)
    delta_rows = int(con.execute(f"SELECT COUNT(*) FROM read_parquet({_lit(out)})").fetchone()[0])
    duplicate_delta_keys = int(con.execute(f"SELECT COUNT(*) FROM (SELECT {', '.join(_q(key) for key in KEY)} FROM read_parquet({_lit(out)}) GROUP BY ALL HAVING COUNT(*) > 1)").fetchone()[0])
    if duplicate_delta_keys:
        raise RuntimeError(f"delta has {duplicate_delta_keys} duplicate keys")
    result = {
        "read_only": True,
        "cache_mutated": False,
        "new_lineage": False,
        "canonical_schema_unchanged": True,
        "source_artifacts": len(entries),
        "source_rows": raw_rows,
        "unique_source_keys": source_keys,
        "canonical_duplicate_exact_keys": base_duplicate_keys,
        "unmatched_canonical_keys": unmatched,
        "delta_rows": delta_rows,
        "supported_fields": report_fields,
        "blocked_schema_fields": blocked,
    }
    report.parent.mkdir(parents=True, exist_ok=True)
    report.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    con.close()
    return result


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--base", type=Path, required=True)
    ap.add_argument("--manifest", type=Path, required=True)
    ap.add_argument("--out", type=Path, required=True)
    ap.add_argument("--report", type=Path, required=True)
    args = ap.parse_args()
    print(json.dumps(build(base=args.base, manifest=args.manifest, out=args.out, report=args.report), sort_keys=True))


if __name__ == "__main__":
    main()
