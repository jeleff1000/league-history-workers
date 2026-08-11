"""Build a read-only, exact-player-key null-fill candidate from source artifacts.

Structured player artifacts carry outcome evidence for a specific canonical
player-week.  They are not complete player snapshots, so this builder never
inserts player rows.  It emits only supported source values where one and only
one canonical player row matches the exact source identity and the canonical
cell is NULL.
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


def _q(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _lit(path: Path) -> str:
    return "'" + str(path.resolve()).replace("'", "''") + "'"


def _join(left: str, right: str) -> str:
    return " AND ".join(
        f"{left}.{_q(column)} IS NOT DISTINCT FROM {right}.{_q(column)}"
        for column in KEY
    )


def _write_json(path: Path, value: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def build(*, base: Path, manifest: Path, out: Path, report: Path) -> dict:
    entries = json.loads(manifest.read_text(encoding="utf-8"))
    if not isinstance(entries, list) or not entries:
        raise ValueError("manifest must be a non-empty list")
    con = duckdb.connect(str(base), read_only=True)
    base_columns = {row[0] for row in con.execute("DESCRIBE public.player_fantasy").fetchall()}
    required = set(KEY) | {target for target, _ in SUPPORTED.values()}
    if missing := sorted(required - base_columns):
        raise ValueError(f"canonical player schema missing: {missing}")

    selects: list[str] = []
    for entry in entries:
        artifact_id = int(entry["artifact_id"])
        source = Path(entry["path"])
        if not source.exists():
            raise FileNotFoundError(source)
        source_columns = {
            row[0]
            for row in con.execute(f"DESCRIBE SELECT * FROM read_parquet({_lit(source)})").fetchall()
        }
        if missing := sorted(set(KEY) - source_columns):
            raise ValueError(f"{artifact_id}: source missing player key fields: {missing}")
        terms = [
            f"{artifact_id}::BIGINT AS artifact_id",
            *(f"{_q(column)} AS {_q(column)}" for column in KEY),
        ]
        for source_field, (_, type_name) in SUPPORTED.items():
            expression = (
                f"TRY_CAST({_q(source_field)} AS {type_name})"
                if source_field in source_columns else f"NULL::{type_name}"
            )
            terms.append(f"{expression} AS {_q(source_field)}")
        selects.append(f"SELECT {', '.join(terms)} FROM read_parquet({_lit(source)})")
    con.execute("CREATE OR REPLACE TEMP TABLE raw_source AS " + " UNION ALL ".join(selects))
    raw_rows = int(con.execute("SELECT COUNT(*) FROM raw_source").fetchone()[0])

    aggregate_terms = [
        "string_agg(DISTINCT CAST(artifact_id AS VARCHAR), '|' ORDER BY CAST(artifact_id AS VARCHAR)) AS source_artifact_ids"
    ]
    for source_field in SUPPORTED:
        aggregate_terms.extend([
            f"COUNT(DISTINCT {_q(source_field)}) FILTER (WHERE {_q(source_field)} IS NOT NULL) AS {_q(source_field + '_variants')}",
            f"MAX({_q(source_field)}) FILTER (WHERE {_q(source_field)} IS NOT NULL) AS {_q(source_field)}",
        ])
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE source_unique AS
        SELECT {', '.join(_q(column) for column in KEY)}, {', '.join(aggregate_terms)}
        FROM raw_source
        GROUP BY {', '.join(_q(column) for column in KEY)}
    """)
    source_keys = int(con.execute("SELECT COUNT(*) FROM source_unique").fetchone()[0])
    conflict_predicate = " OR ".join(
        f"{_q(source_field + '_variants')} > 1" for source_field in SUPPORTED
    )
    source_conflicts = int(con.execute(
        f"SELECT COUNT(*) FROM source_unique WHERE {conflict_predicate}"
    ).fetchone()[0])

    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE base_key_counts AS
        SELECT {', '.join(_q(column) for column in KEY)},
               COUNT(*) AS canonical_rows, MIN(rowid) AS player_rowid
        FROM public.player_fantasy
        GROUP BY {', '.join(_q(column) for column in KEY)}
    """)
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE matched AS
        SELECT s.*, b.canonical_rows, b.player_rowid,
               {', '.join(f'p.{_q(target)} AS {_q(target)}' for target, _ in SUPPORTED.values())}
        FROM source_unique s
        LEFT JOIN base_key_counts b ON {_join('b', 's')}
        LEFT JOIN public.player_fantasy p ON p.rowid = b.player_rowid
    """)
    unmatched = int(con.execute("SELECT COUNT(*) FROM matched WHERE canonical_rows IS NULL").fetchone()[0])
    ambiguous = int(con.execute("SELECT COUNT(*) FROM matched WHERE canonical_rows > 1").fetchone()[0])

    report_fields: dict[str, dict[str, int]] = {}
    filters: list[str] = []
    for source_field, (target, _) in SUPPORTED.items():
        variants = _q(source_field + "_variants")
        safe = f"{variants} <= 1 AND {_q(source_field)} IS NOT NULL AND canonical_rows = 1"
        report_fields[target] = {
            "source_non_null_keys": int(con.execute(
                f"SELECT COUNT(*) FROM matched WHERE {_q(source_field)} IS NOT NULL"
            ).fetchone()[0]),
            "source_conflicting_keys": int(con.execute(
                f"SELECT COUNT(*) FROM matched WHERE {variants} > 1"
            ).fetchone()[0]),
            "cache_equal": int(con.execute(
                f"SELECT COUNT(*) FROM matched WHERE {safe} AND {_q(target)} IS NOT NULL AND {_q(target)} IS NOT DISTINCT FROM {_q(source_field)}"
            ).fetchone()[0]),
            "cache_null_candidates": int(con.execute(
                f"SELECT COUNT(*) FROM matched WHERE {safe} AND {_q(target)} IS NULL"
            ).fetchone()[0]),
            "cache_conflicts": int(con.execute(
                f"SELECT COUNT(*) FROM matched WHERE {safe} AND {_q(target)} IS NOT NULL AND {_q(target)} IS DISTINCT FROM {_q(source_field)}"
            ).fetchone()[0]),
        }
        filters.append(f"({safe} AND {_q(target)} IS NULL)")

    out.parent.mkdir(parents=True, exist_ok=True)
    con.execute(f"""
        COPY (
          SELECT player_rowid, {', '.join(_q(column) for column in KEY)}, source_artifact_ids,
                 {', '.join(_q(source_field) for source_field in SUPPORTED)}
          FROM matched
          WHERE {' OR '.join(filters)}
        ) TO {_lit(out)} (FORMAT PARQUET)
    """)
    delta_rows = int(con.execute(f"SELECT COUNT(*) FROM read_parquet({_lit(out)})").fetchone()[0])
    duplicate_rows = int(con.execute(
        f"SELECT COUNT(*) FROM (SELECT player_rowid FROM read_parquet({_lit(out)}) GROUP BY player_rowid HAVING COUNT(*) > 1)"
    ).fetchone()[0])
    if duplicate_rows:
        raise RuntimeError(f"delta has {duplicate_rows} duplicate canonical player rows")

    result = {
        "read_only": True,
        "cache_mutated": False,
        "new_lineage": False,
        "canonical_schema_unchanged": True,
        "source_artifacts": len(entries),
        "source_rows": raw_rows,
        "unique_source_player_keys": source_keys,
        "source_conflicting_player_keys": source_conflicts,
        "unmatched_canonical_player_keys": unmatched,
        "ambiguous_canonical_player_keys": ambiguous,
        "delta_rows": delta_rows,
        "supported_fields": report_fields,
    }
    _write_json(report, result)
    con.close()
    return result


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base", type=Path, required=True)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--report", type=Path, required=True)
    args = parser.parse_args()
    print(json.dumps(build(base=args.base, manifest=args.manifest, out=args.out, report=args.report), sort_keys=True))


if __name__ == "__main__":
    main()
