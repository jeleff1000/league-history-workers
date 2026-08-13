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
    "source_loss": ("loss", "INTEGER"),
    "source_tie": ("tie", "INTEGER"),
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


def build(
    *, base: Path, manifest: Path, out: Path, report: Path,
    rejected_out: Path | None = None,
) -> dict:
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

    # Restrict the cardinality check to source identities.  Grouping all
    # canonical rows is both unnecessary and expensive for a targeted repair.
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE source_base_matches AS
        SELECT {', '.join(f's.{_q(column)} AS {_q(column)}' for column in KEY)},
               COUNT(p.rowid) AS canonical_rows, MIN(p.rowid) AS player_rowid
        FROM source_unique s
        LEFT JOIN public.player_fantasy p ON {_join('p', 's')}
        GROUP BY {', '.join(f's.{_q(column)}' for column in KEY)}
    """)
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE matched AS
        SELECT s.*, b.canonical_rows, b.player_rowid,
               {', '.join(f'p.{_q(target)} AS {_q(target)}' for target, _ in SUPPORTED.values())}
        FROM source_unique s
        JOIN source_base_matches b ON {_join('b', 's')}
        LEFT JOIN public.player_fantasy p ON p.rowid = b.player_rowid
    """)
    unmatched = int(con.execute("SELECT COUNT(*) FROM matched WHERE canonical_rows = 0").fetchone()[0])
    ambiguous = int(con.execute("SELECT COUNT(*) FROM matched WHERE canonical_rows > 1").fetchone()[0])
    ambiguous_identity_nullness = {
        "null_nfl_player_id": int(con.execute(
            "SELECT COUNT(*) FROM matched WHERE canonical_rows > 1 AND NFL_player_id IS NULL"
        ).fetchone()[0]),
        "null_manager": int(con.execute(
            "SELECT COUNT(*) FROM matched WHERE canonical_rows > 1 AND manager IS NULL"
        ).fetchone()[0]),
        "both_null": int(con.execute(
            "SELECT COUNT(*) FROM matched WHERE canonical_rows > 1 AND NFL_player_id IS NULL AND manager IS NULL"
        ).fetchone()[0]),
        "fully_specified": int(con.execute(
            "SELECT COUNT(*) FROM matched WHERE canonical_rows > 1 AND NFL_player_id IS NOT NULL AND manager IS NOT NULL"
        ).fetchone()[0]),
    }

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
    delta_fields = []
    for source_field, (target, type_name) in SUPPORTED.items():
        variants = _q(source_field + "_variants")
        delta_fields.append(
            f"CASE WHEN {variants} <= 1 AND {_q(source_field)} IS NOT NULL "
            f"AND canonical_rows = 1 AND {_q(target)} IS NULL "
            f"THEN {_q(source_field)} ELSE NULL::{type_name} END AS {_q(source_field)}"
        )
    con.execute(f"""
        COPY (
          SELECT player_rowid, {', '.join(_q(column) for column in KEY)}, source_artifact_ids,
                 {', '.join(delta_fields)}
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

    residual_null_cells_by_reason = {
        "ambiguous_canonical_identity": 0,
        "source_value_conflict": 0,
    }
    if rejected_out is not None:
        rejected_out.parent.mkdir(parents=True, exist_ok=True)
        rejected_terms: list[str] = []
        for source_field, (target, _) in SUPPORTED.items():
            variants = _q(source_field + "_variants")
            safe_source = f"{variants} <= 1 AND {_q(source_field)} IS NOT NULL"
            rejected_terms.append(f"""
                SELECT {', '.join(_q(column) for column in KEY)},
                       source_artifact_ids,
                       '{target}' AS target_field,
                       '{source_field}' AS source_field,
                       {_q(source_field)} AS source_value,
                       {_q(target)} AS canonical_value,
                       canonical_rows AS canonical_rows_for_identity,
                       CASE
                         WHEN canonical_rows > 1 THEN 'ambiguous_canonical_identity'
                         WHEN {safe_source} AND canonical_rows = 1
                              AND {_q(target)} IS NOT NULL
                              AND {_q(target)} IS DISTINCT FROM {_q(source_field)}
                           THEN 'source_value_conflict'
                       END AS rejection_reason,
                       CASE WHEN {_q(target)} IS NULL THEN 1 ELSE 0 END AS canonical_null_rows
                FROM matched
                WHERE ({safe_source} AND canonical_rows > 1 AND {_q(target)} IS NULL)
                   OR ({safe_source} AND canonical_rows = 1 AND {_q(target)} IS NOT NULL
                       AND {_q(target)} IS DISTINCT FROM {_q(source_field)})
            """)
        con.execute(
            f"COPY ({' UNION ALL '.join(rejected_terms)}) TO {_lit(rejected_out)} (FORMAT PARQUET)"
        )
        for reason in residual_null_cells_by_reason:
            residual_null_cells_by_reason[reason] = int(con.execute(
                f"SELECT COALESCE(SUM(canonical_null_rows), 0) FROM read_parquet({_lit(rejected_out)}) "
                f"WHERE rejection_reason = '{reason}'"
            ).fetchone()[0])

    artifact_current_state = []
    for artifact_id in sorted(int(entry["artifact_id"]) for entry in entries):
        metrics = con.execute(f"""
            WITH artifact_source AS (
              SELECT {', '.join(_q(column) for column in KEY)},
                     {', '.join(f'MAX({_q(source_field)}) FILTER (WHERE {_q(source_field)} IS NOT NULL) AS {_q(source_field)}' for source_field in SUPPORTED)},
                     {', '.join(f'COUNT(DISTINCT {_q(source_field)}) FILTER (WHERE {_q(source_field)} IS NOT NULL) AS {_q(source_field + "_variants")}' for source_field in SUPPORTED)}
              FROM raw_source
              WHERE artifact_id = {artifact_id}
              GROUP BY {', '.join(_q(column) for column in KEY)}
            ), joined AS (
              SELECT s.*, b.canonical_rows,
                     {', '.join(f'p.{_q(target)} AS {_q(target)}' for target, _ in SUPPORTED.values())}
              FROM artifact_source s
              JOIN source_base_matches b ON {_join('b', 's')}
              LEFT JOIN public.player_fantasy p ON p.rowid = b.player_rowid
            ), fields AS (
              {' UNION ALL '.join(f"SELECT canonical_rows, {_q(source_field)} AS source_value, {_q(source_field + '_variants')} AS variants, {_q(target)} AS canonical_value FROM joined" for source_field, (target, _) in SUPPORTED.items())}
            )
            SELECT
              (SELECT COUNT(*) FROM raw_source WHERE artifact_id = {artifact_id}) AS source_rows,
              COALESCE(SUM(CASE WHEN variants <= 1 AND source_value IS NOT NULL AND canonical_rows = 1
                                 AND canonical_value IS NOT NULL AND canonical_value IS NOT DISTINCT FROM source_value
                                THEN 1 ELSE 0 END), 0) AS cache_equal_cells,
              COALESCE(SUM(CASE WHEN variants <= 1 AND source_value IS NOT NULL AND canonical_rows = 1
                                 AND canonical_value IS NULL
                                THEN 1 ELSE 0 END), 0) AS safe_null_candidates,
              COALESCE(SUM(CASE WHEN variants <= 1 AND source_value IS NOT NULL AND canonical_rows = 1
                                 AND canonical_value IS NOT NULL AND canonical_value IS DISTINCT FROM source_value
                                THEN 1 ELSE 0 END), 0) AS cache_conflict_cells,
              COALESCE(SUM(CASE WHEN variants <= 1 AND source_value IS NOT NULL AND canonical_rows > 1
                                 AND canonical_value IS NULL
                                THEN 1 ELSE 0 END), 0) AS ambiguous_null_cells
            FROM fields
        """).fetchone()
        artifact_current_state.append({
            "artifact_id": artifact_id,
            "source_rows": int(metrics[0]),
            "cache_equal_cells": int(metrics[1]),
            "safe_null_candidates": int(metrics[2]),
            "cache_conflict_cells": int(metrics[3]),
            "ambiguous_null_cells": int(metrics[4]),
        })

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
        "ambiguous_identity_nullness": ambiguous_identity_nullness,
        "delta_rows": delta_rows,
        "residual_null_cells_by_reason": residual_null_cells_by_reason,
        "artifact_current_state": artifact_current_state,
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
    parser.add_argument("--rejected-out", type=Path)
    parser.add_argument("--report", type=Path, required=True)
    args = parser.parse_args()
    print(json.dumps(build(
        base=args.base, manifest=args.manifest, out=args.out,
        rejected_out=args.rejected_out, report=args.report,
    ), sort_keys=True))


if __name__ == "__main__":
    main()
