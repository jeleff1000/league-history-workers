"""Build a read-only NULL-fill candidate from unique source manager-week signals.

The source rows are team outcomes, not player rows.  A fan-out is allowed only
when one normalized source manager represents one source team in a league-week;
the signal is then applied only to existing canonical player rows for that same
manager-week and only where the target cell is NULL.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import duckdb


TEAM_KEY = ("db_name", "year", "week", "source_manager")
PLAYER_KEY = ("db_name", "year", "week", "NFL_player_id", "manager")
SUPPORTED = {
    "win": ("win", "INTEGER"),
    "team_points": ("team_points", "DOUBLE"),
    "is_playoffs": ("is_playoffs", "INTEGER"),
    "champion": ("champion", "INTEGER"),
}


def _q(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _lit(path: Path) -> str:
    return "'" + str(path.resolve()).replace("'", "''") + "'"


def _write_json(path: Path, value: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def build(*, base: Path, manifest: Path, out: Path, report: Path) -> dict:
    entries = json.loads(manifest.read_text(encoding="utf-8"))
    if not isinstance(entries, list) or not entries:
        raise ValueError("manifest must be a non-empty list")
    con = duckdb.connect(str(base), read_only=True)
    try:
        base_columns = {row[0] for row in con.execute("DESCRIBE public.player_fantasy").fetchall()}
        required_base = set(PLAYER_KEY) | set(SUPPORTED)
        if missing := sorted(required_base - base_columns):
            raise ValueError(f"canonical player schema missing: {missing}")

        selects: list[str] = []
        for entry in entries:
            artifact_id = int(entry["artifact_id"])
            source = Path(entry["path"])
            if not source.exists():
                raise FileNotFoundError(source)
            source_columns = {row[0] for row in con.execute(f"DESCRIBE SELECT * FROM read_parquet({_lit(source)})").fetchall()}
            required_source = {"db_name", "year", "week", "manager", "team_key"}
            if missing := sorted(required_source - source_columns):
                raise ValueError(f"{artifact_id}: source missing team identity fields: {missing}")
            terms = [
                f"{artifact_id}::BIGINT AS artifact_id",
                "db_name", "year", "week",
                "LOWER(TRIM(manager)) AS source_manager",
                "team_key",
            ]
            for field, (_, type_name) in SUPPORTED.items():
                expression = f"TRY_CAST({_q(field)} AS {type_name})" if field in source_columns else f"NULL::{type_name}"
                terms.append(f"{expression} AS {_q('source_' + field)}")
            selects.append(f"SELECT {', '.join(terms)} FROM read_parquet({_lit(source)}) WHERE manager IS NOT NULL AND TRIM(manager) <> ''")
        con.execute("CREATE OR REPLACE TEMP TABLE raw_source AS " + " UNION ALL ".join(selects))
        raw_rows = int(con.execute("SELECT COUNT(*) FROM raw_source").fetchone()[0])

        aggregate = [
            "string_agg(DISTINCT CAST(artifact_id AS VARCHAR), '|' ORDER BY CAST(artifact_id AS VARCHAR)) AS source_artifact_ids",
            "COUNT(DISTINCT team_key) AS team_key_variants",
        ]
        for field in SUPPORTED:
            aggregate.extend([
                f"COUNT(DISTINCT {_q('source_' + field)}) FILTER (WHERE {_q('source_' + field)} IS NOT NULL) AS {_q('source_' + field + '_variants')}",
                f"MAX({_q('source_' + field)}) FILTER (WHERE {_q('source_' + field)} IS NOT NULL) AS {_q('source_' + field)}",
            ])
        con.execute(f"""
            CREATE OR REPLACE TEMP TABLE source_unique AS
            SELECT db_name, year, week, source_manager, {', '.join(aggregate)}
            FROM raw_source
            GROUP BY db_name, year, week, source_manager
        """)
        source_team_weeks = int(con.execute("SELECT COUNT(*) FROM source_unique").fetchone()[0])
        conflict_terms = ["team_key_variants > 1", *(f"{_q('source_' + field + '_variants')} > 1" for field in SUPPORTED)]
        conflict_predicate = " OR ".join(conflict_terms)
        conflicts = int(con.execute(f"SELECT COUNT(*) FROM source_unique WHERE {conflict_predicate}").fetchone()[0])
        safe_source = f"NOT ({conflict_predicate})"

        con.execute(f"""
            CREATE OR REPLACE TEMP TABLE matched AS
            SELECT p.rowid AS player_rowid,
                   p.db_name, p.year, p.week, p.NFL_player_id, p.manager,
                   s.source_artifact_ids,
                   {', '.join(f's.{_q('source_' + field)}' for field in SUPPORTED)},
                   {', '.join(f'p.{_q(target)} AS {_q('canonical_' + target)}' for target, _ in SUPPORTED.values())}
            FROM source_unique s
            JOIN public.player_fantasy p
              ON p.db_name IS NOT DISTINCT FROM s.db_name
             AND p.year IS NOT DISTINCT FROM s.year
             AND p.week IS NOT DISTINCT FROM s.week
             AND LOWER(TRIM(p.manager)) IS NOT DISTINCT FROM s.source_manager
            WHERE {safe_source}
        """)
        matched_player_rows = int(con.execute("SELECT COUNT(*) FROM matched").fetchone()[0])

        fields: list[str] = []
        filters: list[str] = []
        fill_cells: dict[str, int] = {}
        for field, (target, type_name) in SUPPORTED.items():
            predicate = f"{_q('source_' + field)} IS NOT NULL AND {_q('canonical_' + target)} IS NULL"
            fields.append(f"CASE WHEN {predicate} THEN {_q('source_' + field)} ELSE NULL::{type_name} END AS {_q('source_' + field)}")
            filters.append(f"({predicate})")
            fill_cells[target] = int(con.execute(f"SELECT COUNT(*) FROM matched WHERE {predicate}").fetchone()[0])
        out.parent.mkdir(parents=True, exist_ok=True)
        con.execute(f"""
            COPY (
              SELECT player_rowid, {', '.join(_q(column) for column in PLAYER_KEY)},
                     source_artifact_ids, {', '.join(fields)}
              FROM matched
              WHERE {' OR '.join(filters)}
            ) TO {_lit(out)} (FORMAT PARQUET)
        """)
        delta_rows = int(con.execute(f"SELECT COUNT(*) FROM read_parquet({_lit(out)})").fetchone()[0])
        duplicate_rows = int(con.execute(f"SELECT COUNT(*) FROM (SELECT player_rowid FROM read_parquet({_lit(out)}) GROUP BY 1 HAVING COUNT(*) > 1)").fetchone()[0])
        if duplicate_rows:
            raise RuntimeError(f"manager fan-out emitted duplicate canonical player rows: {duplicate_rows}")
        result = {
            "read_only": True,
            "cache_mutated": False,
            "new_lineage": False,
            "canonical_schema_unchanged": True,
            "source_artifacts": len(entries),
            "source_team_weeks": source_team_weeks,
            "conflicting_source_team_weeks": conflicts,
            "safe_source_team_weeks": source_team_weeks - conflicts,
            "matched_player_rows": matched_player_rows,
            "delta_rows": delta_rows,
            "null_fill_cells_by_field": fill_cells,
            "join_contract": ["db_name", "year", "week", "normalized_manager"],
        }
        _write_json(report, result)
        return result
    finally:
        con.close()


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
