"""Read-only pilot for resolving manager-null MFL player rows by team points.

The bridge is admissible only when one source player-week with no manager has a
non-null source team total that identifies exactly one existing canonical
player-week/team.  It never writes the cache and it emits only canonical NULL
fills; source conflicts and tied team-point matches remain evidence only.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import duckdb


SOURCE_KEY = ("db_name", "year", "week", "NFL_player_id")
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
        required_base = set(SOURCE_KEY) | {"manager", *[target for target, _ in SUPPORTED.values()]}
        if missing := sorted(required_base - base_columns):
            raise ValueError(f"canonical player schema missing: {missing}")

        selects: list[str] = []
        for entry in entries:
            artifact_id = int(entry["artifact_id"])
            source = Path(entry["path"])
            if not source.exists():
                raise FileNotFoundError(source)
            source_columns = {
                row[0] for row in con.execute(f"DESCRIBE SELECT * FROM read_parquet({_lit(source)})").fetchall()
            }
            if missing := sorted((set(SOURCE_KEY) | {"manager"}) - source_columns):
                raise ValueError(f"{artifact_id}: source missing player bridge fields: {missing}")
            terms = [
                f"{artifact_id}::BIGINT AS artifact_id",
                *(f"{_q(column)} AS {_q(column)}" for column in SOURCE_KEY),
                f"{_q('manager')} AS source_manager",
            ]
            for source_field, (_, type_name) in SUPPORTED.items():
                expression = f"TRY_CAST({_q(source_field)} AS {type_name})" if source_field in source_columns else f"NULL::{type_name}"
                terms.append(f"{expression} AS {_q(source_field)}")
            selects.append(f"SELECT {', '.join(terms)} FROM read_parquet({_lit(source)})")
        con.execute("CREATE OR REPLACE TEMP TABLE raw_source AS " + " UNION ALL ".join(selects))

        aggregate = [
            "string_agg(DISTINCT CAST(artifact_id AS VARCHAR), '|' ORDER BY CAST(artifact_id AS VARCHAR)) AS source_artifact_ids"
        ]
        for source_field in SUPPORTED:
            aggregate.extend([
                f"COUNT(DISTINCT {_q(source_field)}) FILTER (WHERE {_q(source_field)} IS NOT NULL) AS {_q(source_field + '_variants')}",
                f"MAX({_q(source_field)}) FILTER (WHERE {_q(source_field)} IS NOT NULL) AS {_q(source_field)}",
            ])
        con.execute(f"""
            CREATE OR REPLACE TEMP TABLE source_unique AS
            SELECT {', '.join(_q(column) for column in SOURCE_KEY)}, {', '.join(aggregate)}
            FROM raw_source
            WHERE source_manager IS NULL AND NFL_player_id IS NOT NULL
                  AND source_team_points IS NOT NULL
            GROUP BY {', '.join(_q(column) for column in SOURCE_KEY)}
        """)
        source_keys = int(con.execute("SELECT COUNT(*) FROM source_unique").fetchone()[0])
        con.execute(f"""
            CREATE OR REPLACE TEMP TABLE bridged AS
            SELECT s.*, COUNT(p.rowid) AS canonical_rows, MIN(p.rowid) AS player_rowid,
                   MIN(p.manager) AS manager,
                   {', '.join(f'MIN(p.{_q(target)}) AS {_q(target)}' for target, _ in SUPPORTED.values())}
            FROM source_unique s
            LEFT JOIN public.player_fantasy p
              ON p.db_name IS NOT DISTINCT FROM s.db_name
             AND p.year IS NOT DISTINCT FROM s.year
             AND p.week IS NOT DISTINCT FROM s.week
             AND p.NFL_player_id IS NOT DISTINCT FROM s.NFL_player_id
             AND p.manager IS NOT NULL
             AND p.team_points IS NOT DISTINCT FROM s.source_team_points
            GROUP BY {', '.join(f's.{_q(column)}' for column in SOURCE_KEY)},
                     s.source_artifact_ids,
                     {', '.join(f's.{_q(source_field + suffix)}' for source_field in SUPPORTED for suffix in ('', '_variants'))}
        """)
        source_conflict_predicate = " OR ".join(f"{_q(field + '_variants')} > 1" for field in SUPPORTED)
        source_conflicts = int(con.execute(f"SELECT COUNT(*) FROM bridged WHERE {source_conflict_predicate}").fetchone()[0])
        bridge_counts = {
            "unique_team_points_bridges": int(con.execute("SELECT COUNT(*) FROM bridged WHERE canonical_rows = 1").fetchone()[0]),
            "ambiguous_team_points_bridges": int(con.execute("SELECT COUNT(*) FROM bridged WHERE canonical_rows > 1").fetchone()[0]),
            "unmatched_team_points_bridges": int(con.execute("SELECT COUNT(*) FROM bridged WHERE canonical_rows = 0").fetchone()[0]),
        }
        artifact_bridge_state = []
        for artifact_id in sorted(int(entry["artifact_id"]) for entry in entries):
            metrics = con.execute(f"""
                WITH source_keys AS (
                  SELECT {', '.join(_q(column) for column in SOURCE_KEY)},
                         MAX(source_team_points) AS source_team_points
                  FROM raw_source
                  WHERE artifact_id = {artifact_id}
                    AND source_manager IS NULL
                    AND NFL_player_id IS NOT NULL
                    AND source_team_points IS NOT NULL
                  GROUP BY {', '.join(_q(column) for column in SOURCE_KEY)}
                ), candidates AS (
                  SELECT s.{', s.'.join(_q(column) for column in SOURCE_KEY)},
                         s.source_team_points,
                         COUNT(p.rowid) AS canonical_player_rows,
                         COUNT(p.rowid) FILTER (
                           WHERE p.manager IS NOT NULL
                             AND LOWER(TRIM(p.manager)) NOT IN ('unrostered', 'fa', 'free agent', 'waivers')
                         ) AS assigned_player_rows,
                         COUNT(p.rowid) FILTER (
                           WHERE p.manager IS NOT NULL
                             AND LOWER(TRIM(p.manager)) NOT IN ('unrostered', 'fa', 'free agent', 'waivers')
                             AND p.team_points IS NOT DISTINCT FROM s.source_team_points
                         ) AS canonical_rows,
                         COUNT(p.rowid) FILTER (
                           WHERE p.manager IS NOT NULL
                             AND LOWER(TRIM(p.manager)) NOT IN ('unrostered', 'fa', 'free agent', 'waivers')
                             AND p.team_points IS NULL
                         ) AS assigned_null_team_points_rows,
                         COUNT(p.rowid) FILTER (
                           WHERE p.manager IS NOT NULL
                             AND LOWER(TRIM(p.manager)) NOT IN ('unrostered', 'fa', 'free agent', 'waivers')
                             AND p.team_points IS NOT NULL
                             AND p.team_points IS DISTINCT FROM s.source_team_points
                         ) AS assigned_different_team_points_rows
                  FROM source_keys s
                  LEFT JOIN public.player_fantasy p
                    ON p.db_name IS NOT DISTINCT FROM s.db_name
                   AND p.year IS NOT DISTINCT FROM s.year
                   AND p.week IS NOT DISTINCT FROM s.week
                   AND p.NFL_player_id IS NOT DISTINCT FROM s.NFL_player_id
                  GROUP BY {', '.join(f's.{_q(column)}' for column in SOURCE_KEY)}, s.source_team_points
                )
                SELECT COUNT(*),
                       COUNT(*) FILTER (WHERE canonical_rows = 1),
                       COUNT(*) FILTER (WHERE canonical_rows > 1),
                       COUNT(*) FILTER (WHERE canonical_rows = 0),
                       COUNT(*) FILTER (WHERE canonical_player_rows = 0),
                       COUNT(*) FILTER (WHERE canonical_player_rows > 0 AND assigned_player_rows = 0),
                       COUNT(*) FILTER (
                         WHERE canonical_rows = 0 AND assigned_player_rows > 0
                           AND assigned_different_team_points_rows > 0
                       ),
                       COUNT(*) FILTER (
                         WHERE canonical_rows = 0 AND assigned_player_rows > 0
                           AND assigned_different_team_points_rows = 0
                           AND assigned_null_team_points_rows > 0
                       )
                FROM candidates
            """).fetchone()
            artifact_bridge_state.append({
                "artifact_id": artifact_id,
                "manager_null_source_keys_with_team_points": int(metrics[0]),
                "unique_team_points_bridges": int(metrics[1]),
                "ambiguous_team_points_bridges": int(metrics[2]),
                "unmatched_team_points_bridges": int(metrics[3]),
                "canonical_player_absent": int(metrics[4]),
                "canonical_only_unassigned": int(metrics[5]),
                "canonical_same_player_different_team_points": int(metrics[6]),
                "canonical_same_player_null_team_points": int(metrics[7]),
            })

        candidate_fields: list[str] = []
        output_fields: list[str] = []
        null_fill_counts: dict[str, int] = {}
        for source_field, (target, type_name) in SUPPORTED.items():
            safe = f"{_q(source_field + '_variants')} <= 1 AND {_q(source_field)} IS NOT NULL AND canonical_rows = 1 AND {_q(target)} IS NULL"
            candidate_fields.append(f"({safe})")
            output_fields.append(
                f"CASE WHEN {safe} THEN {_q(source_field)} ELSE NULL::{type_name} END AS {_q(source_field)}"
            )
            null_fill_counts[target] = int(con.execute(f"SELECT COUNT(*) FROM bridged WHERE {safe}").fetchone()[0])

        out.parent.mkdir(parents=True, exist_ok=True)
        con.execute(f"""
            COPY (
              SELECT player_rowid, {', '.join(_q(column) for column in SOURCE_KEY)}, manager,
                     source_artifact_ids, {', '.join(output_fields)}
              FROM bridged
              WHERE {' OR '.join(candidate_fields)}
            ) TO {_lit(out)} (FORMAT PARQUET)
        """)
        null_fill_rows = int(con.execute(f"SELECT COUNT(*) FROM read_parquet({_lit(out)})").fetchone()[0])
        duplicate_rows = int(con.execute(f"SELECT COUNT(*) FROM (SELECT player_rowid FROM read_parquet({_lit(out)}) GROUP BY 1 HAVING COUNT(*) > 1)").fetchone()[0])
        if duplicate_rows:
            raise RuntimeError(f"team-points bridge emitted duplicate canonical rows: {duplicate_rows}")

        result = {
            "read_only": True,
            "cache_mutated": False,
            "new_lineage": False,
            "canonical_schema_unchanged": True,
            "source_artifacts": len(entries),
            "manager_null_source_keys_with_team_points": source_keys,
            "source_conflicting_player_keys": source_conflicts,
            **bridge_counts,
            "artifact_bridge_state": artifact_bridge_state,
            "null_fill_rows": null_fill_rows,
            "null_fill_cells_by_field": null_fill_counts,
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
