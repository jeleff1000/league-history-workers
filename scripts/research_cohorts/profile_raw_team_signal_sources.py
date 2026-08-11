"""Batch-profile retained team/week artifacts against the frozen player cache.

This is diagnostic evidence for the artifact ledger.  It intentionally uses
the same exact team-week identity as the receipt reader:
``(db_name, year, week, team_key)``.  All raw sources are materialized once,
then joined to ``player_fantasy`` once; it must never scan the canonical table
once per source artifact.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import duckdb


TEAM_KEY = ("db_name", "year", "week", "team_key")


def _sql_path(path: Path) -> str:
    return str(path.resolve()).replace("\\", "/").replace("'", "''")


def _manifest_entries(manifest: Path) -> list[dict[str, Any]]:
    entries = json.loads(manifest.read_text(encoding="utf-8"))
    if not isinstance(entries, list) or not entries:
        raise SystemExit("team-signal manifest must be a non-empty JSON list")
    seen_ids: set[int] = set()
    result: list[dict[str, Any]] = []
    for entry in entries:
        artifact_id = int(entry["artifact_id"])
        path = Path(entry["path"])
        if artifact_id in seen_ids:
            raise SystemExit(f"duplicate artifact_id in manifest: {artifact_id}")
        if not path.exists() or path.stat().st_size == 0:
            raise SystemExit(f"{artifact_id}: source parquet missing or empty: {path}")
        seen_ids.add(artifact_id)
        result.append({"artifact_id": artifact_id, "path": _sql_path(path)})
    return result


def profile(*, base: Path, manifest: Path) -> dict[str, Any]:
    """Return per-artifact exact-team-key coverage without changing ``base``."""
    entries = _manifest_entries(manifest)
    con = duckdb.connect(str(base), read_only=True)
    try:
        player_columns = {row[0] for row in con.execute("DESCRIBE public.player_fantasy").fetchall()}
        missing_player_columns = set(TEAM_KEY) - player_columns
        if missing_player_columns:
            raise SystemExit(f"canonical player table missing team-key columns: {sorted(missing_player_columns)}")

        paths = ",".join("'" + entry["path"] + "'" for entry in entries)
        con.execute(
            "CREATE OR REPLACE TEMP VIEW raw_sources AS "
            f"SELECT * FROM read_parquet([{paths}], union_by_name=true, filename=true)"
        )
        raw_columns = {row[0] for row in con.execute("DESCRIBE raw_sources").fetchall()}
        missing_source_columns = set(TEAM_KEY) - raw_columns
        if missing_source_columns:
            raise SystemExit(f"raw team source missing required key columns: {sorted(missing_source_columns)}")

        con.execute("CREATE OR REPLACE TEMP TABLE manifest_entries (artifact_id BIGINT, path VARCHAR)")
        con.executemany(
            "INSERT INTO manifest_entries VALUES (?, ?)",
            [(entry["artifact_id"], entry["path"]) for entry in entries],
        )
        # DuckDB normalizes paths differently on Windows and Linux.  Normalize
        # both sides before matching so the manifest remains the sole source of
        # artifact attribution.
        con.execute("""
            CREATE OR REPLACE TEMP TABLE source_rows AS
            SELECT
              m.artifact_id,
              CAST(r.db_name AS VARCHAR) AS db_name,
              CAST(r."year" AS INTEGER) AS year,
              CAST(r.week AS INTEGER) AS week,
              NULLIF(TRIM(CAST(r.team_key AS VARCHAR)), '') AS team_key
            FROM raw_sources r
            JOIN manifest_entries m
              ON REPLACE(CAST(r.filename AS VARCHAR), '\\', '/') = m.path
        """)
        source_rows = con.execute("SELECT COUNT(*) FROM source_rows").fetchone()[0]
        if not source_rows:
            raise SystemExit("source manifest produced zero attributable rows")
        con.execute("""
            CREATE OR REPLACE TEMP TABLE source_team_keys AS
            SELECT artifact_id, db_name, year, week, team_key, COUNT(*) AS source_rows
            FROM source_rows
            WHERE team_key IS NOT NULL
            GROUP BY 1,2,3,4,5
        """)
        # This is the sole canonical-table access: DuckDB builds from the small
        # source key relation and performs one exact team-week join.
        con.execute("""
            CREATE OR REPLACE TEMP TABLE matched_team_keys AS
            SELECT DISTINCT s.artifact_id, s.db_name, s.year, s.week, s.team_key
            FROM source_team_keys s
            JOIN public.player_fantasy p
              ON p.db_name IS NOT DISTINCT FROM s.db_name
             AND p."year" IS NOT DISTINCT FROM s.year
             AND p.week IS NOT DISTINCT FROM s.week
             AND p.team_key IS NOT DISTINCT FROM s.team_key
        """)
        rows = []
        for row in con.execute("""
            WITH raw_counts AS (
              SELECT artifact_id, COUNT(*) AS source_team_rows
              FROM source_rows GROUP BY artifact_id
            ), key_counts AS (
              SELECT
                s.artifact_id,
                COUNT(*) AS source_team_keys,
                COALESCE(SUM(CASE WHEN m.artifact_id IS NOT NULL THEN s.source_rows ELSE 0 END), 0) AS matched_source_team_rows,
                COALESCE(SUM(CASE WHEN m.artifact_id IS NULL THEN s.source_rows ELSE 0 END), 0) AS unmatched_source_team_rows,
                COUNT(*) FILTER (WHERE m.artifact_id IS NOT NULL) AS matched_source_team_keys,
                COUNT(*) FILTER (WHERE m.artifact_id IS NULL) AS unmatched_source_team_keys
              FROM source_team_keys s
              LEFT JOIN matched_team_keys m
                ON m.artifact_id=s.artifact_id
               AND m.db_name IS NOT DISTINCT FROM s.db_name
               AND m.year IS NOT DISTINCT FROM s.year
               AND m.week IS NOT DISTINCT FROM s.week
               AND m.team_key IS NOT DISTINCT FROM s.team_key
              GROUP BY s.artifact_id
            )
            SELECT r.artifact_id, r.source_team_rows,
                   COALESCE(k.source_team_keys, 0),
                   COALESCE(k.matched_source_team_rows, 0),
                   r.source_team_rows - COALESCE(k.matched_source_team_rows, 0),
                   COALESCE(k.matched_source_team_keys, 0),
                   COALESCE(k.unmatched_source_team_keys, 0)
            FROM raw_counts r
            LEFT JOIN key_counts k USING (artifact_id)
            ORDER BY r.artifact_id
        """).fetchall():
            (
                artifact_id, source_team_rows, source_team_keys,
                matched_source_team_rows, unmatched_source_team_rows,
                matched_source_team_keys, unmatched_source_team_keys,
            ) = row
            rows.append({
                "artifact_id": int(artifact_id),
                "source_team_rows": int(source_team_rows),
                "source_team_keys": int(source_team_keys),
                "matched_source_team_rows": int(matched_source_team_rows),
                "unmatched_source_team_rows": int(unmatched_source_team_rows),
                "matched_source_team_keys": int(matched_source_team_keys),
                "unmatched_source_team_keys": int(unmatched_source_team_keys),
            })
        if len(rows) != len(entries):
            raise SystemExit("profile did not account for every manifest artifact")
        return {
            "read_only": True,
            "cache_mutated": False,
            "new_lineage": False,
            "join_key": list(TEAM_KEY),
            "source_artifacts": len(entries),
            "source_rows": int(source_rows),
            "rows": rows,
        }
    finally:
        con.close()


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--base", type=Path, required=True)
    ap.add_argument("--manifest", type=Path, required=True)
    ap.add_argument("--out", type=Path, required=True)
    args = ap.parse_args()
    result = profile(base=args.base, manifest=args.manifest)
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    main()
