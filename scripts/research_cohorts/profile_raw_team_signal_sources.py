"""Batch-profile retained team/week artifacts against the frozen player cache.

This is diagnostic evidence for the artifact ledger.  It intentionally uses
the same exact team-week identity as the receipt reader:
``(db_name, year, week, canonical_team_key)``.  All raw sources are
materialized once, then joined to ``player_fantasy`` once; it must never scan
the canonical table once per source artifact.

MFL is deliberately special-cased: its raw ``team_key`` is a four-digit roster
slot, while the canonical player rows identify the same team with the stable
``manager_guid`` / franchise key.  Treating the slot as the cache key silently
loses every MFL team-week signal.
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
    """Return per-artifact canonical-team-week coverage without changing ``base``."""
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
        missing_source_columns = {"db_name", "year", "week"} - raw_columns
        if missing_source_columns:
            raise SystemExit(f"raw team source missing required key columns: {sorted(missing_source_columns)}")
        if not ({"team_key", "manager_guid", "franchise_id"} & raw_columns):
            raise SystemExit("raw team source has no usable team identity column")

        con.execute("CREATE OR REPLACE TEMP TABLE manifest_entries (artifact_id BIGINT, path VARCHAR)")
        con.executemany(
            "INSERT INTO manifest_entries VALUES (?, ?)",
            [(entry["artifact_id"], entry["path"]) for entry in entries],
        )
        # DuckDB normalizes paths differently on Windows and Linux.  Normalize
        # both sides before matching so the manifest remains the sole source of
        # artifact attribution.
        raw_team_key = (
            "NULLIF(TRIM(CAST(r.team_key AS VARCHAR)), '')"
            if "team_key" in raw_columns else "NULL::VARCHAR"
        )
        manager_guid = (
            "NULLIF(TRIM(CAST(r.manager_guid AS VARCHAR)), '')"
            if "manager_guid" in raw_columns else "NULL::VARCHAR"
        )
        franchise_id = (
            "NULLIF(TRIM(CAST(r.franchise_id AS VARCHAR)), '')"
            if "franchise_id" in raw_columns else "NULL::VARCHAR"
        )
        platform = (
            "LOWER(NULLIF(TRIM(CAST(r.platform AS VARCHAR)), ''))"
            if "platform" in raw_columns else "NULL::VARCHAR"
        )
        con.execute(f"""
            CREATE OR REPLACE TEMP TABLE source_rows AS
            SELECT
              m.artifact_id,
              CAST(r.db_name AS VARCHAR) AS db_name,
              CAST(r."year" AS INTEGER) AS year,
              CAST(r.week AS INTEGER) AS week,
              {platform} AS platform,
              {raw_team_key} AS raw_team_key,
              {manager_guid} AS manager_guid,
              {franchise_id} AS franchise_id,
              CASE
                WHEN {platform}='mfl' AND COALESCE({manager_guid}, {franchise_id}) IS NOT NULL
                  THEN COALESCE({manager_guid}, {franchise_id})
                ELSE {raw_team_key}
              END AS canonical_team_key,
              CASE
                WHEN {platform}='mfl' AND COALESCE({manager_guid}, {franchise_id}) IS NOT NULL
                  THEN 'mfl_manager_guid'
                WHEN {raw_team_key} IS NOT NULL THEN 'team_key'
                ELSE 'missing_identity'
              END AS identity_strategy
            FROM raw_sources r
            JOIN manifest_entries m
              ON REPLACE(CAST(r.filename AS VARCHAR), '\\', '/') = m.path
        """)
        source_rows = con.execute("SELECT COUNT(*) FROM source_rows").fetchone()[0]
        if not source_rows:
            raise SystemExit("source manifest produced zero attributable rows")
        con.execute("""
            CREATE OR REPLACE TEMP TABLE source_team_keys AS
            SELECT artifact_id, db_name, year, week, canonical_team_key, identity_strategy,
                   COUNT(*) AS source_rows
            FROM source_rows
            WHERE canonical_team_key IS NOT NULL
            GROUP BY 1,2,3,4,5,6
        """)
        # This is the sole canonical-table access: DuckDB builds from the small
        # source key relation and performs one exact team-week join.
        con.execute("""
            CREATE OR REPLACE TEMP TABLE matched_team_keys AS
            SELECT DISTINCT s.artifact_id, s.db_name, s.year, s.week,
                            s.canonical_team_key, s.identity_strategy
            FROM source_team_keys s
            JOIN public.player_fantasy p
              ON p.db_name IS NOT DISTINCT FROM s.db_name
             AND p."year" IS NOT DISTINCT FROM s.year
             AND p.week IS NOT DISTINCT FROM s.week
             AND p.team_key IS NOT DISTINCT FROM s.canonical_team_key
        """)
        con.execute("""
            CREATE OR REPLACE TEMP TABLE matched_player_fanout AS
            SELECT s.artifact_id, s.db_name, s.year, s.week,
                   s.canonical_team_key, s.identity_strategy,
                   COUNT(*) AS player_rows
            FROM source_team_keys s
            JOIN public.player_fantasy p
              ON p.db_name IS NOT DISTINCT FROM s.db_name
             AND p."year" IS NOT DISTINCT FROM s.year
             AND p.week IS NOT DISTINCT FROM s.week
             AND p.team_key IS NOT DISTINCT FROM s.canonical_team_key
            GROUP BY 1,2,3,4,5,6
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
                COUNT(*) FILTER (WHERE m.artifact_id IS NULL) AS unmatched_source_team_keys,
                COUNT(*) FILTER (WHERE m.identity_strategy='mfl_manager_guid' AND m.artifact_id IS NOT NULL) AS matched_by_mfl_manager_guid,
                COALESCE(SUM(f.player_rows), 0) AS matched_source_player_rows
              FROM source_team_keys s
              LEFT JOIN matched_team_keys m
                ON m.artifact_id=s.artifact_id
               AND m.db_name IS NOT DISTINCT FROM s.db_name
               AND m.year IS NOT DISTINCT FROM s.year
               AND m.week IS NOT DISTINCT FROM s.week
               AND m.canonical_team_key IS NOT DISTINCT FROM s.canonical_team_key
               AND m.identity_strategy IS NOT DISTINCT FROM s.identity_strategy
              LEFT JOIN matched_player_fanout f
                ON f.artifact_id=s.artifact_id
               AND f.db_name IS NOT DISTINCT FROM s.db_name
               AND f.year IS NOT DISTINCT FROM s.year
               AND f.week IS NOT DISTINCT FROM s.week
               AND f.canonical_team_key IS NOT DISTINCT FROM s.canonical_team_key
               AND f.identity_strategy IS NOT DISTINCT FROM s.identity_strategy
              GROUP BY s.artifact_id
            )
            SELECT r.artifact_id, r.source_team_rows,
                   COALESCE(k.source_team_keys, 0),
                   COALESCE(k.matched_source_team_rows, 0),
                   r.source_team_rows - COALESCE(k.matched_source_team_rows, 0),
                   COALESCE(k.matched_source_team_keys, 0),
                   COALESCE(k.unmatched_source_team_keys, 0),
                   COALESCE(k.matched_by_mfl_manager_guid, 0),
                   COALESCE(k.matched_source_player_rows, 0)
            FROM raw_counts r
            LEFT JOIN key_counts k USING (artifact_id)
            ORDER BY r.artifact_id
        """).fetchall():
            (
                artifact_id, source_team_rows, source_team_keys,
                matched_source_team_rows, unmatched_source_team_rows,
                matched_source_team_keys, unmatched_source_team_keys,
                matched_by_mfl_manager_guid, matched_source_player_rows,
            ) = row
            rows.append({
                "artifact_id": int(artifact_id),
                "source_team_rows": int(source_team_rows),
                "source_team_keys": int(source_team_keys),
                "matched_source_team_rows": int(matched_source_team_rows),
                "unmatched_source_team_rows": int(unmatched_source_team_rows),
                "matched_source_team_keys": int(matched_source_team_keys),
                "unmatched_source_team_keys": int(unmatched_source_team_keys),
                "matched_by_mfl_manager_guid": int(matched_by_mfl_manager_guid),
                "matched_source_player_rows": int(matched_source_player_rows),
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
