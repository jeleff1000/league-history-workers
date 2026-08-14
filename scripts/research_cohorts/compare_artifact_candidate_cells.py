"""Read-only comparison of normalized artifact cells to canonical recipients."""
from __future__ import annotations

import hashlib
from collections import Counter
from pathlib import Path
from typing import Any

import duckdb


REQUIRED_CANDIDATE_COLUMNS = {
    "artifact_id", "artifact_file", "file_instance", "source_grain", "db_name", "year", "week",
    "NFL_player_id", "team_key", "manager", "source_column", "target_column", "source_value_text",
}


def _quote(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _hash(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _join_sql(grain: str) -> str:
    common = "p.db_name=c.db_name AND CAST(p.year AS VARCHAR)=c.year AND CAST(p.week AS VARCHAR)=c.week"
    if grain == "player_week":
        return common + " AND CAST(p.NFL_player_id AS VARCHAR)=c.NFL_player_id"
    if grain == "team_week":
        return common + " AND CAST(p.team_key AS VARCHAR)=c.team_key"
    if grain == "team_week_manager":
        return common + " AND lower(trim(CAST(p.manager AS VARCHAR)))=lower(trim(c.manager))"
    if grain == "league_year_settings":
        return "p.db_name=c.db_name AND CAST(p.year AS VARCHAR)=c.year"
    raise ValueError(f"unsupported source grain: {grain}")


def compare_candidate_cells(base: Path, candidate: Path) -> dict[str, Any]:
    """Classify candidates without mutating the canonical database.

    The returned counts are cell-level evidence.  A later apply stage may
    promote only the ``safe_null_fills`` cohort after source-conflict policy
    and a fresh readback are both available.
    """
    before = _hash(base)
    con = duckdb.connect(str(base), read_only=True)
    try:
        player_schema = con.execute("DESCRIBE public.player_fantasy").fetchall()
        types = {row[0]: row[1] for row in player_schema}
        con.execute("CREATE TEMP TABLE canonical_columns(column_name VARCHAR)")
        con.executemany("INSERT INTO canonical_columns VALUES (?)", [(field,) for field in types])
        candidate_sql = str(candidate).replace("'", "''")
        con.execute(f"CREATE TEMP VIEW c0 AS SELECT *, row_number() OVER () AS candidate_id FROM read_parquet('{candidate_sql}')")
        ccols = {row[0] for row in con.execute("DESCRIBE c0").fetchall()}
        missing = sorted(REQUIRED_CANDIDATE_COLUMNS - ccols)
        if missing:
            raise ValueError(f"candidate schema missing: {missing}")

        con.execute("CREATE TEMP TABLE blocked(candidate_id BIGINT, reason VARCHAR)")
        con.execute("""
            INSERT INTO blocked
            SELECT candidate_id, 'target_column_missing_from_canonical_schema'
            FROM c0 WHERE target_column NOT IN (SELECT column_name FROM canonical_columns)
        """)
        con.execute("""
            INSERT INTO blocked
            SELECT candidate_id, 'team_signal_cannot_award_player_championship_credit'
            FROM c0 WHERE target_column='champion' AND source_grain <> 'player_week'
        """)
        con.execute("""
            INSERT INTO blocked
            SELECT c.candidate_id, 'championship_candidate_missing_direct_started_evidence'
            FROM c0 c
            WHERE c.target_column='champion' AND c.source_grain='player_week'
              AND NOT EXISTS (
                SELECT 1 FROM c0 s
                WHERE s.artifact_id=c.artifact_id AND s.artifact_file=c.artifact_file
                  AND s.file_instance=c.file_instance AND s.source_grain='player_week'
                  AND s.db_name=c.db_name AND s.year=c.year AND s.week=c.week
                  AND s.NFL_player_id=c.NFL_player_id AND s.target_column='is_started'
                  AND lower(trim(s.source_value_text)) IN ('1','true','t','yes')
              )
        """)
        con.execute("CREATE TEMP VIEW c AS SELECT * FROM c0 WHERE candidate_id NOT IN (SELECT candidate_id FROM blocked)")

        safe: Counter[str] = Counter()
        same: Counter[str] = Counter()
        conflicts: Counter[str] = Counter()
        unmatched = 0
        unsupported_grains = 0
        by_artifact: dict[str, dict[str, Any]] = {}
        for artifact_id, count in con.execute("SELECT artifact_id, COUNT(*) FROM c0 GROUP BY 1").fetchall():
            by_artifact[str(artifact_id)] = {"candidate_cells": int(count), "safe_null_fills": {}, "same_values": {}, "conflicts": {}, "unmatched_candidates": 0, "blocked": {}}
        for artifact_id, reason, count in con.execute("SELECT c0.artifact_id, b.reason, COUNT(*) FROM blocked b JOIN c0 USING(candidate_id) GROUP BY 1,2").fetchall():
            by_artifact[str(artifact_id)]["blocked"][str(reason)] = int(count)
        for grain in ("player_week", "team_week", "team_week_manager", "league_year_settings"):
            grain_count = con.execute("SELECT COUNT(*) FROM c WHERE source_grain=?", [grain]).fetchone()[0]
            if not grain_count:
                continue
            try:
                join = _join_sql(grain)
            except ValueError:
                unsupported_grains += grain_count
                continue
            for field, data_type in types.items():
                count = con.execute(
                    "SELECT COUNT(*) FROM c WHERE source_grain=? AND target_column=?", [grain, field]
                ).fetchone()[0]
                if not count:
                    continue
                cast = f"TRY_CAST(c.source_value_text AS {data_type})"
                relation = f"""
                    FROM c LEFT JOIN public.player_fantasy p ON {join}
                    WHERE c.source_grain='{grain}' AND c.target_column='{field}'
                """
                unmatched_count = con.execute(f"SELECT COUNT(*) {relation} AND p.db_name IS NULL").fetchone()[0]
                unmatched += unmatched_count
                safe_count = con.execute(
                    f"SELECT COUNT(*) {relation} AND p.db_name IS NOT NULL AND p.{_quote(field)} IS NULL AND {cast} IS NOT NULL"
                ).fetchone()[0]
                same_count = con.execute(
                    f"SELECT COUNT(*) {relation} AND p.db_name IS NOT NULL AND p.{_quote(field)} IS NOT NULL AND p.{_quote(field)} IS NOT DISTINCT FROM {cast}"
                ).fetchone()[0]
                conflict_count = con.execute(
                    f"SELECT COUNT(*) {relation} AND p.db_name IS NOT NULL AND p.{_quote(field)} IS NOT NULL AND {cast} IS NOT NULL AND p.{_quote(field)} IS DISTINCT FROM {cast}"
                ).fetchone()[0]
                if safe_count:
                    safe[field] += safe_count
                if same_count:
                    same[field] += same_count
                if conflict_count:
                    conflicts[field] += conflict_count
                for artifact_id, count in con.execute(
                    f"SELECT c.artifact_id, COUNT(*) {relation} AND p.db_name IS NULL GROUP BY 1"
                ).fetchall():
                    by_artifact[str(artifact_id)]["unmatched_candidates"] += int(count)
                for label, condition, bucket in (
                    ("safe_null_fills", f"p.db_name IS NOT NULL AND p.{_quote(field)} IS NULL AND {cast} IS NOT NULL", "safe_null_fills"),
                    ("same_values", f"p.db_name IS NOT NULL AND p.{_quote(field)} IS NOT NULL AND p.{_quote(field)} IS NOT DISTINCT FROM {cast}", "same_values"),
                    ("conflicts", f"p.db_name IS NOT NULL AND p.{_quote(field)} IS NOT NULL AND {cast} IS NOT NULL AND p.{_quote(field)} IS DISTINCT FROM {cast}", "conflicts"),
                ):
                    for artifact_id, count in con.execute(
                        f"SELECT c.artifact_id, COUNT(*) {relation} AND {condition} GROUP BY 1"
                    ).fetchall():
                        by_artifact[str(artifact_id)][bucket][field] = int(count)

        blocked_counts = dict(sorted(con.execute("SELECT reason, COUNT(*) FROM blocked GROUP BY 1 ORDER BY 1").fetchall()))
        result = {
            "candidate_cells": int(con.execute("SELECT COUNT(*) FROM c0").fetchone()[0]),
            "admissible_candidate_cells": int(con.execute("SELECT COUNT(*) FROM c").fetchone()[0]),
            "blocked_by_reason": blocked_counts,
            "safe_null_fills_by_field": dict(sorted(safe.items())),
            "same_value_by_field": dict(sorted(same.items())),
            "conflicts_by_field": dict(sorted(conflicts.items())),
            "unmatched_candidates": int(unmatched),
            "unsupported_grain_candidates": int(unsupported_grains),
            "by_artifact": by_artifact,
            "cache_sha256_before": before,
            "cache_mutated": False,
            "new_lineage": False,
        }
    finally:
        con.close()
    after = _hash(base)
    if after != before:
        raise RuntimeError("read-only candidate comparison mutated the canonical cache")
    result["cache_sha256_after"] = after
    return result
