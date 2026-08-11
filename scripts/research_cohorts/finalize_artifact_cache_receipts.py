"""Append current canonical-cache readback receipts to the 9,479-artifact ledger.

This is intentionally read-only.  It neither applies candidates nor saves a
cache: it answers the narrower, auditable question of whether the one
canonical cache currently contains every non-null source value referenced by
each artifact's existing candidate delta.
"""
from __future__ import annotations

import argparse
import json
import re
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

import duckdb


PLAYER_KEY = [
    "db_name", "year", "week", "NFL_player_id", "platform", "manager",
    "team_key", "team_name",
]
TEAM_FIELDS = {
    "source_win": "win", "source_loss": "loss", "source_tie": "tie",
    "source_team_points": "team_points", "source_playoffs": "is_playoffs",
    "source_champion": "champion", "source_final_playoff_seed": "final_playoff_seed",
    "source_made_playoffs": "made_playoffs",
}
EXACT_FIELDS = {
    **TEAM_FIELDS,
    "source_has_po_signal": "has_po_signal",
    "source_clutch_equity": "clutch_equity",
}
STRUCTURED_FIELDS = {
    "source_win": "win", "source_loss": "loss", "source_tie": "tie",
    "source_team_points": "team_points", "source_is_playoffs": "is_playoffs",
    "source_champion": "champion", "source_final_playoff_seed": "final_playoff_seed",
    "source_made_playoffs": "made_playoffs", "source_clutch_equity": "clutch_equity",
}
CANONICAL_SNAPSHOT_FIELDS = {
    "win": "win",
    "team_points": "team_points",
    "is_playoffs": "is_playoffs",
    "champion": "champion",
    "has_po_signal": "has_po_signal",
    "final_playoff_seed": "final_playoff_seed",
    "made_playoffs": "made_playoffs",
    "clutch_equity": "clutch_equity",
}
SETTINGS_FIELDS = {
    "source_scoring_pass_td": "scoring_pass_td",
    "source_playoff_teams": "playoff_teams",
    "source_roster_FLX": "roster_FLX",
    "source_roster_SUPER_FLEX": "roster_SUPER_FLEX",
    "source_roster_IDP": "roster_IDP",
    "source_sleeper_best_ball": "sleeper_best_ball",
}
RAW_TEAM_SIGNAL_COLUMNS = {
    "source_win": "win",
    "source_loss": "loss",
    "source_tie": "tie",
    "source_team_points": "team_points",
    "source_playoffs": "is_playoffs",
    "source_champion": "champion",
    "source_final_playoff_seed": "final_playoff_seed",
    "source_made_playoffs": "made_playoffs",
}
RAW_TEAM_SIGNAL_FALLBACK_TYPES = {
    "source_win": "BIGINT",
    "source_loss": "BIGINT",
    "source_tie": "BIGINT",
    "source_team_points": "DOUBLE",
    "source_playoffs": "BOOLEAN",
    "source_champion": "BIGINT",
    "source_final_playoff_seed": "BIGINT",
    "source_made_playoffs": "BOOLEAN",
}
CANDIDATE_DISPOSITIONS = {
    "direct_player_candidate", "team_week_signal_candidate", "player_signal_candidate",
    "settings_candidate", "structured_evidence_candidate",
}


def _source_artifact_id(path: str | None) -> int | None:
    if not path:
        return None
    patterns = (
        r"settings-candidate-shard-\d+-\d+/(\d+)/[^/|]+$",
        r"/candidates/(\d+)/candidates/\d+__[^/|]+$",
        r"(?:^|/)candidates/(\d+)__[^/|]+$",
    )
    for pattern in patterns:
        match = re.search(pattern, str(path))
        if match:
            return int(match.group(1))
    return None


def _columns(con: duckdb.DuckDBPyConnection, relation: str) -> set[str]:
    return {row[0] for row in con.execute(f"DESCRIBE {relation}").fetchall()}


def _quoted(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _join(left: str, right: str, columns: list[str]) -> str:
    return " AND ".join(
        f"{left}.{_quoted(column)} IS NOT DISTINCT FROM {right}.{_quoted(column)}"
        for column in columns
    )


def _add_result(
    totals: dict[int, Counter], artifact_id: int | None, values: dict[str, int],
) -> None:
    if artifact_id is None:
        return
    counter = totals[artifact_id]
    for key, value in values.items():
        counter[key] += int(value or 0)


def _seed_prior_receipts(
    totals: dict[int, Counter], prior_receipts_path: Path | None,
) -> None:
    """Carry earlier evidence forward before adding a new source-family readback."""
    if prior_receipts_path is None or not prior_receipts_path.exists():
        return
    prior = json.loads(prior_receipts_path.read_text(encoding="utf-8"))
    for row in prior.get("rows", []):
        _add_result(totals, int(row["artifact_id"]), {
            key: int(row.get(key, 0) or 0)
            for key in (
                "source_cells", "cache_match_cells", "cache_missing_cells",
                "cache_conflict_cells", "unmatched_cache_cells", "blocked_schema_cells",
            )
        })


def _audit_delta(
    con: duckdb.DuckDBPyConnection,
    *,
    path: Path | None,
    view_name: str,
    source_fields: dict[str, str],
    target_relation: str,
    join_columns: list[str],
    totals: dict[int, Counter],
) -> None:
    if path is None or not path.exists() or path.stat().st_size == 0:
        return
    escaped = str(path.resolve()).replace("'", "''")
    con.execute(f"CREATE OR REPLACE TEMP VIEW {view_name}_raw AS SELECT * FROM read_parquet('{escaped}')")
    raw_columns = _columns(con, f"{view_name}_raw")
    if "source_files" not in raw_columns:
        return
    selected_fields = {source: target for source, target in source_fields.items() if source in raw_columns}
    if not selected_fields:
        return
    required_keys = set(join_columns)
    if not required_keys <= raw_columns:
        return
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE {view_name}_evidence AS
        SELECT CAST(source_artifact_id(CAST(u.path AS VARCHAR)) AS BIGINT) AS artifact_id,
               r.* EXCLUDE (source_files)
        FROM {view_name}_raw r,
             UNNEST(string_split(CAST(r.source_files AS VARCHAR), '|')) AS u(path)
        WHERE source_artifact_id(CAST(u.path AS VARCHAR)) IS NOT NULL
    """)
    evidence_columns = _columns(con, f"{view_name}_evidence")
    if not evidence_columns:
        return
    target_columns = _columns(con, target_relation)
    supported = {source: target for source, target in selected_fields.items() if target in target_columns}
    blocked = {source: target for source, target in selected_fields.items() if target not in target_columns}
    if blocked:
        source_list = ", ".join(
            f"SUM(CASE WHEN e.{_quoted(source)} IS NOT NULL THEN 1 ELSE 0 END) AS {_quoted('blocked__' + target)}"
            for source, target in blocked.items()
        )
        for row in con.execute(
            f"SELECT e.artifact_id, {source_list} FROM {view_name}_evidence e GROUP BY e.artifact_id"
        ).fetchall():
            artifact_id, *counts = row
            _add_result(totals, artifact_id, {
                "blocked_schema_cells": sum(int(count or 0) for count in counts),
                "source_cells": sum(int(count or 0) for count in counts),
            })
    if not supported:
        return
    select_terms: list[str] = []
    for source, target in supported.items():
        select_terms.extend([
            f"SUM(CASE WHEN e.{_quoted(source)} IS NOT NULL AND p.rowid IS NULL THEN 1 ELSE 0 END) AS {_quoted('unmatched__' + target)}",
            f"SUM(CASE WHEN e.{_quoted(source)} IS NOT NULL AND p.rowid IS NOT NULL AND p.{_quoted(target)} IS NULL THEN 1 ELSE 0 END) AS {_quoted('missing__' + target)}",
            f"SUM(CASE WHEN e.{_quoted(source)} IS NOT NULL AND p.rowid IS NOT NULL AND p.{_quoted(target)} IS NOT NULL AND p.{_quoted(target)} IS NOT DISTINCT FROM e.{_quoted(source)} THEN 1 ELSE 0 END) AS {_quoted('match__' + target)}",
            f"SUM(CASE WHEN e.{_quoted(source)} IS NOT NULL AND p.rowid IS NOT NULL AND p.{_quoted(target)} IS NOT NULL AND p.{_quoted(target)} IS DISTINCT FROM e.{_quoted(source)} THEN 1 ELSE 0 END) AS {_quoted('conflict__' + target)}",
        ])
    query = f"""
        SELECT e.artifact_id, {', '.join(select_terms)}
        FROM {view_name}_evidence e
        LEFT JOIN {target_relation} p ON {_join('p', 'e', join_columns)}
        GROUP BY e.artifact_id
    """
    field_names = [term.rsplit(' AS ', 1)[1].strip('"') for term in select_terms]
    for row in con.execute(query).fetchall():
        artifact_id, *counts = row
        summary = Counter()
        for field_name, count in zip(field_names, counts):
            value = int(count or 0)
            summary["source_cells"] += value
            if field_name.startswith("match__"):
                summary["cache_match_cells"] += value
            elif field_name.startswith("missing__"):
                summary["cache_missing_cells"] += value
            elif field_name.startswith("conflict__"):
                summary["cache_conflict_cells"] += value
            elif field_name.startswith("unmatched__"):
                summary["unmatched_cache_cells"] += value
        _add_result(totals, artifact_id, dict(summary))


def _audit_raw_team_signals(
    con: duckdb.DuckDBPyConnection,
    *,
    manifest_path: Path | None,
    target_relation: str,
    totals: dict[int, Counter],
) -> None:
    """Fan raw team-week artifacts out to all player rows on the same team-week."""
    if manifest_path is None or not manifest_path.exists():
        return
    entries = json.loads(manifest_path.read_text(encoding="utf-8"))
    if not isinstance(entries, list):
        raise SystemExit("team-signal manifest must be a JSON list")
    target_columns = _columns(con, target_relation)
    target_types = {
        row[0]: row[1] for row in con.execute(f"DESCRIBE {target_relation}").fetchall()
    }
    selects: list[str] = []
    # Rich matchup-rescue artifacts carry a team key.  Treat that as the
    # identity—not a manager-name fallback—so a duplicate manager name or a
    # multi-team owner cannot fan a signal into another fantasy team.
    required_keys = {"db_name", "year", "week"}
    for entry in entries:
        artifact_id = int(entry["artifact_id"])
        path = Path(entry["path"])
        if not path.exists() or path.stat().st_size == 0:
            continue
        escaped = str(path.resolve()).replace("'", "''")
        raw_columns = {
            row[0] for row in con.execute(
                f"DESCRIBE SELECT * FROM read_parquet('{escaped}')"
            ).fetchall()
        }
        if not required_keys <= raw_columns or not ({"team_key", "manager_guid", "franchise_id"} & raw_columns):
            continue
        raw_team_key = f"NULLIF(TRIM(CAST({_quoted('team_key')} AS VARCHAR)), '')" if "team_key" in raw_columns else "NULL::VARCHAR"
        manager_guid = f"NULLIF(TRIM(CAST({_quoted('manager_guid')} AS VARCHAR)), '')" if "manager_guid" in raw_columns else "NULL::VARCHAR"
        franchise_id = f"NULLIF(TRIM(CAST({_quoted('franchise_id')} AS VARCHAR)), '')" if "franchise_id" in raw_columns else "NULL::VARCHAR"
        platform = f"LOWER(NULLIF(TRIM(CAST({_quoted('platform')} AS VARCHAR)), ''))" if "platform" in raw_columns else "NULL::VARCHAR"
        manager_key = (
            f"NULLIF(REGEXP_REPLACE(TRIM(LOWER(CAST({_quoted('manager')} AS VARCHAR))), '\\s+', ' ', 'g'), '')"
            if "manager" in raw_columns else "NULL::VARCHAR"
        )
        terms = [
            f"{artifact_id}::BIGINT AS artifact_id",
            *(_quoted(key) for key in ["db_name", "year", "week"]),
             f"CASE WHEN {platform}='mfl' AND COALESCE({manager_guid}, {franchise_id}) IS NOT NULL "
             f"THEN COALESCE({manager_guid}, {franchise_id}) ELSE {raw_team_key} END AS canonical_team_key",
             f"{manager_key} AS manager_key",
        ]
        for source, raw_column in RAW_TEAM_SIGNAL_COLUMNS.items():
            target = TEAM_FIELDS[source]
            cast_type = target_types.get(target, RAW_TEAM_SIGNAL_FALLBACK_TYPES[source])
            if raw_column in raw_columns:
                terms.append(f"CAST({_quoted(raw_column)} AS {cast_type}) AS {_quoted(source)}")
            else:
                terms.append(f"NULL::{cast_type} AS {_quoted(source)}")
        selects.append(f"SELECT {', '.join(terms)} FROM read_parquet('{escaped}')")
    if not selects:
        return
    con.execute("CREATE OR REPLACE TEMP TABLE raw_team_signal_evidence AS " + " UNION ALL ".join(selects))
    evidence_columns = _columns(con, "raw_team_signal_evidence")
    selected_fields = {source: target for source, target in TEAM_FIELDS.items() if source in evidence_columns}
    supported = {source: target for source, target in selected_fields.items() if target in target_columns}
    blocked = {source: target for source, target in selected_fields.items() if target not in target_columns}
    if blocked:
        field_sql = ", ".join(
            f"SUM(CASE WHEN e.{_quoted(source)} IS NOT NULL THEN 1 ELSE 0 END)"
            for source in blocked
        )
        for row in con.execute(
            f"SELECT e.artifact_id, {field_sql} FROM raw_team_signal_evidence e GROUP BY e.artifact_id"
        ).fetchall():
            artifact_id, *counts = row
            value = sum(int(count or 0) for count in counts)
            _add_result(totals, artifact_id, {"blocked_schema_cells": value, "source_cells": value})
    if not supported:
        return
    # The old outer join let DuckDB choose the 269M-row player relation as the
    # hash-build side. Materialize source-matched player rows with an inner
    # join, then identify unmatched source keys from that small match relation.
    target_manager_match = (
        "LOWER(TRIM(CAST(p.manager AS VARCHAR))) IS NOT DISTINCT FROM e.manager_key"
        if "manager" in target_columns else "FALSE"
    )
    con.execute("""
        CREATE OR REPLACE TEMP TABLE raw_team_signal_manager_counts AS
        SELECT artifact_id, db_name, year, week, manager_key,
               COUNT(DISTINCT canonical_team_key) AS source_team_count
        FROM raw_team_signal_evidence
        WHERE manager_key IS NOT NULL
        GROUP BY 1,2,3,4,5
    """)
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE raw_team_signal_resolved_keys AS
        SELECT e.artifact_id, e.db_name, e.year, e.week, e.canonical_team_key,
               e.manager_key,
               CASE
                 WHEN EXISTS (
                   SELECT 1 FROM {target_relation} p
                   WHERE e.canonical_team_key IS NOT NULL
                     AND p.db_name IS NOT DISTINCT FROM e.db_name
                     AND p."year" IS NOT DISTINCT FROM e."year"
                     AND p.week IS NOT DISTINCT FROM e.week
                     AND p.team_key IS NOT DISTINCT FROM e.canonical_team_key
                 ) THEN 'team_key'
                 WHEN mc.source_team_count=1 AND EXISTS (
                   SELECT 1 FROM {target_relation} p
                   WHERE p.db_name IS NOT DISTINCT FROM e.db_name
                     AND p."year" IS NOT DISTINCT FROM e."year"
                     AND p.week IS NOT DISTINCT FROM e.week
                     AND {target_manager_match}
                 ) THEN 'unique_manager'
                 ELSE NULL
               END AS match_strategy
        FROM raw_team_signal_evidence e
        LEFT JOIN raw_team_signal_manager_counts mc
          ON mc.artifact_id=e.artifact_id
         AND mc.db_name IS NOT DISTINCT FROM e.db_name
         AND mc.year IS NOT DISTINCT FROM e.year
         AND mc.week IS NOT DISTINCT FROM e.week
         AND mc.manager_key IS NOT DISTINCT FROM e.manager_key
    """)
    con.execute("""
        CREATE OR REPLACE TEMP TABLE raw_team_signal_matched_keys AS
        SELECT DISTINCT artifact_id, db_name, year, week, canonical_team_key AS team_key
        FROM raw_team_signal_resolved_keys
        WHERE match_strategy IS NOT NULL
    """)
    player_fields = ", ".join(
        f"p.{_quoted(target)} AS {_quoted('canonical__' + target)}"
        for target in supported.values()
    )
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE raw_team_signal_player_matches AS
        SELECT e.*, {player_fields}
        FROM raw_team_signal_evidence e
        JOIN raw_team_signal_resolved_keys r
          ON r.artifact_id=e.artifact_id
         AND r.db_name IS NOT DISTINCT FROM e.db_name
         AND r.year IS NOT DISTINCT FROM e.year
         AND r.week IS NOT DISTINCT FROM e.week
         AND r.canonical_team_key IS NOT DISTINCT FROM e.canonical_team_key
        JOIN {target_relation} p
          ON p.db_name IS NOT DISTINCT FROM e.db_name
         AND p."year" IS NOT DISTINCT FROM e."year"
         AND p.week IS NOT DISTINCT FROM e.week
         AND (
           (r.match_strategy='team_key' AND p.team_key IS NOT DISTINCT FROM e.canonical_team_key)
           OR (r.match_strategy='unique_manager' AND {target_manager_match})
         )
        WHERE r.match_strategy IS NOT NULL
    """)
    match_terms: list[str] = []
    unmatched_terms: list[str] = []
    for source, target in supported.items():
        canonical = _quoted('canonical__' + target)
        match_terms.extend([
            f"SUM(CASE WHEN m.{_quoted(source)} IS NOT NULL AND m.{canonical} IS NULL THEN 1 ELSE 0 END) AS {_quoted('missing__' + target)}",
            f"SUM(CASE WHEN m.{_quoted(source)} IS NOT NULL AND m.{canonical} IS NOT NULL AND m.{canonical} IS NOT DISTINCT FROM m.{_quoted(source)} THEN 1 ELSE 0 END) AS {_quoted('match__' + target)}",
            f"SUM(CASE WHEN m.{_quoted(source)} IS NOT NULL AND m.{canonical} IS NOT NULL AND m.{canonical} IS DISTINCT FROM m.{_quoted(source)} THEN 1 ELSE 0 END) AS {_quoted('conflict__' + target)}",
        ])
        unmatched_terms.append(
            f"SUM(CASE WHEN e.{_quoted(source)} IS NOT NULL AND k.artifact_id IS NULL THEN 1 ELSE 0 END) AS {_quoted('unmatched__' + target)}"
        )
    match_names = [term.rsplit(" AS ", 1)[1].strip('"') for term in match_terms]
    unmatched_names = [term.rsplit(" AS ", 1)[1].strip('"') for term in unmatched_terms]
    for row in con.execute(
        f"SELECT m.artifact_id, {', '.join(match_terms)} "
        "FROM raw_team_signal_player_matches m GROUP BY m.artifact_id"
    ).fetchall():
        artifact_id, *counts = row
        summary = Counter()
        for field_name, count in zip(match_names, counts):
            value = int(count or 0)
            summary["source_cells"] += value
            if field_name.startswith("match__"):
                summary["cache_match_cells"] += value
            elif field_name.startswith("missing__"):
                summary["cache_missing_cells"] += value
            else:
                summary["cache_conflict_cells"] += value
        _add_result(totals, artifact_id, dict(summary))
    for row in con.execute(f"""
        SELECT e.artifact_id, {', '.join(unmatched_terms)}
        FROM raw_team_signal_evidence e
        LEFT JOIN raw_team_signal_matched_keys k
          ON k.artifact_id=e.artifact_id
         AND k.db_name IS NOT DISTINCT FROM e.db_name
         AND k."year" IS NOT DISTINCT FROM e."year"
         AND k.week IS NOT DISTINCT FROM e.week
         AND k.team_key IS NOT DISTINCT FROM e.canonical_team_key
        GROUP BY e.artifact_id
    """).fetchall():
        artifact_id, *counts = row
        summary = Counter()
        for field_name, count in zip(unmatched_names, counts):
            value = int(count or 0)
            summary["source_cells"] += value
            summary["unmatched_cache_cells"] += value
        _add_result(totals, artifact_id, dict(summary))


def _audit_raw_structured_player_signals(
    con: duckdb.DuckDBPyConnection,
    *,
    manifest_path: Path | None,
    target_relation: str,
    totals: dict[int, Counter],
) -> None:
    """Read player-indexed recovery artifacts on their exact player-week key.

    These files are outcome evidence, not roster snapshots.  They may repair
    fields on a row already present in ``player_fantasy`` but cannot justify
    inserting a player row that lacks roster/start/position provenance.
    """
    if manifest_path is None or not manifest_path.exists():
        return
    entries = json.loads(manifest_path.read_text(encoding="utf-8"))
    if not isinstance(entries, list):
        raise SystemExit("structured-player manifest must be a JSON list")
    target_columns = _columns(con, target_relation)
    target_types = {
        row[0]: row[1] for row in con.execute(f"DESCRIBE {target_relation}").fetchall()
    }
    required_keys = ["db_name", "year", "week", "NFL_player_id", "manager"]
    selects: list[str] = []
    for entry in entries:
        artifact_id = int(entry["artifact_id"])
        path = Path(entry["path"])
        if not path.exists() or path.stat().st_size == 0:
            continue
        escaped = str(path.resolve()).replace("'", "''")
        raw_columns = {
            row[0] for row in con.execute(
                f"DESCRIBE SELECT * FROM read_parquet('{escaped}')"
            ).fetchall()
        }
        if not set(required_keys) <= raw_columns:
            continue
        terms = [
            f"{artifact_id}::BIGINT AS artifact_id",
            *(_quoted(key) for key in required_keys),
        ]
        for source, target in STRUCTURED_FIELDS.items():
            cast_type = target_types.get(target, RAW_TEAM_SIGNAL_FALLBACK_TYPES.get(source, "VARCHAR"))
            if source in raw_columns:
                terms.append(f"CAST({_quoted(source)} AS {cast_type}) AS {_quoted(source)}")
            else:
                terms.append(f"NULL::{cast_type} AS {_quoted(source)}")
        selects.append(f"SELECT DISTINCT {', '.join(terms)} FROM read_parquet('{escaped}')")
    if not selects:
        return
    con.execute(
        "CREATE OR REPLACE TEMP TABLE raw_structured_player_evidence AS "
        + " UNION ALL ".join(selects)
    )
    evidence_columns = _columns(con, "raw_structured_player_evidence")
    selected_fields = {source: target for source, target in STRUCTURED_FIELDS.items() if source in evidence_columns}
    supported = {source: target for source, target in selected_fields.items() if target in target_columns}
    blocked = {source: target for source, target in selected_fields.items() if target not in target_columns}
    if blocked:
        field_sql = ", ".join(
            f"SUM(CASE WHEN e.{_quoted(source)} IS NOT NULL THEN 1 ELSE 0 END)"
            for source in blocked
        )
        for row in con.execute(
            f"SELECT e.artifact_id, {field_sql} FROM raw_structured_player_evidence e GROUP BY e.artifact_id"
        ).fetchall():
            artifact_id, *counts = row
            value = sum(int(count or 0) for count in counts)
            _add_result(totals, artifact_id, {"blocked_schema_cells": value, "source_cells": value})
    if not supported:
        return
    select_terms: list[str] = []
    for source, target in supported.items():
        select_terms.extend([
            f"SUM(CASE WHEN e.{_quoted(source)} IS NOT NULL AND p.rowid IS NULL THEN 1 ELSE 0 END) AS {_quoted('unmatched__' + target)}",
            f"SUM(CASE WHEN e.{_quoted(source)} IS NOT NULL AND p.rowid IS NOT NULL AND p.{_quoted(target)} IS NULL THEN 1 ELSE 0 END) AS {_quoted('missing__' + target)}",
            f"SUM(CASE WHEN e.{_quoted(source)} IS NOT NULL AND p.rowid IS NOT NULL AND p.{_quoted(target)} IS NOT NULL AND p.{_quoted(target)} IS NOT DISTINCT FROM e.{_quoted(source)} THEN 1 ELSE 0 END) AS {_quoted('match__' + target)}",
            f"SUM(CASE WHEN e.{_quoted(source)} IS NOT NULL AND p.rowid IS NOT NULL AND p.{_quoted(target)} IS NOT NULL AND p.{_quoted(target)} IS DISTINCT FROM e.{_quoted(source)} THEN 1 ELSE 0 END) AS {_quoted('conflict__' + target)}",
        ])
    query = f"""
        SELECT e.artifact_id, {', '.join(select_terms)}
        FROM raw_structured_player_evidence e
        LEFT JOIN {target_relation} p ON {_join('p', 'e', required_keys)}
        GROUP BY e.artifact_id
    """
    field_names = [term.rsplit(" AS ", 1)[1].strip('"') for term in select_terms]
    for row in con.execute(query).fetchall():
        artifact_id, *counts = row
        summary = Counter()
        for field_name, count in zip(field_names, counts):
            value = int(count or 0)
            summary["source_cells"] += value
            if field_name.startswith("match__"):
                summary["cache_match_cells"] += value
            elif field_name.startswith("missing__"):
                summary["cache_missing_cells"] += value
            elif field_name.startswith("conflict__"):
                summary["cache_conflict_cells"] += value
            elif field_name.startswith("unmatched__"):
                summary["unmatched_cache_cells"] += value
        _add_result(totals, artifact_id, dict(summary))


def _audit_canonical_player_snapshots(
    con: duckdb.DuckDBPyConnection,
    *,
    manifest_path: Path | None,
    target_relation: str,
    totals: dict[int, Counter],
) -> None:
    """Read full player snapshots through a unique canonical player-week key.

    These source snapshots deliberately omit team_key and team_name.  Their
    observed source grain is (db_name, year, week, NFL_player_id), which is
    valid only when it resolves to one cache player row after the optional
    manager/platform consistency guards.  Any source duplicate or zero/many
    cache match is counted as unmatched rather than fanned out.
    """
    if manifest_path is None or not manifest_path.exists():
        return
    entries = json.loads(manifest_path.read_text(encoding="utf-8"))
    if not isinstance(entries, list):
        raise SystemExit("canonical-snapshot manifest must be a JSON list")
    target_columns = _columns(con, target_relation)
    target_types = {row[0]: row[1] for row in con.execute(f"DESCRIBE {target_relation}").fetchall()}
    required_keys = ["db_name", "year", "week", "NFL_player_id"]
    selects: list[str] = []
    for entry in entries:
        artifact_id = int(entry["artifact_id"])
        path = Path(entry["path"])
        if not path.exists() or path.stat().st_size == 0:
            continue
        escaped = str(path.resolve()).replace("'", "''")
        raw_columns = {
            row[0] for row in con.execute(f"DESCRIBE SELECT * FROM read_parquet('{escaped}')").fetchall()
        }
        if not set(required_keys) <= raw_columns:
            raise SystemExit(f"{artifact_id}: canonical snapshot misses player-week key")
        terms = [f"{artifact_id}::BIGINT AS artifact_id", *(_quoted(key) for key in required_keys)]
        for guard in ("manager", "platform"):
            terms.append(_quoted(guard) if guard in raw_columns else f"NULL::VARCHAR AS {_quoted(guard)}")
        for source, target in CANONICAL_SNAPSHOT_FIELDS.items():
            cast_type = target_types.get(target, "VARCHAR")
            if source in raw_columns:
                terms.append(f"CAST({_quoted(source)} AS {cast_type}) AS {_quoted('source_' + source)}")
            else:
                terms.append(f"NULL::{cast_type} AS {_quoted('source_' + source)}")
        selects.append(f"SELECT {', '.join(terms)} FROM read_parquet('{escaped}')")
    if not selects:
        return
    con.execute(
        "CREATE OR REPLACE TEMP TABLE canonical_snapshot_evidence AS " + " UNION ALL ".join(selects)
    )
    source_key = ["artifact_id", *required_keys]
    key_partition = ", ".join(_quoted(key) for key in source_key)
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE canonical_snapshot_source AS
        SELECT *,
               COUNT(*) OVER (PARTITION BY {key_partition}) AS source_key_count,
               ROW_NUMBER() OVER () AS source_row_id
        FROM canonical_snapshot_evidence
    """)
    source_columns = _columns(con, "canonical_snapshot_source")
    supported = {
        source: target for source, target in CANONICAL_SNAPSHOT_FIELDS.items()
        if f"source_{source}" in source_columns and target in target_columns
    }
    if not supported:
        return
    canonical_key = " AND ".join(
        f"p.{_quoted(key)} IS NOT DISTINCT FROM s.{_quoted(key)}" for key in required_keys
    )
    guards = """
        AND (s.manager IS NULL OR p.manager IS NOT DISTINCT FROM s.manager)
        AND (s.platform IS NULL OR LOWER(CAST(p.platform AS VARCHAR)) IS NOT DISTINCT FROM LOWER(CAST(s.platform AS VARCHAR)))
    """
    # Materialize the compared canonical values while resolving the unique
    # player row.  Rejoining a 269M-row cache by rowid here is logically
    # redundant and turns a targeted receipt into a second full-cache scan.
    canonical_projection = ", ".join(
        f"MIN(p.{_quoted(target)}) AS {_quoted('canonical__' + target)}"
        for target in supported.values()
    )
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE canonical_snapshot_resolution AS
        SELECT s.source_row_id, COUNT(p.rowid) AS canonical_match_count,
               {canonical_projection}
        FROM canonical_snapshot_source s
        LEFT JOIN {target_relation} p ON {canonical_key} {guards}
        GROUP BY s.source_row_id
    """)
    player_fields = ", ".join(
        f"r.{_quoted('canonical__' + target)}" for target in supported.values()
    )
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE canonical_snapshot_matches AS
        SELECT s.*, r.canonical_match_count, {player_fields}
        FROM canonical_snapshot_source s
        JOIN canonical_snapshot_resolution r ON r.source_row_id=s.source_row_id
    """)
    select_terms: list[str] = []
    for source, target in supported.items():
        value = f"m.{_quoted('source_' + source)}"
        canonical = f"m.{_quoted('canonical__' + target)}"
        unresolved = "(m.source_key_count <> 1 OR m.canonical_match_count <> 1)"
        select_terms.extend([
            f"SUM(CASE WHEN {value} IS NOT NULL AND {unresolved} THEN 1 ELSE 0 END) AS {_quoted('unmatched__' + target)}",
            f"SUM(CASE WHEN {value} IS NOT NULL AND NOT {unresolved} AND {canonical} IS NULL THEN 1 ELSE 0 END) AS {_quoted('missing__' + target)}",
            f"SUM(CASE WHEN {value} IS NOT NULL AND NOT {unresolved} AND {canonical} IS NOT NULL AND {canonical} IS NOT DISTINCT FROM {value} THEN 1 ELSE 0 END) AS {_quoted('match__' + target)}",
            f"SUM(CASE WHEN {value} IS NOT NULL AND NOT {unresolved} AND {canonical} IS NOT NULL AND {canonical} IS DISTINCT FROM {value} THEN 1 ELSE 0 END) AS {_quoted('conflict__' + target)}",
        ])
    field_names = [term.rsplit(" AS ", 1)[1].strip('"') for term in select_terms]
    for row in con.execute(
        f"SELECT m.artifact_id, {', '.join(select_terms)} FROM canonical_snapshot_matches m GROUP BY m.artifact_id"
    ).fetchall():
        artifact_id, *counts = row
        summary = Counter()
        for field_name, count in zip(field_names, counts):
            value = int(count or 0)
            summary["source_cells"] += value
            if field_name.startswith("match__"):
                summary["cache_match_cells"] += value
            elif field_name.startswith("missing__"):
                summary["cache_missing_cells"] += value
            elif field_name.startswith("conflict__"):
                summary["cache_conflict_cells"] += value
            elif field_name.startswith("unmatched__"):
                summary["unmatched_cache_cells"] += value
        _add_result(totals, artifact_id, dict(summary))


def _final_status(row: dict[str, Any], counts: Counter) -> tuple[str, str]:
    source_cells = int(counts["source_cells"])
    if source_cells == 0:
        if int(row.get("candidate_delta_rows", 0) or 0) or int(row.get("settings_source_reference_rows", 0) or 0):
            return "candidate_provenance_not_found", "Ledger cites candidate evidence but no source-path record was found in supplied deltas."
        if CANDIDATE_DISPOSITIONS & set(row.get("dispositions", [])):
            return (
                "no_promotable_candidate_emitted",
                "The frozen extraction/comparison ledger emitted no promotable cache cell for this candidate-classified artifact.",
            )
        return "not_data_bearing", "No candidate data cells were attributed to this artifact."
    if counts["blocked_schema_cells"]:
        if counts["cache_missing_cells"]:
            return (
                "partial_schema_blocked_cache_updates",
                "Source has safe null-cell repairs, plus non-null fields absent from the current canonical schema.",
            )
        if counts["cache_conflict_cells"]:
            return (
                "partial_schema_blocked_conflicts",
                "Source has non-null fields absent from the current canonical schema and differs from existing cache values.",
            )
        if counts["unmatched_cache_cells"]:
            return (
                "partial_schema_blocked_unmatched",
                "Source has non-null fields absent from the current canonical schema and source cells with no current canonical row.",
            )
        return "blocked_schema", "Source has non-null fields absent from the current canonical schema."
    if counts["cache_conflict_cells"]:
        return "cache_conflict_preserved", "Canonical cache has a different non-null value; no overwrite is inferred."
    if counts["unmatched_cache_cells"]:
        return "unmatched_cache_key", "Source candidate key has no matching current canonical row."
    if counts["cache_missing_cells"]:
        return "still_missing_cache_cells", "Current canonical cache remains null for at least one non-null source candidate cell."
    return "cache_verified", "Every non-null candidate source cell equals the current canonical cache value."


def build_receipts(
    *, ledger_path: Path, base_path: Path, team_delta: Path | None,
    exact_delta: Path | None, structured_delta: Path | None,
    settings_delta: Path | None, out_path: Path, team_signal_manifest: Path | None = None,
    structured_player_manifest: Path | None = None,
    canonical_snapshot_manifest: Path | None = None,
    prior_receipts_path: Path | None = None,
) -> dict[str, Any]:
    ledger = json.loads(ledger_path.read_text(encoding="utf-8"))
    ledger_rows = ledger.get("rows", ledger.get("files", []))
    if not ledger_rows:
        raise SystemExit("ledger has no rows")
    canonical_snapshot_ids: set[int] = set()
    if canonical_snapshot_manifest is not None and canonical_snapshot_manifest.exists():
        canonical_snapshot_ids = {
            int(entry["artifact_id"])
            for entry in json.loads(canonical_snapshot_manifest.read_text(encoding="utf-8"))
        }
    con = duckdb.connect(str(base_path), read_only=True)
    con.create_function("source_artifact_id", _source_artifact_id, return_type="BIGINT", null_handling="special")
    totals: dict[int, Counter] = defaultdict(Counter)
    _seed_prior_receipts(totals, prior_receipts_path)
    _audit_delta(con, path=team_delta, view_name="team", source_fields=TEAM_FIELDS,
                 target_relation="public.player_fantasy", join_columns=PLAYER_KEY, totals=totals)
    _audit_delta(con, path=exact_delta, view_name="exact", source_fields=EXACT_FIELDS,
                 target_relation="public.player_fantasy", join_columns=PLAYER_KEY, totals=totals)
    _audit_delta(con, path=structured_delta, view_name="structured", source_fields=STRUCTURED_FIELDS,
                 target_relation="public.player_fantasy", join_columns=["db_name", "year", "week", "manager"], totals=totals)
    _audit_delta(con, path=settings_delta, view_name="settings", source_fields=SETTINGS_FIELDS,
                 target_relation="public.league_settings", join_columns=["db_name", "year"], totals=totals)
    _audit_raw_team_signals(
        con, manifest_path=team_signal_manifest, target_relation="public.player_fantasy", totals=totals,
    )
    _audit_raw_structured_player_signals(
        con, manifest_path=structured_player_manifest, target_relation="public.player_fantasy", totals=totals,
    )
    _audit_canonical_player_snapshots(
        con, manifest_path=canonical_snapshot_manifest, target_relation="public.player_fantasy", totals=totals,
    )
    con.close()

    rows: list[dict[str, Any]] = []
    for row in ledger_rows:
        result = dict(row)
        artifact_id = int(result["artifact_id"])
        counts = totals[artifact_id]
        status, reason = _final_status(result, counts)
        if artifact_id in canonical_snapshot_ids and counts["cache_missing_cells"]:
            status = "candidate_built_pending_canonical_promotion"
            reason = (
                "Canonical snapshot readback found strict uniquely matched null-cell "
                "candidates; no cache mutation was performed."
            )
        result.update({
            "source_cells": int(counts["source_cells"]),
            "cache_match_cells": int(counts["cache_match_cells"]),
            "cache_missing_cells": int(counts["cache_missing_cells"]),
            "cache_conflict_cells": int(counts["cache_conflict_cells"]),
            "unmatched_cache_cells": int(counts["unmatched_cache_cells"]),
            "blocked_schema_cells": int(counts["blocked_schema_cells"]),
            "final_status": status,
            "final_reason": reason,
        })
        rows.append(result)
    summary = {
        "ledger_rows": len(rows),
        "status_counts": dict(sorted(Counter(row["final_status"] for row in rows).items())),
        "source_cells": sum(row["source_cells"] for row in rows),
        "cache_match_cells": sum(row["cache_match_cells"] for row in rows),
        "cache_missing_cells": sum(row["cache_missing_cells"] for row in rows),
        "cache_conflict_cells": sum(row["cache_conflict_cells"] for row in rows),
        "unmatched_cache_cells": sum(row["unmatched_cache_cells"] for row in rows),
        "blocked_schema_cells": sum(row["blocked_schema_cells"] for row in rows),
        "read_only": True,
        "cache_mutated": False,
        "new_lineage": False,
    }
    result = {"summary": summary, "rows": rows}
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return result


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--ledger", type=Path, required=True)
    ap.add_argument("--base", type=Path, required=True)
    ap.add_argument("--team-delta", type=Path)
    ap.add_argument("--exact-delta", type=Path)
    ap.add_argument("--structured-delta", type=Path)
    ap.add_argument("--settings-delta", type=Path)
    ap.add_argument("--team-signal-manifest", type=Path)
    ap.add_argument("--structured-player-manifest", type=Path)
    ap.add_argument("--canonical-snapshot-manifest", type=Path)
    ap.add_argument("--prior-receipts", type=Path)
    ap.add_argument("--out", type=Path, required=True)
    args = ap.parse_args()
    result = build_receipts(
        ledger_path=args.ledger, base_path=args.base, team_delta=args.team_delta,
        exact_delta=args.exact_delta, structured_delta=args.structured_delta,
        settings_delta=args.settings_delta, out_path=args.out,
        team_signal_manifest=args.team_signal_manifest,
        structured_player_manifest=args.structured_player_manifest,
        canonical_snapshot_manifest=args.canonical_snapshot_manifest,
        prior_receipts_path=args.prior_receipts,
    )
    print(json.dumps(result["summary"], sort_keys=True))


if __name__ == "__main__":
    main()
