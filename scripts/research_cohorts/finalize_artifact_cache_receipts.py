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
SETTINGS_FIELDS = {
    "source_scoring_pass_td": "scoring_pass_td",
    "source_playoff_teams": "playoff_teams",
    "source_roster_FLX": "roster_FLX",
    "source_roster_SUPER_FLEX": "roster_SUPER_FLEX",
    "source_roster_IDP": "roster_IDP",
    "source_sleeper_best_ball": "sleeper_best_ball",
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


def _final_status(row: dict[str, Any], counts: Counter) -> tuple[str, str]:
    source_cells = int(counts["source_cells"])
    if source_cells == 0:
        if int(row.get("candidate_delta_rows", 0) or 0) or int(row.get("settings_source_reference_rows", 0) or 0):
            return "candidate_provenance_not_found", "Ledger cites candidate evidence but no source-path record was found in supplied deltas."
        return "not_data_bearing", "No candidate data cells were attributed to this artifact."
    if counts["blocked_schema_cells"]:
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
    settings_delta: Path | None, out_path: Path,
) -> dict[str, Any]:
    ledger = json.loads(ledger_path.read_text(encoding="utf-8"))
    ledger_rows = ledger.get("rows", ledger.get("files", []))
    if not ledger_rows:
        raise SystemExit("ledger has no rows")
    con = duckdb.connect(str(base_path), read_only=True)
    con.create_function("source_artifact_id", _source_artifact_id, return_type="BIGINT", null_handling="special")
    totals: dict[int, Counter] = defaultdict(Counter)
    _audit_delta(con, path=team_delta, view_name="team", source_fields=TEAM_FIELDS,
                 target_relation="public.player_fantasy", join_columns=PLAYER_KEY, totals=totals)
    _audit_delta(con, path=exact_delta, view_name="exact", source_fields=EXACT_FIELDS,
                 target_relation="public.player_fantasy", join_columns=PLAYER_KEY, totals=totals)
    _audit_delta(con, path=structured_delta, view_name="structured", source_fields=STRUCTURED_FIELDS,
                 target_relation="public.player_fantasy", join_columns=["db_name", "year", "week", "manager"], totals=totals)
    _audit_delta(con, path=settings_delta, view_name="settings", source_fields=SETTINGS_FIELDS,
                 target_relation="public.league_settings", join_columns=["db_name", "year"], totals=totals)
    con.close()

    rows: list[dict[str, Any]] = []
    for row in ledger_rows:
        result = dict(row)
        artifact_id = int(result["artifact_id"])
        counts = totals[artifact_id]
        status, reason = _final_status(result, counts)
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
    ap.add_argument("--out", type=Path, required=True)
    args = ap.parse_args()
    result = build_receipts(
        ledger_path=args.ledger, base_path=args.base, team_delta=args.team_delta,
        exact_delta=args.exact_delta, structured_delta=args.structured_delta,
        settings_delta=args.settings_delta, out_path=args.out,
    )
    print(json.dumps(result["summary"], sort_keys=True))


if __name__ == "__main__":
    main()
