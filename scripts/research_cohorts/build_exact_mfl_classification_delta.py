"""Build a read-only exact-player candidate from MFL outcome classifications.

Unlike a team-week signal, an MFL classification row carries the protected NFL
player ID and the source manager that rostered that player.  The canonical
recipient is therefore the exact player-week key, not a cache manager-name
match.  This builder never mutates the cache; it only emits unique, internally
consistent source facts for a later guarded promotion/readback transaction.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import duckdb


KEY = ("db_name", "year", "week", "NFL_player_id")
SOURCE_FIELDS = {
    "source_win": "win",
    "source_loss": "loss",
    "source_tie": "tie",
    "source_team_points": "team_points",
    "source_is_playoffs": "is_playoffs",
}
DIAGNOSTIC_CANONICAL_FIELDS = (
    "manager", "team_key", "team_name", "mfl_player_id", "fantasy_position",
    "is_rostered", "is_started", "fantasy_points",
)


def _literal(path: Path) -> str:
    return "'" + str(path.resolve()).replace("'", "''") + "'"


def _quoted(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _write_json(path: Path, value: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def build(
    *, base: Path, manifest: Path, out: Path, report: Path,
    ambiguous_out: Path | None = None,
) -> dict:
    entries = json.loads(manifest.read_text(encoding="utf-8"))
    if not isinstance(entries, list) or not entries:
        raise ValueError("manifest must be a non-empty list")

    con = duckdb.connect(str(base), read_only=True)
    try:
        pcols = {row[0] for row in con.execute("DESCRIBE public.player_fantasy").fetchall()}
        required = set(KEY) | {"platform"} | set(SOURCE_FIELDS.values())
        missing = sorted(required - pcols)
        if missing:
            raise ValueError(f"canonical player schema missing: {missing}")

        selects: list[str] = []
        for entry in entries:
            artifact_id = int(entry["artifact_id"])
            path = Path(entry["path"])
            if not path.exists():
                raise FileNotFoundError(path)
            source_cols = {
                row[0]
                for row in con.execute(f"DESCRIBE SELECT * FROM read_parquet({_literal(path)})").fetchall()
            }
            source_required = set(KEY) | {"manager", "status"}
            absent = sorted(source_required - source_cols)
            if absent:
                raise ValueError(f"{artifact_id}: classification source missing {absent}")
            fields = [
                f"{artifact_id}::BIGINT AS artifact_id",
                "CAST(db_name AS VARCHAR) AS db_name",
                "TRY_CAST(year AS INTEGER) AS year",
                "TRY_CAST(week AS INTEGER) AS week",
                "NULLIF(TRIM(CAST(NFL_player_id AS VARCHAR)), '') AS NFL_player_id",
                "NULLIF(TRIM(CAST(manager AS VARCHAR)), '') AS source_manager",
            ]
            for source, target in SOURCE_FIELDS.items():
                if source in source_cols:
                    type_name = "DOUBLE" if target == "team_points" else "INTEGER"
                    fields.append(f"TRY_CAST({_quoted(source)} AS {type_name}) AS {_quoted(source)}")
                else:
                    type_name = "DOUBLE" if target == "team_points" else "INTEGER"
                    fields.append(f"NULL::{type_name} AS {_quoted(source)}")
            selects.append(
                "SELECT " + ", ".join(fields) + f" FROM read_parquet({_literal(path)}) "
                "WHERE LOWER(TRIM(CAST(status AS VARCHAR)))='confirmed_outcome'"
            )
        con.execute("CREATE OR REPLACE TEMP TABLE raw_source AS " + " UNION ALL ".join(selects))
        raw_rows = int(con.execute("SELECT COUNT(*) FROM raw_source").fetchone()[0])

        variant_terms: list[str] = []
        value_terms: list[str] = []
        for source in SOURCE_FIELDS:
            variant_terms.append(
                f"COUNT(DISTINCT {_quoted(source)}) FILTER (WHERE {_quoted(source)} IS NOT NULL) "
                f"AS {_quoted(source + '_variants')}"
            )
            value_terms.append(f"MAX({_quoted(source)}) AS {_quoted(source)}")
        con.execute(f"""
            CREATE OR REPLACE TEMP TABLE source_unique AS
            SELECT {', '.join(_quoted(key) for key in KEY)},
                   string_agg(DISTINCT CAST(artifact_id AS VARCHAR), '|' ORDER BY CAST(artifact_id AS VARCHAR))
                     AS source_artifact_ids,
                   COUNT(DISTINCT source_manager) FILTER (WHERE source_manager IS NOT NULL)
                     AS source_manager_variants,
                   MAX(source_manager) AS source_manager,
                   {', '.join(variant_terms)},
                   {', '.join(value_terms)}
            FROM raw_source
            WHERE NFL_player_id IS NOT NULL
            GROUP BY {', '.join(_quoted(key) for key in KEY)}
        """)
        source_keys = int(con.execute("SELECT COUNT(*) FROM source_unique").fetchone()[0])
        conflict_predicate = " OR ".join(
            ["source_manager_variants > 1", *[f"{_quoted(source + '_variants')} > 1" for source in SOURCE_FIELDS]]
        )
        source_conflicting_keys = int(
            con.execute(f"SELECT COUNT(*) FROM source_unique WHERE {conflict_predicate}").fetchone()[0]
        )
        evidence_predicate = " OR ".join(f"{_quoted(source)} IS NOT NULL" for source in SOURCE_FIELDS)

        key_join = " AND ".join(
            [
                "CAST(p.db_name AS VARCHAR)=s.db_name",
                "TRY_CAST(p.year AS INTEGER)=s.year",
                "TRY_CAST(p.week AS INTEGER)=s.week",
                "CAST(p.NFL_player_id AS VARCHAR)=s.NFL_player_id",
            ]
        )
        diagnostic_select = [
            (
                f"p.{_quoted(name)} AS {_quoted('canonical_' + name)}"
                if name in pcols else
                f"NULL::VARCHAR AS {_quoted('canonical_' + name)}"
            )
            for name in DIAGNOSTIC_CANONICAL_FIELDS
        ]
        con.execute(f"""
            CREATE OR REPLACE TEMP TABLE exact_matches AS
            SELECT s.*, p.rowid AS player_rowid,
                   p.platform AS canonical_platform,
                   {', '.join(f'p.{_quoted(target)} AS {_quoted("canonical_" + target)}' for target in SOURCE_FIELDS.values())}
                   {',' if diagnostic_select else ''} {', '.join(diagnostic_select)}
            FROM source_unique s JOIN public.player_fantasy p ON {key_join}
            WHERE LOWER(TRIM(COALESCE(CAST(p.platform AS VARCHAR), '')))='mfl'
        """)
        con.execute("""
            CREATE OR REPLACE TEMP TABLE recipient_cardinality AS
            SELECT db_name, year, week, NFL_player_id, COUNT(*) AS recipient_count
            FROM exact_matches GROUP BY ALL
        """)
        con.execute("""
            CREATE OR REPLACE TEMP TABLE exact_single_matches AS
            SELECT m.*
            FROM exact_matches m JOIN recipient_cardinality c
              USING (db_name, year, week, NFL_player_id)
            WHERE c.recipient_count=1
        """)
        unmatched_source_keys = int(con.execute(f"""
            SELECT COUNT(*) FROM source_unique s
            WHERE ({evidence_predicate}) AND NOT EXISTS (
                SELECT 1 FROM exact_matches m
                WHERE m.db_name=s.db_name AND m.year=s.year AND m.week=s.week
                  AND m.NFL_player_id=s.NFL_player_id
            )
        """).fetchone()[0])
        ambiguous_recipient_keys = int(
            con.execute("SELECT COUNT(*) FROM recipient_cardinality WHERE recipient_count <> 1").fetchone()[0]
        )
        ambiguous_profile_row = con.execute("""
            SELECT
              COUNT(DISTINCT (m.db_name, m.year, m.week, m.NFL_player_id)),
              COUNT(*),
              COUNT(*) FILTER (WHERE m.canonical_manager IS NULL),
              COUNT(*) FILTER (WHERE m.canonical_mfl_player_id IS NULL),
              COUNT(*) FILTER (WHERE m.canonical_team_key IS NULL),
              COUNT(*) FILTER (WHERE m.canonical_team_name IS NULL),
              COUNT(*) FILTER (WHERE m.canonical_fantasy_position IS NULL)
            FROM exact_matches m JOIN recipient_cardinality c
              USING (db_name, year, week, NFL_player_id)
            WHERE c.recipient_count <> 1
        """).fetchone()
        recipient_count_distribution = {
            str(recipient_count): int(key_count)
            for recipient_count, key_count in con.execute("""
                SELECT recipient_count, COUNT(*)
                FROM recipient_cardinality
                WHERE recipient_count <> 1
                GROUP BY recipient_count ORDER BY recipient_count
            """).fetchall()
        }
        ambiguous_recipient_profile = {
            "keys": int(ambiguous_profile_row[0]),
            "recipient_rows": int(ambiguous_profile_row[1]),
            "recipient_count_distribution": recipient_count_distribution,
            "rows_without_canonical_manager": int(ambiguous_profile_row[2]),
            "rows_without_canonical_mfl_player_id": int(ambiguous_profile_row[3]),
            "rows_without_canonical_team_key": int(ambiguous_profile_row[4]),
            "rows_without_canonical_team_name": int(ambiguous_profile_row[5]),
            "rows_without_canonical_fantasy_position": int(ambiguous_profile_row[6]),
        }
        if ambiguous_out is not None:
            ambiguous_out.parent.mkdir(parents=True, exist_ok=True)
            con.execute(f"""
                COPY (
                    SELECT m.db_name, m.year, m.week, m.NFL_player_id,
                           m.player_rowid, c.recipient_count,
                           m.source_manager, m.canonical_platform AS platform,
                           {', '.join(_quoted('canonical_' + name) for name in DIAGNOSTIC_CANONICAL_FIELDS)},
                           {', '.join(_quoted('canonical_' + target) for target in SOURCE_FIELDS.values())},
                           {', '.join(_quoted(source) for source in SOURCE_FIELDS)}
                    FROM exact_matches m JOIN recipient_cardinality c
                      USING (db_name, year, week, NFL_player_id)
                    WHERE c.recipient_count <> 1
                ) TO {_literal(ambiguous_out)} (FORMAT PARQUET, COMPRESSION ZSTD)
            """)
        candidate_where = f"({evidence_predicate}) AND NOT ({conflict_predicate})"
        difference_predicate = " OR ".join(
            f"({_quoted(source)} IS NOT NULL AND {_quoted('canonical_' + target)} IS DISTINCT FROM {_quoted(source)})"
            for source, target in SOURCE_FIELDS.items()
        )
        out.parent.mkdir(parents=True, exist_ok=True)
        con.execute(f"""
            COPY (
                SELECT m.db_name, m.year, m.week, m.NFL_player_id,
                       m.source_manager, m.canonical_platform AS platform,
                       {', '.join(_quoted('canonical_' + target) for target in SOURCE_FIELDS.values())},
                       {', '.join(_quoted(source) for source in SOURCE_FIELDS)},
                       m.source_artifact_ids
                FROM exact_single_matches m
                WHERE {candidate_where} AND ({difference_predicate})
            ) TO {_literal(out)} (FORMAT PARQUET, COMPRESSION ZSTD)
        """)
        candidate_rows = int(con.execute(f"SELECT COUNT(*) FROM read_parquet({_literal(out)})").fetchone()[0])
        con.execute("""
            CREATE OR REPLACE TEMP TABLE artifact_source_keys AS
            SELECT DISTINCT artifact_id, db_name, year, week, NFL_player_id
            FROM raw_source WHERE NFL_player_id IS NOT NULL
        """)
        per_field = []
        for source, target in SOURCE_FIELDS.items():
            canonical = _quoted("canonical_" + target)
            source_q = _quoted(source)
            usable = f"(e.recipient_count=1 AND NOT ({conflict_predicate}))"
            per_field.extend([
                f"COUNT(*) FILTER (WHERE {usable} AND e.{source_q} IS NOT NULL AND e.{canonical} IS NOT DISTINCT FROM e.{source_q})",
                f"COUNT(*) FILTER (WHERE {usable} AND e.{source_q} IS NOT NULL AND e.{canonical} IS NULL)",
                f"COUNT(*) FILTER (WHERE {usable} AND e.{source_q} IS NOT NULL AND e.{canonical} IS NOT NULL AND e.{canonical} IS DISTINCT FROM e.{source_q})",
                f"COUNT(*) FILTER (WHERE (e.recipient_count IS NULL OR e.recipient_count<>1 OR ({conflict_predicate})) AND e.{source_q} IS NOT NULL)",
            ])
        con.execute("""
            CREATE OR REPLACE TEMP TABLE artifact_evaluation AS
            SELECT a.artifact_id, s.*, c.recipient_count,
                   m.canonical_win, m.canonical_loss, m.canonical_tie,
                   m.canonical_team_points, m.canonical_is_playoffs
            FROM artifact_source_keys a
            JOIN source_unique s USING (db_name, year, week, NFL_player_id)
            LEFT JOIN recipient_cardinality c USING (db_name, year, week, NFL_player_id)
            LEFT JOIN exact_single_matches m USING (db_name, year, week, NFL_player_id)
        """)
        source_rows_by_artifact = {
            int(artifact_id): int(rows)
            for artifact_id, rows in con.execute(
                "SELECT artifact_id, COUNT(*) FROM raw_source GROUP BY artifact_id"
            ).fetchall()
        }
        aggregate = con.execute(
            "SELECT artifact_id, " + ", ".join(per_field)
            + " FROM artifact_evaluation e GROUP BY artifact_id ORDER BY artifact_id"
        ).fetchall()
        artifact_current_state = []
        for values in aggregate:
            artifact_id, *counts = values
            equal = sum(int(counts[index]) for index in range(0, len(counts), 4))
            nulls = sum(int(counts[index]) for index in range(1, len(counts), 4))
            conflicts = sum(int(counts[index]) for index in range(2, len(counts), 4))
            ambiguous = sum(int(counts[index]) for index in range(3, len(counts), 4))
            artifact_current_state.append({
                "artifact_id": int(artifact_id),
                "source_rows": source_rows_by_artifact[int(artifact_id)],
                "source_cells": equal + nulls + conflicts + ambiguous,
                "cache_equal_cells": equal,
                "safe_null_candidates": nulls,
                "cache_conflict_cells": conflicts,
                "ambiguous_null_cells": ambiguous,
            })
        # A source row with no manager/franchise identity cannot choose among
        # duplicate cache rows.  Record this separately from ordinary
        # ambiguity: it is an explicit reason not to promote a value, not an
        # invitation to fan a single team outcome across every duplicate row.
        con.execute(f"""
            CREATE OR REPLACE TEMP TABLE unattributable_ambiguous_keys AS
            SELECT artifact_id, db_name, year, week, NFL_player_id,
                   source_win, source_loss, source_tie,
                   source_team_points, source_is_playoffs
            FROM artifact_evaluation
            WHERE recipient_count <> 1
              AND source_manager IS NULL
              AND ({evidence_predicate})
        """)
        identity_absent = " AND ".join(
            f"m.{_quoted('canonical_' + field)} IS NULL"
            for field in (
                "manager", "mfl_player_id", "team_key", "team_name", "fantasy_position",
            )
        )
        artifact_unattributable_state = []
        for values in con.execute(f"""
            WITH source_state AS (
              SELECT artifact_id,
                     COUNT(*) AS unattributable_ambiguous_keys,
                     SUM(
                       (source_win IS NOT NULL)::INTEGER
                       + (source_loss IS NOT NULL)::INTEGER
                       + (source_tie IS NOT NULL)::INTEGER
                       + (source_team_points IS NOT NULL)::INTEGER
                       + (source_is_playoffs IS NOT NULL)::INTEGER
                     ) AS unattributable_source_cells
              FROM unattributable_ambiguous_keys
              GROUP BY artifact_id
            ), recipient_state AS (
              SELECT u.artifact_id,
                     COUNT(*) AS canonical_recipient_rows,
                     COUNT(*) FILTER (WHERE {identity_absent}) AS identity_absent_recipient_rows
              FROM unattributable_ambiguous_keys u
              JOIN exact_matches m USING (db_name, year, week, NFL_player_id)
              GROUP BY u.artifact_id
            )
            SELECT s.artifact_id, s.unattributable_ambiguous_keys,
                   s.unattributable_source_cells, r.canonical_recipient_rows,
                   r.identity_absent_recipient_rows
            FROM source_state s JOIN recipient_state r USING (artifact_id)
            ORDER BY s.artifact_id
        """).fetchall():
            (
                artifact_id, keys, cells, recipient_rows, identity_absent_rows,
            ) = values
            artifact_unattributable_state.append({
                "artifact_id": int(artifact_id),
                "unattributable_ambiguous_keys": int(keys),
                "unattributable_source_cells": int(cells),
                "all_ambiguous_source_keys_lack_manager": True,
                "all_ambiguous_canonical_rows_lack_team_identity": (
                    int(recipient_rows) > 0
                    and int(recipient_rows) == int(identity_absent_rows)
                ),
            })
        result = {
            "read_only": True,
            "cache_mutated": False,
            "new_lineage": False,
            "canonical_schema_unchanged": True,
            "source_artifacts": len(entries),
            "source_rows_confirmed_outcome": raw_rows,
            "unique_exact_source_keys": source_keys,
            "source_conflicting_keys": source_conflicting_keys,
            "unmatched_source_keys": unmatched_source_keys,
            "ambiguous_recipient_keys": ambiguous_recipient_keys,
            "ambiguous_recipient_profile": ambiguous_recipient_profile,
            "candidate_rows": candidate_rows,
            "candidate_key": list(KEY),
            "artifact_current_state": artifact_current_state,
            "artifact_unattributable_state": artifact_unattributable_state,
            "policy": "direct MFL player outcome evidence; candidate only, no cache mutation",
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
    parser.add_argument("--ambiguous-out", type=Path)
    args = parser.parse_args()
    print(json.dumps(build(
        base=args.base, manifest=args.manifest, out=args.out, report=args.report,
        ambiguous_out=args.ambiguous_out,
    ), sort_keys=True))


if __name__ == "__main__":
    main()
