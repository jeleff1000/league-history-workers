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


def _literal(path: Path) -> str:
    return "'" + str(path.resolve()).replace("'", "''") + "'"


def _quoted(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _write_json(path: Path, value: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def build(*, base: Path, manifest: Path, out: Path, report: Path) -> dict:
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
        con.execute(f"""
            CREATE OR REPLACE TEMP TABLE exact_matches AS
            SELECT s.*, p.rowid AS player_rowid,
                   p.platform AS canonical_platform,
                   {', '.join(f'p.{_quoted(target)} AS {_quoted("canonical_" + target)}' for target in SOURCE_FIELDS.values())}
            FROM source_unique s JOIN public.player_fantasy p ON {key_join}
            WHERE LOWER(TRIM(COALESCE(CAST(p.platform AS VARCHAR), '')))='mfl'
        """)
        con.execute("""
            CREATE OR REPLACE TEMP TABLE recipient_cardinality AS
            SELECT db_name, year, week, NFL_player_id, COUNT(*) AS recipient_count
            FROM exact_matches GROUP BY ALL
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
        candidate_where = f"({evidence_predicate}) AND NOT ({conflict_predicate})"
        out.parent.mkdir(parents=True, exist_ok=True)
        con.execute(f"""
            COPY (
                SELECT m.db_name, m.year, m.week, m.NFL_player_id,
                       m.source_manager, m.canonical_platform AS platform,
                       {', '.join(_quoted('canonical_' + target) for target in SOURCE_FIELDS.values())},
                       {', '.join(_quoted(source) for source in SOURCE_FIELDS)},
                       m.source_artifact_ids
                FROM exact_matches m
                JOIN recipient_cardinality c USING (db_name, year, week, NFL_player_id)
                WHERE c.recipient_count=1 AND {candidate_where}
            ) TO {_literal(out)} (FORMAT PARQUET, COMPRESSION ZSTD)
        """)
        candidate_rows = int(con.execute(f"SELECT COUNT(*) FROM read_parquet({_literal(out)})").fetchone()[0])
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
            "candidate_rows": candidate_rows,
            "candidate_key": list(KEY),
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
    args = parser.parse_args()
    print(json.dumps(build(base=args.base, manifest=args.manifest, out=args.out, report=args.report), sort_keys=True))


if __name__ == "__main__":
    main()
