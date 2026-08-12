"""Insert only source-proven, exact-schema missing player rows.

This mutates a restored *working copy* of the approved research cache in one
DuckDB transaction.  It accepts no update semantics: each candidate logical
key must be absent before the insert.  The caller is responsible for publishing
the already-validated working copy back under the same approved cache key.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import duckdb


KEY = ("db_name", "year", "week", "NFL_player_id", "manager")


def _q(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _columns(con: duckdb.DuckDBPyConnection, relation: str) -> list[str]:
    return [str(row[0]) for row in con.execute(f"DESCRIBE {relation}").fetchall()]


def _path(path: Path) -> str:
    return "'" + str(path.resolve()).replace("\\", "/").replace("'", "''") + "'"


def _key_join(left: str, right: str) -> str:
    return " AND ".join(
        f"{left}.{_q(column)} IS NOT DISTINCT FROM {right}.{_q(column)}"
        for column in KEY
    )


def apply(*, base: Path, candidates: Path, report: Path) -> dict[str, Any]:
    """Insert an exact-schema candidate only when every logical key is absent."""
    con = duckdb.connect(str(base))
    try:
        before_schema = _columns(con, "public.player_fantasy")
        missing = sorted(set(KEY) - set(before_schema))
        if missing:
            raise ValueError(f"canonical player schema missing key columns: {missing}")
        con.execute(f"CREATE OR REPLACE TEMP VIEW candidate_rows AS SELECT * FROM read_parquet({_path(candidates)})")
        candidate_schema = _columns(con, "candidate_rows")
        if candidate_schema != before_schema:
            raise ValueError(
                "candidate schema differs from the canonical schema: "
                f"candidate={candidate_schema} canonical={before_schema}"
            )
        candidate_count = int(con.execute("SELECT COUNT(*) FROM candidate_rows").fetchone()[0])
        if candidate_count < 1:
            raise ValueError("candidate has no rows")
        null_keys = int(con.execute(
            "SELECT COUNT(*) FROM candidate_rows WHERE "
            + " OR ".join(f"{_q(column)} IS NULL" for column in KEY)
        ).fetchone()[0])
        if null_keys:
            raise ValueError(f"candidate has {null_keys} rows with null logical keys")
        duplicate_candidates = int(con.execute(
            "SELECT COUNT(*) FROM (SELECT "
            + ", ".join(_q(column) for column in KEY)
            + " FROM candidate_rows GROUP BY ALL HAVING COUNT(*) > 1)"
        ).fetchone()[0])
        if duplicate_candidates:
            raise ValueError(f"candidate has {duplicate_candidates} duplicate logical keys")
        preexisting = int(con.execute(
            "SELECT COUNT(*) FROM candidate_rows c JOIN public.player_fantasy p ON "
            + _key_join("c", "p")
        ).fetchone()[0])
        if preexisting:
            raise ValueError(
                f"{preexisting} candidate logical keys already exist in the canonical cache; row additions may not update"
            )

        before_rows = int(con.execute("SELECT COUNT(*) FROM public.player_fantasy").fetchone()[0])
        fields = ", ".join(_q(column) for column in before_schema)
        con.execute("BEGIN")
        try:
            con.execute(f"INSERT INTO public.player_fantasy ({fields}) SELECT {fields} FROM candidate_rows")
            after_schema = _columns(con, "public.player_fantasy")
            after_rows = int(con.execute("SELECT COUNT(*) FROM public.player_fantasy").fetchone()[0])
            if after_schema != before_schema:
                raise ValueError("canonical schema changed during player-row addition")
            if after_rows != before_rows + candidate_count:
                raise ValueError(
                    "unexpected player row count after addition: "
                    f"before={before_rows} candidates={candidate_count} after={after_rows}"
                )
            readback_missing = int(con.execute(
                "SELECT COUNT(*) FROM candidate_rows c WHERE NOT EXISTS ("
                "SELECT 1 FROM public.player_fantasy p WHERE " + _key_join("c", "p") + ")"
            ).fetchone()[0])
            mismatch_terms = [
                f"p.{_q(column)} IS DISTINCT FROM c.{_q(column)}"
                for column in before_schema
            ]
            readback_mismatches = int(con.execute(
                "SELECT COUNT(*) FROM candidate_rows c JOIN public.player_fantasy p ON "
                + _key_join("c", "p") + " WHERE " + " OR ".join(mismatch_terms)
            ).fetchone()[0])
            if readback_missing or readback_mismatches:
                raise ValueError(
                    f"candidate readback failed: missing={readback_missing}, mismatches={readback_mismatches}"
                )
            con.execute("COMMIT")
        except Exception:
            con.execute("ROLLBACK")
            raise

        result = {
            "in_place": True,
            "cache_mutated": True,
            "new_lineage": False,
            "schema_unchanged": True,
            "candidate_rows": candidate_count,
            "inserted_rows": candidate_count,
            "preexisting_candidate_keys": 0,
            "readback_missing_keys": 0,
            "readback_mismatched_rows": 0,
            "before_player_rows": before_rows,
            "after_player_rows": after_rows,
            "logical_key": list(KEY),
            "schema": before_schema,
        }
        report.parent.mkdir(parents=True, exist_ok=True)
        report.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        return result
    finally:
        con.close()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base", type=Path, required=True)
    parser.add_argument("--candidates", type=Path, required=True)
    parser.add_argument("--report", type=Path, required=True)
    args = parser.parse_args()
    print(json.dumps(apply(base=args.base, candidates=args.candidates, report=args.report), sort_keys=True))


if __name__ == "__main__":
    main()
