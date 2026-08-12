"""Add only the approved nullable loss/tie fields to the canonical player table.

This is a narrow, same-cache preparation step. It never creates a cache key,
does not touch the ops cache, and rejects any schema other than the current
canonical schema plus the two explicitly sanctioned fields.
"""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

import duckdb

try:
    from .canonical_player_schema import validate_player_schema
except ImportError:
    from canonical_player_schema import validate_player_schema


APPROVED_COLUMNS = ("loss", "tie")


def _hash(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _columns(con: duckdb.DuckDBPyConnection) -> list[str]:
    return [str(row[0]) for row in con.execute("DESCRIBE public.player_fantasy").fetchall()]


def add_columns(*, base: Path, ops: Path, report: Path) -> dict:
    """Add the two approved nullable fields, preserving every other invariant."""
    ops_before = _hash(ops)
    con = duckdb.connect(str(base))
    try:
        before = _columns(con)
        validate_player_schema(before)
        rows_before = int(con.execute("SELECT COUNT(*) FROM public.player_fantasy").fetchone()[0])
        added: list[str] = []
        con.execute("BEGIN")
        try:
            for column in APPROVED_COLUMNS:
                if column not in before:
                    con.execute(f'ALTER TABLE public.player_fantasy ADD COLUMN "{column}" INTEGER')
                    added.append(column)
            after = _columns(con)
            validate_player_schema(after)
            expected = before + added
            if after != expected:
                raise RuntimeError(
                    f"schema changed outside approved columns: expected {expected!r}, got {after!r}"
                )
            rows_after = int(con.execute("SELECT COUNT(*) FROM public.player_fantasy").fetchone()[0])
            if rows_after != rows_before:
                raise RuntimeError("player row count changed while adding loss/tie columns")
            con.execute("COMMIT")
        except Exception:
            con.execute("ROLLBACK")
            raise
    finally:
        con.close()
    ops_after = _hash(ops)
    result = {
        "cache_mutated": True,
        "new_lineage": False,
        "approved_columns": list(APPROVED_COLUMNS),
        "added_columns": added,
        "schema_before": before,
        "schema_after": after,
        "player_rows_before": rows_before,
        "player_rows_after": rows_after,
        "ops_unchanged": ops_before == ops_after,
    }
    if not result["ops_unchanged"]:
        raise RuntimeError("ops cache changed while adding loss/tie columns")
    report.parent.mkdir(parents=True, exist_ok=True)
    report.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return result


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base", type=Path, required=True)
    parser.add_argument("--ops", type=Path, required=True)
    parser.add_argument("--report", type=Path, required=True)
    args = parser.parse_args()
    print(json.dumps(add_columns(base=args.base, ops=args.ops, report=args.report), sort_keys=True))


if __name__ == "__main__":
    main()
