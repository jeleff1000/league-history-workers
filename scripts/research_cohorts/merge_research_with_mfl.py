"""Create an ephemeral, same-schema union of the research corpus and MFL cache.

The registered MFL cache is authoritative for the exact (db_name, year) pairs
recorded in its immutable index.  Source caches are attached read-only; this
script never saves, deletes, or modifies them.
"""
from __future__ import annotations

import argparse
import json
import time
from collections import Counter
from pathlib import Path

import duckdb


TABLES = ("league_settings", "matchup", "player_fantasy")


def qi(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def table_columns(
    con: duckdb.DuckDBPyConnection, database: str, schema: str, table: str
) -> list[tuple[str, str]]:
    return [(str(name), str(type_name)) for name, type_name, *_ in con.execute(
        f"DESCRIBE {database}.{qi(schema)}.{qi(table)}"
    ).fetchall()]


def public_tables(con: duckdb.DuckDBPyConnection, database: str) -> list[str]:
    return [
        str(row[0])
        for row in con.execute(
            "SELECT table_name FROM duckdb_tables() WHERE database_name=? AND schema_name='public'",
            [database],
        ).fetchall()
    ]


def table_schema(con: duckdb.DuckDBPyConnection, database: str, table: str) -> str | None:
    row = con.execute(
        "SELECT schema_name FROM duckdb_tables() "
        "WHERE database_name=? AND table_name=? "
        "ORDER BY CASE schema_name WHEN 'public' THEN 0 WHEN 'main' THEN 1 ELSE 2 END LIMIT 1",
        [database, table],
    ).fetchone()
    return str(row[0]) if row else None


def relation(database: str, schema: str, table: str) -> str:
    return f"{database}.{qi(schema)}.{qi(table)}"


def parse_ledger(raw: str) -> dict[int, int]:
    value = json.loads(raw)
    if not isinstance(value, dict):
        raise SystemExit("expected ledger must be a JSON object of year to league count")
    return {int(year): int(count) for year, count in value.items()}


def protected_mfl_pairs(index: dict, expected_ledger: dict[int, int]) -> list[tuple[str, int]]:
    accepted = {int(year): int(count) for year, count in index.get("accepted_by_year", {}).items()}
    if accepted != expected_ledger or int(index.get("league_count", -1)) != sum(expected_ledger.values()):
        raise SystemExit(
            "protected MFL league-year ledger mismatch: "
            f"expected={expected_ledger}, accepted={accepted}, league_count={index.get('league_count')}"
        )
    pairs: list[tuple[str, int]] = []
    for row in index.get("leagues", []):
        db_name = row.get("db_name")
        season = row.get("season")
        if not db_name or season is None:
            raise SystemExit(f"MFL index has incomplete league record: {row}")
        pairs.append((str(db_name), int(season)))
    if len(pairs) != len(set(pairs)):
        raise SystemExit("MFL index contains duplicate protected league-year keys")
    if len(pairs) != sum(expected_ledger.values()):
        raise SystemExit(
            f"MFL index pair count mismatch: expected={sum(expected_ledger.values())}, actual={len(pairs)}"
        )
    actual_ledger = Counter(year for _, year in pairs)
    if dict(sorted(actual_ledger.items())) != dict(sorted(expected_ledger.items())):
        raise SystemExit(
            f"MFL index pair-year ledger mismatch: expected={expected_ledger}, actual={dict(actual_ledger)}"
        )
    return pairs


def count_rows_for_pairs(con: duckdb.DuckDBPyConnection, relation: str) -> int:
    return int(con.execute(
        f"SELECT COUNT(*) FROM {relation} s JOIN _mfl_pairs p "
        "ON s.db_name=p.db_name AND CAST(s.year AS INTEGER)=p.year"
    ).fetchone()[0])


def canonical_source_projection(
    base_schema: list[tuple[str, str]], mfl_columns: set[str], *, db_name_expr: str | None = None
) -> str:
    """Project an MFL relation into the canonical base schema for comparison."""
    return ", ".join(
        (
            f"CAST({db_name_expr} AS {type_name}) AS {qi(name)}"
            if name == "db_name" and db_name_expr is not None
            else
            f"CAST(s.{qi(name)} AS {type_name}) AS {qi(name)}"
            if name in mfl_columns
            else f"CAST(NULL AS {type_name}) AS {qi(name)}"
        )
        for name, type_name in base_schema
    )


def normalize_mfl_league_id(value: object) -> str:
    """Normalize numeric MFL league keys without losing the zero identity."""
    raw = str(value).strip()
    if not raw:
        raise ValueError("MFL league identity is blank")
    return raw.lstrip("0") or "0"


def build_mfl_merge_pairs(
    con: duckdb.DuckDBPyConnection, *, pairs: list[tuple[str, int]], mfl_settings_schema: str
) -> None:
    """Map each recovered MFL db name to its canonical base db name if present.

    The immutable identity is `(season, normalized league_key)`, not the
    storage-specific db_name.  A base `smpl_mfl_*` row and recovered
    `mfl_<season>_<id>` row therefore represent one identity when their MFL
    league keys agree.
    """
    mfl_settings_columns = {name for name, _ in table_columns(con, "mfl", mfl_settings_schema, "league_settings")}
    base_settings_columns = {name for name, _ in table_columns(con, "base", "public", "league_settings")}
    expected = set(pairs)
    mfl_rows = con.execute(
        f"SELECT db_name, CAST(year AS INTEGER), "
        f"{qi('league_key') if 'league_key' in mfl_settings_columns else 'NULL'} "
        f"FROM {relation('mfl', mfl_settings_schema, 'league_settings')}"
    ).fetchall()
    mfl_identity: dict[tuple[str, int], str] = {}
    for db_name, year, league_key in mfl_rows:
        pair = (str(db_name), int(year))
        if pair not in expected:
            continue
        if league_key is not None:
            mfl_identity[pair] = normalize_mfl_league_id(league_key)
    # Older narrow synthetic fixtures can only prove direct db-name overlap;
    # production inputs carry league_key and therefore must resolve it.
    missing = expected - set(mfl_identity)
    for db_name, year in missing:
        mfl_identity[(db_name, year)] = f"__db_name__:{db_name}"

    base_by_identity: dict[tuple[int, str], list[str]] = {}
    if "league_key" in base_settings_columns:
        platform_expr = (
            f"LOWER(COALESCE(CAST({qi('platform')} AS VARCHAR), ''))"
            if "platform" in base_settings_columns
            else "''"
        )
        base_rows = con.execute(
            f"SELECT db_name, CAST(year AS INTEGER), {qi('league_key')}, {platform_expr} "
            "FROM base.public.league_settings"
        ).fetchall()
        for db_name, year, league_key, platform in base_rows:
            if league_key is None:
                continue
            # Restrict normalized matching to known MFL rows.  This prevents a
            # coincidental Yahoo/Sleeper league_key from being treated as MFL.
            if str(platform) != "mfl" and "mfl" not in str(db_name).lower():
                continue
            key = (int(year), normalize_mfl_league_id(league_key))
            base_by_identity.setdefault(key, []).append(str(db_name))

    base_direct = {
        (str(db_name), int(year)): str(db_name)
        for db_name, year in con.execute("SELECT db_name, CAST(year AS INTEGER) FROM base.public.league_settings").fetchall()
    }
    mapped: list[tuple[str, int, str, str | None, str]] = []
    for db_name, year in sorted(expected):
        identity = mfl_identity[(db_name, year)]
        candidates = base_by_identity.get((year, identity), [])
        if not candidates and (db_name, year) in base_direct:
            candidates = [base_direct[(db_name, year)]]
        if len(candidates) > 1:
            raise SystemExit(
                f"base corpus has conflicting duplicate normalized MFL identity: {(year, identity)} -> {candidates}"
            )
        base_db_name = candidates[0] if candidates else None
        mapped.append((db_name, year, base_db_name or db_name, base_db_name, identity))
    con.execute(
        "CREATE OR REPLACE TEMP TABLE _mfl_merge_pairs "
        "(mfl_db_name VARCHAR, year INTEGER, canonical_db_name VARCHAR, base_db_name VARCHAR, normalized_league_id VARCHAR)"
    )
    con.executemany("INSERT INTO _mfl_merge_pairs VALUES (?, ?, ?, ?, ?)", mapped)


def validate_no_conflicting_overlap(
    *, base: Path, mfl: Path, pairs: list[tuple[str, int]]
) -> None:
    """Fail before candidate creation when sources disagree on an overlap.

    The existing corpus is canonical.  A same league-year from the recovered
    MFL source may be omitted only when its canonical-column multiset is
    identical; a difference is never silently chosen or overwritten.
    """
    con = duckdb.connect(":memory:")
    try:
        base_path = str(base.resolve()).replace("'", "''")
        mfl_path = str(mfl.resolve()).replace("'", "''")
        con.execute(f"ATTACH '{base_path}' AS base (READ_ONLY)")
        con.execute(f"ATTACH '{mfl_path}' AS mfl (READ_ONLY)")
        base_tables = public_tables(con, "base")
        missing = [table for table in TABLES if table not in base_tables]
        if missing:
            raise SystemExit(f"base corpus missing required tables: {missing}")
        mfl_schemas = {table: table_schema(con, "mfl", table) for table in TABLES}
        missing = [table for table, schema in mfl_schemas.items() if schema is None]
        if missing:
            raise SystemExit(f"MFL cache missing required tables: {missing}")

        con.execute("CREATE TEMP TABLE _mfl_pairs (db_name VARCHAR, year INTEGER)")
        con.executemany("INSERT INTO _mfl_pairs VALUES (?, ?)", pairs)
        build_mfl_merge_pairs(
            con,
            pairs=pairs,
            mfl_settings_schema=mfl_schemas["league_settings"],
        )
        for ordinal, table in enumerate(TABLES):
            base_schema = table_columns(con, "base", "public", table)
            columns = ", ".join(qi(name) for name, _ in base_schema)
            base_columns = ", ".join(f"b.{qi(name)} AS {qi(name)}" for name, _ in base_schema)
            mfl_schema_name = mfl_schemas[table]
            assert mfl_schema_name is not None
            mfl_relation = relation("mfl", mfl_schema_name, table)
            mfl_columns = {name for name, _ in table_columns(con, "mfl", mfl_schema_name, table)}
            missing_keys = {"db_name", "year"} - mfl_columns
            if missing_keys:
                raise SystemExit(f"MFL {table} is missing required merge keys: {sorted(missing_keys)}")
            projection = canonical_source_projection(base_schema, mfl_columns)
            suffix = str(ordinal)
            con.execute(
                f"CREATE TEMP TABLE _overlap_pairs_{suffix} AS "
                f"SELECT DISTINCT p.mfl_db_name, p.base_db_name, p.year "
                f"FROM {mfl_relation} s JOIN _mfl_merge_pairs p "
                "ON s.db_name=p.mfl_db_name AND CAST(s.year AS INTEGER)=p.year "
                f"JOIN base.public.{qi(table)} b "
                "ON b.db_name=p.base_db_name AND CAST(b.year AS INTEGER)=p.year "
                "WHERE p.base_db_name IS NOT NULL"
            )
            con.execute(
                f"CREATE TEMP TABLE _overlap_base_{suffix} AS "
                f"SELECT {base_columns} FROM base.public.{qi(table)} b "
                f"JOIN _overlap_pairs_{suffix} p "
                "ON b.db_name=p.base_db_name AND CAST(b.year AS INTEGER)=p.year"
            )
            overlap_projection = canonical_source_projection(
                base_schema, mfl_columns, db_name_expr="p.base_db_name"
            )
            con.execute(
                f"CREATE TEMP TABLE _overlap_mfl_{suffix} AS "
                f"SELECT {overlap_projection} FROM {mfl_relation} s "
                f"JOIN _overlap_pairs_{suffix} p "
                "ON s.db_name=p.mfl_db_name AND CAST(s.year AS INTEGER)=p.year"
            )
            difference_rows = int(
                con.execute(
                    f"SELECT COUNT(*) FROM ("
                    f"SELECT {columns} FROM _overlap_base_{suffix} "
                    f"EXCEPT ALL SELECT {columns} FROM _overlap_mfl_{suffix} "
                    "UNION ALL "
                    f"SELECT {columns} FROM _overlap_mfl_{suffix} "
                    f"EXCEPT ALL SELECT {columns} FROM _overlap_base_{suffix}"
                    ")"
                ).fetchone()[0]
            )
            if difference_rows:
                pair_count = int(con.execute(f"SELECT COUNT(*) FROM _overlap_pairs_{suffix}").fetchone()[0])
                raise SystemExit(
                    f"conflicting overlap payload in {table}: "
                    f"{difference_rows} differing canonical rows across {pair_count} league-years"
                )
    finally:
        con.close()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base", type=Path, required=True)
    parser.add_argument("--mfl", type=Path, required=True)
    parser.add_argument("--mfl-index", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--expected-ledger", required=True)
    args = parser.parse_args()

    proof_out = args.out.with_suffix(".json")
    if args.out.exists() or proof_out.exists():
        existing = args.out if args.out.exists() else proof_out
        raise SystemExit(f"candidate output already exists; refusing to replace evidence: {existing}")
    expected_ledger = parse_ledger(args.expected_ledger)
    index = json.loads(args.mfl_index.read_text(encoding="utf-8"))
    pairs = protected_mfl_pairs(index, expected_ledger)
    validate_no_conflicting_overlap(base=args.base, mfl=args.mfl, pairs=pairs)

    con = duckdb.connect(str(args.out))
    try:
        base_path = str(args.base.resolve()).replace("'", "''")
        mfl_path = str(args.mfl.resolve()).replace("'", "''")
        con.execute(f"ATTACH '{base_path}' AS base (READ_ONLY)")
        con.execute(f"ATTACH '{mfl_path}' AS mfl (READ_ONLY)")
        con.execute("CREATE SCHEMA public")
        base_tables = public_tables(con, "base")
        missing = [table for table in TABLES if table not in base_tables]
        if missing:
            raise SystemExit(f"base corpus missing required tables: {missing}")
        mfl_schemas = {table: table_schema(con, "mfl", table) for table in TABLES}
        missing = [table for table, schema in mfl_schemas.items() if schema is None]
        if missing:
            raise SystemExit(f"MFL cache missing required tables: {missing}")

        con.execute("CREATE TEMP TABLE _mfl_pairs (db_name VARCHAR, year INTEGER)")
        con.executemany("INSERT INTO _mfl_pairs VALUES (?, ?)", pairs)
        build_mfl_merge_pairs(
            con,
            pairs=pairs,
            mfl_settings_schema=mfl_schemas["league_settings"],
        )
        report: dict[str, object] = {
            "schema_unchanged": True,
            "new_lineage": False,
            "protected_mfl_ledger": dict(sorted(expected_ledger.items())),
            "protected_mfl_league_years": len(pairs),
            "tables": {},
        }

        for table in base_tables:
            table_started = time.monotonic()
            base_schema = table_columns(con, "base", "public", table)
            columns = ", ".join(qi(name) for name, _ in base_schema)
            if table not in TABLES:
                con.execute(f"CREATE TABLE public.{qi(table)} AS SELECT {columns} FROM base.public.{qi(table)}")
                continue
            mfl_schema_name = mfl_schemas[table]
            assert mfl_schema_name is not None
            mfl_relation = relation("mfl", mfl_schema_name, table)
            mfl_schema = table_columns(con, "mfl", mfl_schema_name, table)
            mfl_columns = {name for name, _ in mfl_schema}
            missing_keys = {"db_name", "year"} - mfl_columns
            if missing_keys:
                raise SystemExit(f"MFL {table} is missing required merge keys: {sorted(missing_keys)}")
            # The research corpus schema is canonical at this boundary.  MFL
            # has a wider settings contract, so project by canonical column
            # name and cast to the corpus type instead of requiring identical
            # physical schemas.  MFL-only fields remain in the protected MFL
            # cache and are intentionally not invented in the research lake.
            source_columns = canonical_source_projection(
                base_schema, mfl_columns, db_name_expr="p.canonical_db_name"
            )
            source_rows = int(con.execute(
                f"SELECT COUNT(*) FROM {mfl_relation} s JOIN _mfl_merge_pairs p "
                "ON s.db_name=p.mfl_db_name AND CAST(s.year AS INTEGER)=p.year"
            ).fetchone()[0])
            con.execute(
                "CREATE OR REPLACE TEMP TABLE _mfl_table_pairs AS "
                f"SELECT DISTINCT p.mfl_db_name, p.canonical_db_name, p.base_db_name, p.year "
                f"FROM {mfl_relation} s JOIN _mfl_merge_pairs p "
                "ON s.db_name=p.mfl_db_name AND CAST(s.year AS INTEGER)=p.year"
            )
            source_pair_count = int(con.execute("SELECT COUNT(*) FROM _mfl_table_pairs").fetchone()[0])
            print(
                f"[mfl-merge] phase=table_source table={table} "
                f"mfl_source_rows={source_rows} source_league_years={source_pair_count} "
                f"missing_league_years={len(pairs) - source_pair_count}",
                flush=True,
            )
            # A registered MFL league-year can legitimately lack one table
            # (notably league_settings).  That is not a deletion signal: only
            # source pairs physically present in this table supersede the base.
            base_preserved = int(con.execute(
                f"SELECT COUNT(*) FROM base.public.{qi(table)} b WHERE NOT EXISTS "
                "(SELECT 1 FROM _mfl_table_pairs p WHERE b.db_name=p.base_db_name "
                "AND CAST(b.year AS INTEGER)=p.year)"
            ).fetchone()[0])
            con.execute(
                f"CREATE TABLE public.{qi(table)} AS "
                f"SELECT {columns} FROM base.public.{qi(table)} b WHERE NOT EXISTS "
                "(SELECT 1 FROM _mfl_table_pairs p WHERE b.db_name=p.base_db_name "
                "AND CAST(b.year AS INTEGER)=p.year) "
                "UNION ALL "
                f"SELECT {source_columns} FROM {mfl_relation} s JOIN _mfl_table_pairs p "
                "ON s.db_name=p.mfl_db_name AND CAST(s.year AS INTEGER)=p.year"
            )
            output_source_rows = int(con.execute(
                f"SELECT COUNT(*) FROM public.{qi(table)} s JOIN _mfl_table_pairs p "
                "ON s.db_name=p.canonical_db_name AND CAST(s.year AS INTEGER)=p.year"
            ).fetchone()[0])
            output_preserved = int(con.execute(
                f"SELECT COUNT(*) FROM public.{qi(table)} b WHERE NOT EXISTS "
                "(SELECT 1 FROM _mfl_table_pairs p WHERE b.db_name=p.base_db_name "
                "AND CAST(b.year AS INTEGER)=p.year)"
            ).fetchone()[0])
            if output_source_rows != source_rows or output_preserved != base_preserved:
                raise SystemExit(
                    f"row preservation failed for {table}: source={source_rows}/{output_source_rows}, "
                    f"preserved={base_preserved}/{output_preserved}"
                )
            report["tables"][table] = {
                "mfl_source_rows": source_rows,
                "mfl_source_league_years": source_pair_count,
                "mfl_missing_league_years": len(pairs) - source_pair_count,
                "preserved_base_rows": base_preserved,
                "output_rows": int(con.execute(f"SELECT COUNT(*) FROM public.{qi(table)}").fetchone()[0]),
            }
            print(
                f"[mfl-merge] phase=table_complete table={table} "
                f"output_rows={report['tables'][table]['output_rows']} "
                f"seconds={time.monotonic() - table_started:.3f}",
                flush=True,
            )

        proof_out.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
        print(json.dumps(report, indent=2))
    finally:
        con.close()


if __name__ == "__main__":
    main()
