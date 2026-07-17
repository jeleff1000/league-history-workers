"""build_corpus_snapshot.py -- fold the grind corpus (smpl_* league DuckDBs) into ONE local
DuckDB with the same four tables/columns the real-leagues snapshot carries, so the research
cohort builders can read (real UNION corpus) through LocalReader.

Resumable: a _sources table records every folded db_name; re-running only appends new leagues
(the grind keeps running -- leagues locked by an in-flight ingest are skipped and picked up on
the next run). Read-only ATTACH per league; no writes to any smpl db.

Folding is ~290x smaller than the raw ingest dir (32 MB -> 110 KB per league), which is what
makes the GH-Actions crawl shippable as an artifact. `open_snapshot()` / `fold_league()` are
the reusable halves -- batch_ingest_corpus.py --fold-into calls them to fold as it crawls.

    py -3 scripts/sleeper_corpus/build_corpus_snapshot.py
"""
from __future__ import annotations

import os
import time
from pathlib import Path

import duckdb

# env-overridable so this runs unchanged on a Linux GH runner (corpus_crawl_worker.yml)
CORPUS = Path(os.environ.get("CORPUS_DIR", "D:/league-history-data/fantasy_leagues/sampling_corpus"))
OUT = CORPUS / "corpus_snapshot.duckdb"

# Mirror snapshot_real_leagues.py column contracts (week included for the weekly builders).
# The eligibility gates (docs/runbooks/research-eligibility-gates-2026-07-16.md) all read from
# league_settings, so every gating field must be carried or the corpus silently can't be gated.
LS_COLS = [
    ("db_name", "VARCHAR"), ("year", "INTEGER"), ("num_teams", "INTEGER"),
    ("is_dynasty", "BOOLEAN"), ("max_keepers", "INTEGER"), ("sleeper_best_ball", "BOOLEAN"),
    ("roster_QB", "INTEGER"), ("roster_RB", "INTEGER"), ("roster_WR", "INTEGER"),
    ("roster_TE", "INTEGER"), ("roster_FLX", "INTEGER"), ("roster_SUPER_FLEX", "INTEGER"),
    ("roster_IDP", "INTEGER"), ("roster_DL", "INTEGER"), ("roster_LB", "INTEGER"),
    ("roster_DB", "INTEGER"), ("roster_DB_LB", "INTEGER"), ("roster_DL_LB", "INTEGER"),
    ("scoring_rec", "DOUBLE"), ("scoring_pass_td", "DOUBLE"),
    # --- Class B gate: playoff eligibility. NEVER hardcode week 15 -- it varies (15=74%,
    # 14=19.8%, 16=4.6%), so a fixed week misclassifies 26% of leagues.
    ("playoff_start_week", "INTEGER"), ("playoff_teams", "INTEGER"),
    ("regular_season_weeks", "INTEGER"), ("end_week", "INTEGER"),
    ("has_multiweek_championship", "BOOLEAN"),
    # --- Class A gate: position eligibility. NULL means "no such slot" (values are only 1/2),
    # so COALESCE(x,0)>0 is a true test -- without these a kicker's ceiling is capped by the
    # share of leagues carrying a K slot, which masquerades as a format effect.
    ("roster_K", "INTEGER"), ("roster_DEF", "INTEGER"),
    # --- roster depth: start_rate is 1/depth if you divide by rostered weeks; carried so the
    # depth confound can be measured rather than guessed at.
    ("roster_BN", "INTEGER"), ("roster_TAXI", "INTEGER"), ("roster_IR", "INTEGER"),
    ("platform", "VARCHAR"),
    # per-year Sleeper league id -- lets LocalReader exclude corpus league-years that are
    # ALSO real customer leagues in ___leagues (double-count guard)
    ("league_key", "VARCHAR"),
]
DRAFT_COLS = [
    ("db_name", "VARCHAR"), ("year", "INTEGER"), ("NFL_player_id", "VARCHAR"),
    ("pick", "INTEGER"), ("cost", "DOUBLE"), ("draft_value_zscore", "DOUBLE"),
    ("pick_quality_zscore", "DOUBLE"), ("manager_lamar", "DOUBLE"),
    ("total_fantasy_points", "DOUBLE"),
    # --- Class C gate: keeper eligibility. is_keeper is the only trustworthy signal
    # (100% populated on every platform); league_settings.max_keepers is a Yahoo capture gap.
    ("is_keeper", "BOOLEAN"), ("round", "INTEGER"), ("pick_in_round", "INTEGER"),
]
TXN_COLS = [
    ("db_name", "VARCHAR"), ("year", "INTEGER"), ("week", "INTEGER"),
    ("NFL_player_id", "VARCHAR"), ("transaction_type", "VARCHAR"), ("faab_bid", "DOUBLE"),
    ("transaction_score", "DOUBLE"), ("manager_lamar_ros_managed", "DOUBLE"),
    ("player_lamar_ros_total", "DOUBLE"),
]
PF_COLS = [
    ("db_name", "VARCHAR"), ("year", "INTEGER"), ("week", "INTEGER"),
    ("NFL_player_id", "VARCHAR"), ("is_started", "INTEGER"), ("is_rostered", "INTEGER"),
    ("fantasy_points", "DOUBLE"), ("win", "INTEGER"), ("champion", "INTEGER"),
    ("clutch_equity", "DOUBLE"), ("manager_lamar", "DOUBLE"),
]
TABLES = {
    "league_settings": (LS_COLS, ""),
    "draft": (DRAFT_COLS, "WHERE pick IS NOT NULL OR cost IS NOT NULL"),
    "transactions": (TXN_COLS, ""),
    "player_fantasy": (PF_COLS, "WHERE CAST(is_rostered AS INT) = 1"),
}


def league_dirs(corpus: Path = CORPUS) -> list[Path]:
    dirs = sorted(corpus.glob("leagues/smpl_*"))
    dirs += sorted(d for d in corpus.glob("smpl_*") if d.is_dir())  # stray root-level ingest dirs
    return [d for d in dirs if (d / f"{d.name}.duckdb").exists()]


def _select_sql(src_cols: set[str], cols: list[tuple[str, str]], table: str, where: str) -> str:
    parts = []
    for name, typ in cols:
        if name in src_cols:
            parts.append(f'CAST("{name}" AS {typ}) AS "{name}"')
        else:
            parts.append(f'CAST(NULL AS {typ}) AS "{name}"')
    return f'SELECT {", ".join(parts)} FROM src.public.{table} {where}'


def open_snapshot(path: Path | str = OUT) -> duckdb.DuckDBPyConnection:
    """Open (creating if needed) the central snapshot, with the schema + _sources ledger.

    Migrates in place: CREATE TABLE IF NOT EXISTS leaves an OLD schema alone, so adding a column
    to TABLES above would otherwise make every INSERT fail with a column-count mismatch against
    an existing snapshot. Add missing columns rather than demand a full re-fold.
    """
    con = duckdb.connect(str(path))
    con.execute("SET memory_limit='2000MB'")
    con.execute("CREATE SCHEMA IF NOT EXISTS public")
    for t, (cols, _) in TABLES.items():
        ddl = ", ".join(f'"{n}" {ty}' for n, ty in cols)
        con.execute(f"CREATE TABLE IF NOT EXISTS public.{t} ({ddl})")
        have = {r[0] for r in con.execute(f"DESCRIBE public.{t}").fetchall()}
        for name, typ in cols:
            if name not in have:
                con.execute(f'ALTER TABLE public.{t} ADD COLUMN "{name}" {typ}')
                print(f"[schema] public.{t}: added missing column {name} {typ}")
    con.execute("CREATE TABLE IF NOT EXISTS _sources (db_name VARCHAR PRIMARY KEY, folded_at TIMESTAMP)")
    return con


def folded_set(con: duckdb.DuckDBPyConnection) -> set[str]:
    return {r[0] for r in con.execute("SELECT db_name FROM _sources").fetchall()}


def evict_league(con: duckdb.DuckDBPyConnection, db_name: str) -> None:
    """Remove a league from the snapshot so fold_league() will take it again.

    The _sources ledger makes folding resumable, which works AGAINST us after a league's source
    db is updated in place (e.g. the sim/clutch backfill writes p_champ + clutch_equity into an
    already-folded league). Without evicting first, fold_league() silently skips it and the new
    columns never reach the lake. Caller serializes, same as fold_league().
    """
    con.execute("BEGIN")
    try:
        for t in TABLES:
            con.execute(f"DELETE FROM public.{t} WHERE db_name = ?", [db_name])
        con.execute("DELETE FROM _sources WHERE db_name = ?", [db_name])
        con.execute("COMMIT")
    except Exception:
        con.execute("ROLLBACK")
        raise


def fold_league(con: duckdb.DuckDBPyConnection, league_dir: Path) -> tuple[bool, str]:
    """Fold ONE smpl_* league into the open snapshot. Caller serializes (DuckDB single-writer).

    Returns (ok, message). Never raises; a locked/corrupt league is skipped for a later run.
    """
    db = league_dir / f"{league_dir.name}.duckdb"
    if not db.exists():
        return False, "no duckdb"
    try:
        con.execute(f"ATTACH '{db.as_posix()}' AS src (READ_ONLY)")
    except Exception as e:  # locked by an in-flight ingest, or corrupt
        return False, str(e).splitlines()[0][:120]
    try:
        src_tables = {r[0] for r in con.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_catalog='src' AND table_schema='public'").fetchall()}
        if not {"league_settings", "player_fantasy"} <= src_tables:
            return False, f"missing core tables ({sorted(src_tables)[:4]})"
        con.execute("BEGIN")
        for t, (cols, where) in TABLES.items():
            if t not in src_tables:
                continue
            src_cols = {r[0] for r in con.execute(f"DESCRIBE src.public.{t}").fetchall()}
            # Name the target columns explicitly. INSERT ... SELECT binds BY POSITION, and
            # ALTER TABLE ADD COLUMN appends to the end -- so after a schema migration the
            # table's physical order no longer matches the order of the cols list, and values
            # land in the wrong columns (this silently tried to cast platform='sleeper' into
            # playoff_start_week INTEGER and skipped all 2,200 leagues).
            collist = ", ".join(f'"{n}"' for n, _ in cols)
            con.execute(f"INSERT INTO public.{t} ({collist}) {_select_sql(src_cols, cols, t, where)}")
        con.execute("INSERT INTO _sources VALUES (?, now())", [league_dir.name])
        con.execute("COMMIT")
        return True, ""
    except Exception as e:
        try:
            con.execute("ROLLBACK")
        except Exception:
            pass
        return False, str(e).splitlines()[0][:120]
    finally:
        try:
            con.execute("DETACH src")
        except Exception:
            pass


def main() -> None:
    import argparse
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--rebuild", action="store_true",
                    help="drop and re-fold everything. Needed after WIDENING the column "
                         "contracts above: open_snapshot() adds missing columns, but rows folded "
                         "earlier keep NULL in them, and _sources makes fold_league() skip those "
                         "leagues forever. Re-folding is the only way to populate new columns.")
    args = ap.parse_args()

    con = open_snapshot(OUT)
    if args.rebuild:
        n_before = con.execute("SELECT COUNT(*) FROM _sources").fetchone()[0]
        print(f"[corpus-snapshot] --rebuild: dropping {n_before:,} folded leagues, re-folding from source")
        for t in TABLES:
            con.execute(f"DELETE FROM public.{t}")
        con.execute("DELETE FROM _sources")
    done = folded_set(con)
    dirs = [d for d in league_dirs() if d.name not in done]
    print(f"[corpus-snapshot] {len(done):,} already folded; {len(dirs):,} to fold")
    ok = skipped = 0
    t0 = time.time()
    for i, d in enumerate(dirs, 1):
        good, msg = fold_league(con, d)
        if good:
            ok += 1
        else:
            skipped += 1
            print(f"  [skip] {d.name}: {msg}")
        if i % 200 == 0:
            print(f"  ... {i:,}/{len(dirs):,} folded ({time.time()-t0:.0f}s)")

    print(f"[corpus-snapshot] folded {ok:,} new leagues, skipped {skipped:,} -> {OUT}")
    for t in TABLES:
        n, lg = con.execute(f"SELECT COUNT(*), COUNT(DISTINCT db_name) FROM public.{t}").fetchone()
        print(f"  {t}: {n:,} rows / {lg:,} leagues")
    con.close()


if __name__ == "__main__":
    main()
