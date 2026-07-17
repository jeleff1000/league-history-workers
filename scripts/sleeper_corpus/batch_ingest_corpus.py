"""batch_ingest_corpus.py -- the corpus grind engine (parallel, all-partition, resumable).

Selects discovered leagues by COHORT NEED (take-all scarce, cap abundant) across all three
partitions and runs each through the local recipe into a smpl_* DuckDB on D:
  - single_season (is_dynasty=false, best_ball=false): ingest --stop-after 3 + playoff sim (clutch)
  - dynasty       (is_dynasty=true,  best_ball=false): same (dynasty has H2H playoffs)
  - best_ball     (best_ball=true):                    ingest ONLY (skip sim -- no H2H; draft lane)

Fully local, per-league, resumable (done-ledger). Parallel via --workers (each ingest is a
subprocess; the per-process Sleeper limiter + 429-backoff keep the API polite).

    py -3 scripts/sleeper_corpus/batch_ingest_corpus.py --plan-only
    py -3 scripts/sleeper_corpus/batch_ingest_corpus.py --all-partitions --workers 3 --limit 6
    py -3 scripts/sleeper_corpus/batch_ingest_corpus.py --all-partitions --workers 3   # full grind
"""
from __future__ import annotations
import argparse, json, os, shutil, subprocess, sys, threading, time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parent))
from build_corpus_snapshot import fold_league, open_snapshot

# Paths are env-overridable so this runs unchanged on a Linux GH runner (see
# corpus_crawl_worker.yml), where nothing lives on D:.
ROOT = Path(os.environ.get("YAHOO_OAUTH_ROOT", "d:/yahoo_oauth"))
CORPUS = Path(os.environ.get("CORPUS_DIR", "D:/league-history-data/fantasy_leagues/sampling_corpus"))
DRAIN = CORPUS / "discovery" / "drain_leagues.parquet"
DRAFT_COHORT = Path(os.environ.get(
    "DRAFT_COHORT_PARQUET",
    "D:/league-history-data/fantasy_leagues/cohort_aggregates/research_draft_player_season.parquet"))
SMPL_DIR = CORPUS / "leagues"
DONE = CORPUS / "ingest_ledger.json"
OPS_CACHE = Path(os.environ.get("OPS_CACHE_PATH", str(CORPUS / "ops_cache.duckdb")))
IMPORT_PY = ROOT / "fantasy_football_data_scripts" / "sleeper_initial_import.py"
SIM_PY = ROOT / "fantasy_football_data_scripts" / "multi_league" / "transformations" / "matchup" / "playoff_odds_import.py"

_lock = threading.Lock()


SLEEPER_IP_BUDGET = 1000  # Sleeper's documented ceiling, per IP (api.sleeper.app)


def load_env(workers: int = 1) -> dict:
    # Local runs read .env; on a GH runner there is no .env and secrets arrive as env vars.
    env_file = ROOT / ".env"
    if env_file.exists():
        for line in env_file.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, _, v = line.partition("="); os.environ.setdefault(k.strip(), v.strip().strip("'").strip('"'))
    env = dict(os.environ)
    env["OPS_CACHE_PATH"] = str(OPS_CACHE)
    env["PYTHONPATH"] = str(ROOT / "fantasy_football_data_scripts")
    # Each ingest is a subprocess with its OWN limiter, so the host's real rate is
    # workers x per-process budget. Divide the IP budget so N workers still total <=1000/min.
    # This is what makes it safe to raise --workers; without it, workers multiply the rate.
    env["SLEEPER_RATE_LIMIT_PER_MIN"] = str(max(1, SLEEPER_IP_BUDGET // max(1, workers)))
    return env


# partition -> (extra WHERE on the drain ledger, may subtract real-league baseline?, skip sim?)
#
# best_ball previously set skip_sim=True on the rationale "no H2H". That premise is false:
# measured 2026-07-16 across the 500 best-ball league-years already in the corpus, 97.0% of
# player rows carry a `win` and there are 18,409 champion rows -- these leagues are head-to-head.
# The sim also handles them fine (real-snapshot best-ball clutch has SD 1.03 vs managed's 1.23,
# 61% nonzero). Skipping it cost clutch on every best-ball league we crawled. Sims are cheap in
# parallel, so pay it during the crawl rather than backfilling later -- especially since --prune
# deletes the raw dir, which would make a later sim impossible.
PARTITIONS = {
    "single_season": ("is_dynasty=false AND COALESCE(best_ball,false)=false", True, False),
    "dynasty":       ("is_dynasty=true AND COALESCE(best_ball,false)=false", False, False),
    "best_ball":     ("COALESCE(best_ball,false)=true", False, False),
}


def build_plan(target: int, years: tuple[int, int], partitions: list[str],
               subtract_baseline: bool = False) -> list[dict]:
    """Plan the crawl. `subtract_baseline` decides whether the REAL customer leagues count
    toward a cohort's target.

    It defaults False: the corpus is meant to stand alone as the research population, so a
    cohort's need is measured from zero. When it was True, every popular flx cohort-year read
    as already-satisfied by the customer baseline (n>=35 in ___leagues), so `cohort_need`
    fell to 0 and the crawler skipped exactly the cells the research pages serve -- while
    dynasty/best_ball (which never subtract) kept a full need of `target` and soaked up the
    whole grind. That asymmetry is why 1,893 crawled leagues yielded only 57 managed ones.
    """
    con = duckdb.connect()
    con.execute(f"CREATE VIEW led AS SELECT * FROM '{DRAIN.as_posix()}' WHERE season IS NOT NULL")
    # The baseline view is built from the CUSTOMER-league draft cohort. Only touch it when the
    # caller actually wants it subtracted -- otherwise the crawl needs no customer-derived file
    # at all, which is what keeps the public-repo seed to public Sleeper data only.
    if subtract_baseline:
        con.execute(f"CREATE VIEW cur AS SELECT teams,roster,ppr,td,year, MAX(n_leagues) cur_n "
                    f"FROM '{DRAFT_COHORT.as_posix()}' WHERE cohort_level=4 GROUP BY 1,2,3,4,5")
    out: list[dict] = []
    for part in partitions:
        where, may_subtract, skip_sim = PARTITIONS[part]
        use_base = may_subtract and subtract_baseline
        base = "COALESCE(c.cur_n,0)" if use_base else "0"
        join = ("LEFT JOIN cur c ON c.teams=h.teams AND c.roster=h.roster "
                "AND c.ppr=h.ppr AND c.td=h.td AND c.year=h.season") if use_base else ""
        res = con.execute(f"""
          WITH heads AS (
            SELECT league_id, teams, roster, ppr, td, season, created_year
            FROM led
            WHERE {where} AND teams IS NOT NULL AND season BETWEEN {years[0]} AND {years[1]}
              AND league_id NOT IN (SELECT prev_league_id FROM led WHERE prev_league_id IS NOT NULL)
          ),
          need AS (
            SELECT h.*, GREATEST({target} - {base}, 0) AS cohort_need
            FROM heads h {join}
          ),
          ranked AS (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY teams,roster,ppr,td,season
                                         ORDER BY created_year DESC, league_id) AS rn
            FROM need WHERE cohort_need > 0
          )
          SELECT league_id, teams, roster, ppr, td, season, cohort_need
          FROM ranked WHERE rn <= cohort_need
          -- ORDER BY is load-bearing: --offset slices this plan across matrix jobs, so the
          -- order must be identical in every job or slices overlap / drop leagues.
          ORDER BY teams, roster, ppr, td, season, league_id
        """)
        cols = [d[0] for d in res.description]
        for r in res.fetchall():
            d = dict(zip(cols, r)); d["partition"] = part; d["skip_sim"] = skip_sim
            out.append(d)
    # dedup by league_id (a league belongs to one partition), scarce cohorts first.
    # league_id is the final tiebreaker so the ordering is total (see --offset above).
    seen, dedup = set(), []
    for r in sorted(out, key=lambda x: (-x["cohort_need"], x["partition"], x["league_id"])):
        if r["league_id"] not in seen:
            seen.add(r["league_id"]); dedup.append(r)
    return dedup


def ingest_one(r: dict, env: dict) -> tuple[str, str]:
    lid = r["league_id"]
    ddir = SMPL_DIR / f"smpl_{lid}"
    db = f"smpl_{lid}"
    p1 = subprocess.run(
        [sys.executable, str(IMPORT_PY), "--league-id", lid, "--data-dir", str(ddir),
         "--database-name", db, "--skip-track-1", "--stop-after", "3"],
        env=env, capture_output=True, text=True, timeout=3000)
    if p1.returncode != 0:
        return "import_failed", (p1.stderr or p1.stdout)[-300:]
    if not r.get("skip_sim"):
        p2 = subprocess.run(
            [sys.executable, str(SIM_PY), "--db", db, "--data-dir", str(ddir)],
            env=env, capture_output=True, text=True, timeout=3000)
        if p2.returncode != 0:
            return "sim_failed", (p2.stderr or p2.stdout)[-300:]
    return "done", ""


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--target", type=int, default=35)
    ap.add_argument("--limit", type=int, default=None)
    ap.add_argument("--offset", type=int, default=0,
                    help="skip the first N of the (deterministic) not-yet-done plan; "
                         "with --limit this carves the disjoint slice for one matrix job")
    ap.add_argument("--workers", type=int, default=1,
                    help="parallel ingests on THIS host. The Sleeper IP budget (1000/min) is "
                         "divided across them automatically (SLEEPER_RATE_LIMIT_PER_MIN), so "
                         "raising this does not raise the host's request rate -- it trades "
                         "per-league latency for concurrency. Scale ACROSS runners for more IPs.")
    ap.add_argument("--years", default="2017,2025")
    ap.add_argument("--all-partitions", action="store_true")
    ap.add_argument("--subtract-baseline", action="store_true",
                    help="count REAL customer leagues toward each cohort's target. Off by "
                         "default: the corpus stands alone as the research population, and "
                         "subtracting makes the crawler skip the popular flx cells the pages "
                         "serve (see build_plan docstring).")
    ap.add_argument("--plan-only", action="store_true")
    ap.add_argument("--fold-into", default=None,
                    help="path to a central corpus snapshot; fold each league in as it lands "
                         "(110 KB/league vs 32 MB raw) so the slice ships as a small artifact")
    ap.add_argument("--prune", action="store_true",
                    help="rm -rf each raw league dir once folded (requires --fold-into); "
                         "keeps runner disk flat at ~workers x 32 MB")
    args = ap.parse_args()
    if args.prune and not args.fold_into:
        ap.error("--prune requires --fold-into (refusing to delete unfolded leagues)")
    y0, y1 = (int(x) for x in args.years.split(","))
    parts = list(PARTITIONS) if args.all_partitions else ["single_season"]

    plan = build_plan(args.target, (y0, y1), parts, subtract_baseline=args.subtract_baseline)
    from collections import Counter
    by_part = Counter(r["partition"] for r in plan)
    print(f"[plan] {len(plan):,} leagues selected (target {args.target}/cohort-year, "
          f"baseline={'subtracted' if args.subtract_baseline else 'ignored'}) across {parts}", flush=True)
    for p, n in by_part.items():
        print(f"    {p}: {n:,}", flush=True)

    SMPL_DIR.mkdir(parents=True, exist_ok=True)
    with _lock:
        done = json.loads(DONE.read_text()) if DONE.exists() else {}
    # Slice AFTER the done-filter so every job carves from the same remaining list.
    todo = [r for r in plan if done.get(r["league_id"]) != "done"]
    if args.offset:
        todo = todo[args.offset:]
    if args.limit:
        todo = todo[:args.limit]
    print(f"[plan] {len(todo):,} in this slice "
          f"(offset={args.offset}, limit={args.limit}, "
          f"{sum(1 for v in done.values() if v=='done'):,} already done)", flush=True)
    if args.plan_only:
        for r in todo[:10]:
            print(f"    {r['league_id']} {r['partition']} {r['teams']}/{r['roster']}/"
                  f"{r['ppr']}/{r['td']} {r['season']}", flush=True)
        return

    env = load_env(args.workers)
    print(f"[rate] {args.workers} workers x {env['SLEEPER_RATE_LIMIT_PER_MIN']} req/min "
          f"= {int(env['SLEEPER_RATE_LIMIT_PER_MIN']) * args.workers}/min for this IP "
          f"(Sleeper budget {SLEEPER_IP_BUDGET}/min)", flush=True)
    snap = open_snapshot(args.fold_into) if args.fold_into else None
    counts = {"done": 0, "fail": 0, "folded": 0}
    t_start = time.time()

    def run(r):
        t0 = time.time()
        status, msg = ingest_one(r, env)
        # DuckDB is single-writer: the fold shares the _lock with the ledger write, so
        # ingests stay parallel while folds serialize behind them.
        with _lock:
            done[r["league_id"]] = status
            DONE.write_text(json.dumps(done))
            counts["done" if status == "done" else "fail"] += 1
            if snap is not None and status == "done":
                ddir = SMPL_DIR / f"smpl_{r['league_id']}"
                ok, fmsg = fold_league(snap, ddir)
                if ok:
                    counts["folded"] += 1
                    if args.prune:
                        shutil.rmtree(ddir, ignore_errors=True)
                else:
                    msg = (msg + f" | fold: {fmsg}").strip(" |")
            tag = "OK" if status == "done" else status.upper()
            n = counts["done"] + counts["fail"]
            print(f"  [{n}/{len(todo)}] {r['league_id']} {r['partition'][:4]} "
                  f"{r['teams']}/{r['roster']}/{r['ppr']}/{r['td']} {r['season']} -> {tag} "
                  f"({time.time()-t0:.0f}s){' | '+msg[:100] if msg else ''}", flush=True)

    if args.workers <= 1:
        for r in todo:
            run(r)
    else:
        with ThreadPoolExecutor(max_workers=args.workers) as ex:
            list(as_completed(ex.submit(run, r) for r in todo))

    if snap is not None:
        snap.close()
    print(f"\n[done] {counts['done']} ok, {counts['fail']} failed, "
          f"{counts['folded']} folded in {(time.time()-t_start)/60:.0f} min -> {SMPL_DIR}", flush=True)
    # Exit non-zero when nothing worked. Counting failures but returning 0 let a CI job go
    # green on a 100% failure rate (the first GH pilot did exactly that: ok=0, failed=15,
    # every step green). A caller cannot distinguish "crawled nothing" from "nothing to do".
    if todo and not counts["done"]:
        raise SystemExit(f"[fatal] 0 of {len(todo)} leagues ingested -- see failures above")


if __name__ == "__main__":
    main()
