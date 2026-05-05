#!/usr/bin/env python
"""Revalidate and warm the bounded public Vercel cache surface for one league.

This is intentionally small and conservative: one league tag/path invalidation,
then a handful of hot page/API URLs. It is safe to run after every successful
quick/full import because it never fans out across all leagues.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import time
from collections.abc import Iterable
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from urllib.parse import quote, urlencode, urljoin
from urllib.request import Request, urlopen


DB_NAME_RE = re.compile(r"^[a-z0-9][a-z0-9_]{0,62}$")


QUICK_WARM_PATHS = (
    "/{db}",
    "/api/league/{db}/overview",
    "/api/league/{db}/features",
    "/api/league/{db}/standings",
    "/api/league/{db}/matchups?meta=1",
    "/api/league/{db}/players/weekly?meta=1",
)

FULL_WARM_PATHS = QUICK_WARM_PATHS + (
    "/{db}/managers",
    "/{db}/players",
    "/{db}/draft",
    "/{db}/team-stats",
    "/{db}/transactions",
    "/api/league/{db}/team-stats",
    "/api/league/{db}/transactions",
)


@dataclass(frozen=True)
class WarmResult:
    url: str
    status: int
    ms: int
    cache: str
    bytes_read: int
    error: str | None = None


def build_url(site_url: str, path: str) -> str:
    return urljoin(site_url.rstrip("/") + "/", path.lstrip("/"))


def revalidate(site_url: str, db: str, secret: str, strategy: str, timeout: int) -> None:
    params = urlencode({"db": db, "secret": secret, "strategy": strategy})
    url = build_url(site_url, f"/api/revalidate?{params}")
    req = Request(url, method="POST")
    with urlopen(req, timeout=timeout) as resp:
        body = resp.read().decode("utf-8", errors="replace")
        if resp.status >= 400:
            raise RuntimeError(f"revalidate failed ({resp.status}): {body}")
        payload = json.loads(body)
        if not payload.get("revalidated"):
            raise RuntimeError(f"revalidate did not confirm success: {body}")


def warm_one(url: str, timeout: int) -> WarmResult:
    start = time.perf_counter()
    req = Request(url, headers={"User-Agent": "leaguehistory-cache-warmer/1.0"})
    try:
        with urlopen(req, timeout=timeout) as resp:
            body = resp.read()
            ms = int((time.perf_counter() - start) * 1000)
            cache = resp.headers.get("x-vercel-cache") or resp.headers.get("x-nextjs-cache") or ""
            return WarmResult(url, resp.status, ms, cache, len(body))
    except Exception as exc:  # noqa: BLE001 - report and continue warming others
        ms = int((time.perf_counter() - start) * 1000)
        return WarmResult(url, 0, ms, "", 0, str(exc))


def iter_warm_urls(site_url: str, db: str, mode: str) -> Iterable[str]:
    if mode == "none":
        return ()
    paths = FULL_WARM_PATHS if mode == "full" else QUICK_WARM_PATHS
    safe_db = quote(db, safe="")
    return (build_url(site_url, path.format(db=safe_db)) for path in paths)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", required=True, help="League db_name to revalidate/warm")
    parser.add_argument("--secret", default="", help="REVALIDATION_SECRET")
    parser.add_argument("--site-url", default="https://leaguehistory.app")
    parser.add_argument("--mode", choices=("none", "quick", "full"), default="quick")
    parser.add_argument(
        "--strategy",
        choices=("swr", "expire"),
        default="expire",
        help="Use expire after imports so the warmer refreshes caches before users arrive.",
    )
    parser.add_argument("--timeout", type=int, default=25)
    parser.add_argument("--concurrency", type=int, default=3)
    parser.add_argument(
        "--strict",
        action="store_true",
        help="Return non-zero if revalidation or any warm request fails.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    db = args.db.strip()
    if not DB_NAME_RE.match(db):
        print(f"Invalid db name: {db}", file=sys.stderr)
        return 2

    if not args.secret:
        print("REVALIDATION_SECRET is not set; skipping Vercel cache warm-up")
        return 0

    site_url = args.site_url.rstrip("/")
    print(f"Revalidating {db} on {site_url} with strategy={args.strategy}")
    try:
        revalidate(site_url, db, args.secret, args.strategy, args.timeout)
    except Exception as exc:  # noqa: BLE001 - cache warm-up is best-effort by default
        print(f"Revalidation failed: {exc}", file=sys.stderr)
        return 1 if args.strict else 0

    urls = list(iter_warm_urls(site_url, db, args.mode))
    if not urls:
        print("Warm mode is none; revalidation complete")
        return 0

    print(f"Warming {len(urls)} URL(s) with concurrency={args.concurrency}")
    failures = 0
    with ThreadPoolExecutor(max_workers=max(1, args.concurrency)) as pool:
        futures = [pool.submit(warm_one, url, args.timeout) for url in urls]
        for future in as_completed(futures):
            result = future.result()
            marker = "OK" if result.status and result.status < 500 and not result.error else "FAIL"
            if marker == "FAIL":
                failures += 1
            cache = f" cache={result.cache}" if result.cache else ""
            err = f" error={result.error}" if result.error else ""
            print(f"{marker} {result.status} {result.ms}ms {result.bytes_read}B{cache} {result.url}{err}")

    if failures:
        print(f"Warm-up completed with {failures} failure(s)", file=sys.stderr)
        return 1 if args.strict else 0
    print("Warm-up complete")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
