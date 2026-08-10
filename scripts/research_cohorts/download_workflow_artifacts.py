"""Download known GitHub Actions artifacts by ID without the 200-item index cap."""
from __future__ import annotations

import argparse
import io
import os
import time
import urllib.error
import urllib.request
from urllib.parse import urlparse
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path


class _BlobRedirectWithoutBearer(urllib.request.HTTPRedirectHandler):
    """Do not forward the GitHub API bearer token to Azure blob storage."""

    def redirect_request(self, req, fp, code, msg, headers, newurl):  # type: ignore[override]
        redirected = super().redirect_request(req, fp, code, msg, headers, newurl)
        if redirected is not None and urlparse(newurl).netloc != "api.github.com":
            redirected.headers.pop("Authorization", None)
        return redirected


_OPENER = urllib.request.build_opener(_BlobRedirectWithoutBearer)


def download(repo: str, artifact_id: str, out: Path, token: str) -> None:
    url = f"https://api.github.com/repos/{repo}/actions/artifacts/{artifact_id}/zip"
    request = urllib.request.Request(
        url,
        headers={
            "Accept": "application/vnd.github+json",
            "Authorization": f"Bearer {token}",
            "User-Agent": "league-history-research-audit",
        },
    )
    last: Exception | None = None
    for attempt in range(6):
        try:
            with _OPENER.open(request, timeout=180) as response:
                payload = response.read()
            target = out / str(artifact_id)
            target.mkdir(parents=True, exist_ok=True)
            with zipfile.ZipFile(io.BytesIO(payload)) as archive:
                archive.extractall(target)
            return
        except (urllib.error.HTTPError, urllib.error.URLError, TimeoutError, zipfile.BadZipFile) as exc:
            last = exc
            if attempt == 5:
                break
            time.sleep(2**attempt)
    raise RuntimeError(f"artifact {artifact_id} failed after retries: {last}")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--repo", required=True)
    ap.add_argument("--run-id", required=True)
    ap.add_argument("--ids", required=True)
    ap.add_argument("--out", type=Path, required=True)
    ap.add_argument("--workers", type=int, default=8)
    args = ap.parse_args()
    token = os.environ.get("GITHUB_TOKEN")
    if not token:
        raise SystemExit("GITHUB_TOKEN is required")
    ids = [x.strip() for x in args.ids.split(",") if x.strip()]
    if not ids:
        raise SystemExit("no artifact IDs supplied")
    args.out.mkdir(parents=True, exist_ok=True)
    workers = max(1, min(args.workers, len(ids)))
    failures = []
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(download, args.repo, artifact_id, args.out, token): artifact_id for artifact_id in ids}
        for index, future in enumerate(as_completed(futures), 1):
            artifact_id = futures[future]
            try:
                future.result()
            except Exception as exc:
                failures.append((artifact_id, str(exc)))
                for pending in futures:
                    pending.cancel()
                break
            print(f"downloaded {index}/{len(ids)} artifact {artifact_id}", flush=True)
    if failures:
        raise RuntimeError("artifact download failures: " + repr(failures))


if __name__ == "__main__":
    main()
