"""Download known GitHub Actions artifacts by ID without the 200-item index cap."""
from __future__ import annotations

import argparse
import io
import os
import time
import urllib.error
import urllib.request
import zipfile
from pathlib import Path


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
            with urllib.request.urlopen(request, timeout=180) as response:
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
    args = ap.parse_args()
    token = os.environ.get("GITHUB_TOKEN")
    if not token:
        raise SystemExit("GITHUB_TOKEN is required")
    ids = [x.strip() for x in args.ids.split(",") if x.strip()]
    if not ids:
        raise SystemExit("no artifact IDs supplied")
    args.out.mkdir(parents=True, exist_ok=True)
    for index, artifact_id in enumerate(ids, 1):
        download(args.repo, artifact_id, args.out, token)
        print(f"downloaded {index}/{len(ids)} artifact {artifact_id}", flush=True)


if __name__ == "__main__":
    main()
