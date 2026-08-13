"""Extract only the MFL bridge members needed for a guarded cache promotion."""
from __future__ import annotations

import argparse
import shutil
import sys
import zipfile
from pathlib import Path


REQUIRED_MEMBERS = {
    "out/team_signals.parquet": "team_signals.parquet",
    "out/player_bridge.parquet": "player_bridge.parquet",
}


def extract(*, archive: Path, out: Path) -> None:
    try:
        with zipfile.ZipFile(archive) as source:
            available = set(source.namelist())
            missing = sorted(set(REQUIRED_MEMBERS) - available)
            if missing:
                raise ValueError(f"required artifact members missing: {missing}")
            out.mkdir(parents=True, exist_ok=True)
            for member, filename in REQUIRED_MEMBERS.items():
                with source.open(member) as src, (out / filename).open("wb") as dst:
                    shutil.copyfileobj(src, dst)
    except (OSError, ValueError, zipfile.BadZipFile) as exc:
        raise SystemExit(str(exc)) from exc


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--archive", required=True, type=Path)
    parser.add_argument("--out", required=True, type=Path)
    args = parser.parse_args()
    extract(archive=args.archive, out=args.out)


if __name__ == "__main__":
    main()
