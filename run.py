"""
Entry point for the EFF build pipeline.

Full flow, starting from raw_zipped/:

    # 0. get the archives into raw_zipped/ (flat, named as BdE serves them).
    #    Either drop them there by hand, or:
    export EFF_ACCESS_URL='...'          # the link BdE emails you; see download.py
    python download.py

    python unpack.py                     # 1. dry run: show what would be extracted
    python unpack.py --execute           #    raw_zipped/ -> raw/<wave>/, catalog/labels/, docs/
    python run.py                        # 2. build      -> temp/, output/
    python validate.py                   # 3. reproduce BdE's published tables

Or in one go:
    python run.py --download --unpack

Build options:
    python run.py --list              # print the input -> output contract and exit
    python run.py --resume 05         # rerun from step 05 onwards, reusing temp/ parquets
    python run.py --steps 03 06       # run only these step ids
    python run.py --serial            # one step at a time (low memory)
    python run.py --workers 4         # parallel step concurrency (default 3)
    python run.py --waves 2020 2022   # restrict the waves
    python run.py --catalog           # also rebuild catalog/ (labels.py, build_catalog.py)

Step ids: 01 household · 02 members · 03 derived · 04 weights · 05 panel · 06 pool.
"""
from __future__ import annotations

import argparse
import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))


def _script(name: str, *args: str) -> int:
    print(f">>> {name} {' '.join(args)}\n")
    return subprocess.run([sys.executable, str(ROOT / name), *args]).returncode


def main() -> int:
    sys.stdout.reconfigure(encoding="utf-8")
    from pipeline import BY_ID, describe, run_pipeline

    ap = argparse.ArgumentParser(description="EFF build pipeline")
    ap.add_argument("--list", action="store_true", help="print the input -> output contract, exit")
    ap.add_argument("--steps", nargs="+", default=None, help="run only these step ids (e.g. 03 06)")
    ap.add_argument("--resume", default=None, metavar="ID",
                    help="rerun from this step onwards, reusing earlier temp/ parquets")
    ap.add_argument("--serial", action="store_true", help="run build steps sequentially")
    ap.add_argument("--workers", type=int, default=3, help="parallel step concurrency (default 3)")
    ap.add_argument("--waves", nargs="+", type=int, default=None, help="restrict to these waves")
    ap.add_argument("--download", action="store_true", help="run download.py first")
    ap.add_argument("--unpack", action="store_true", help="run unpack.py --execute before building")
    ap.add_argument("--catalog", action="store_true", help="rebuild labels.py + build_catalog.py")
    args = ap.parse_args()

    if args.list:
        print(describe())
        return 0

    for sid in (args.steps or []) + ([args.resume] if args.resume else []):
        if sid not in BY_ID:
            print(f"unknown step id {sid!r}; choose from {sorted(BY_ID)}", file=sys.stderr)
            return 2
    if args.steps and args.resume:
        print("--steps and --resume are mutually exclusive", file=sys.stderr)
        return 2

    if args.waves:                       # propagate to every step subprocess
        os.environ["EFF_WAVES"] = ",".join(str(w) for w in args.waves)
        print(f"WAVE OVERRIDE: {args.waves}\n")

    if args.download:
        if _script("download.py") != 0:
            print("download failed", file=sys.stderr)
            return 1
        print()
    if args.unpack:
        # --execute, because unpack.py is dry-run by default and `run.py --unpack` is an
        # unambiguous instruction to actually do it.
        if _script("unpack.py", "--execute") != 0:
            return 1
        print()
    if args.catalog:
        _script("labels.py")
        _script("build_catalog.py")
        print()

    failed = run_pipeline(only=args.steps, workers=args.workers,
                          parallel=not args.serial, resume_from=args.resume)
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
