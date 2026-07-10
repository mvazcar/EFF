"""
Step 04 — replicate weights, one row per (wave, household).

Three files per wave, each ~1,000 columns wide:

    replicate_weights_<wave>       wt3r_1..R      cross-sectional
    replicate_pan1weights_<wave>   wtpan1r_1..R   panel, calibrated to the PREVIOUS wave's population
    replicate_pan2weights_<wave>   wtpan2r_1..R   panel, calibrated to THIS wave's population

R is 999 in 2002 and 1000 thereafter — read off the header, never assumed, because the two User
Guides that state it for 2014 contradict each other (1000 in section 1.2, 999 in section 6).
2002 has no panel replicate files: it is the first wave.

The companion `ntimesr_i` / `ntimespan{1,2}r_i` columns give the number of times each household
was drawn into replicate i. They are dropped here. `wt3r_i` already embeds the multiplicity, so
weighted statistics do not need them; keeping 1,000 extra integer columns would double the file
for nothing. Re-run with `--keep-ntimes` if you have an estimator that rebuilds the resample.

These files are large (the 2022 cross-section is 58 MB of Stata, 2,001 columns) and are NOT in
download.py's default groups. Fetch them with `python download.py --groups replicate` when you
need standard errors, which is whenever you are reporting a number to someone else.

INPUT   raw/<wave>/replicate_weights_<wave>.dta              optional; ~700 MB across all waves
        raw/<wave>/replicate_pan{1,2}weights_<wave>.dta      absent in 2002 (first wave)
OUTPUT  temp/weights_<wave>_<kind>.parquet                   one row per household, R weight columns

Absent input is not a failure: this step reports "skipping" and exits 0, because the replicate
weights are outside download.py's default groups and a fresh checkout legitimately lacks them.
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

import polars as pl

from config import DEFAULT_FORMAT, REPLICATE_SETS, TEMP_DIR, WAVES, hh_key
from readers import EFFFileNotFound, read_replicate


def build(wave: int, kind: str, fmt: str = DEFAULT_FORMAT,
          keep_ntimes: bool = False) -> pl.DataFrame:
    df, _, _ = read_replicate(wave, kind, fmt)
    key = hh_key(wave)
    w_prefix, n_prefix = REPLICATE_SETS[kind]

    weight_cols = sorted((c for c in df.columns if c.startswith(w_prefix)),
                         key=lambda c: int(c[len(w_prefix):]))
    keep = [key, *weight_cols]
    if keep_ntimes:
        keep += sorted((c for c in df.columns if c.startswith(n_prefix)),
                       key=lambda c: int(c[len(n_prefix):]))
    return df.select(keep).with_columns(pl.col(key).cast(pl.Int64).alias("hh_id"))


def main() -> int:
    sys.stdout.reconfigure(encoding="utf-8")
    ap = argparse.ArgumentParser(description="Convert replicate weights to parquet")
    ap.add_argument("--keep-ntimes", action="store_true",
                    help="keep the ntimes* multiplicity columns (doubles the file size)")
    args = ap.parse_args()

    print("=" * 74)
    print("STEP 04 — replicate weights")
    print("=" * 74)
    built = 0
    for wave in WAVES:
        for kind in REPLICATE_SETS:
            try:
                df = build(wave, kind, keep_ntimes=args.keep_ntimes)
            except EFFFileNotFound:
                continue
            out = TEMP_DIR / f"weights_{wave}_{kind}.parquet"
            df.write_parquet(out)
            built += 1
            prefix = REPLICATE_SETS[kind][0]
            R = sum(c.startswith(prefix) for c in df.columns)
            print(f"  {wave}  {kind:<24} {len(df):>6,} households   R={R:>4}")

    if not built:
        # Nothing to do is not a failure. The replicate weights are ~700 MB and deliberately
        # outside download.py's default groups, so their absence is the normal state of a fresh
        # checkout. Exiting non-zero here would make every default build look broken.
        print("\nno replicate weights present — skipping (standard errors will be unavailable).\n"
              "  python download.py --groups replicate   (~700 MB across all waves)")
        return 0
    print(f"\n  -> {TEMP_DIR}/weights_<wave>_<kind>.parquet   ({built} files)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
