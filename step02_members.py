"""
Step 02 — member table, one row per (wave, household, member, implicate).

Section 6 is delivered household-wide: one row per household, with every question repeated nine
times, once per member slot. `p6_13_4_2` is question 13 for MEMBER 4's second employee job. This
step turns that into the long form everyone actually wants.

Two things make it more than a mechanical melt.

The member index is the first suffix, not the last. `p6_13_4_2` is member 4 job 2, not member 2.
readers.section6_members() encodes that, and validates it against the Stata variable labels
("Miem. 4.") before reshaping — raising rather than guessing if a wave ever breaks the pattern.

The nine member slots are padded. A three-person household still has columns for members 4..9,
all missing. `p1` (in otras_secciones, not section 6) gives the true member count, so this step
joins it in and drops the padding. Without that join a "member" row exists for 9 x 6,385
households and two thirds of them are phantoms.

INPUT   raw/<wave>/seccion6_<wave>_imp{1..5}.dta             (section6_* in 2002)
        raw/<wave>/otras_secciones_<wave>_imp1.dta           for p1, the member count
OUTPUT  temp/members_<wave>.parquet                          one row per (household, member, implicate)
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

import polars as pl

from config import DEFAULT_FORMAT, IMPLICATES, MAX_MEMBERS, MEMBER_COUNT, TEMP_DIR, WAVES, hh_key
from readers import EFFFileNotFound, read_other, section6_members


def build(wave: int, fmt: str = DEFAULT_FORMAT) -> pl.DataFrame:
    key = hh_key(wave)
    other, _, _ = read_other(wave, 1, fmt)
    counts = other.select([key, MEMBER_COUNT])

    frames = [section6_members(wave, imp, fmt, n_members=counts) for imp in IMPLICATES]
    long = pl.concat(frames, how="vertical_relaxed")
    return (long.with_columns(pl.lit(wave, dtype=pl.Int16).alias("wave"),
                              pl.col(key).cast(pl.Int64).alias("hh_id"))
                .sort("implicate", "hh_id", "member"))


def main() -> int:
    sys.stdout.reconfigure(encoding="utf-8")
    print("=" * 74)
    print("STEP 02 — member table (section 6 reshaped to one row per member)")
    print("=" * 74)
    built = 0
    for wave in WAVES:
        try:
            df = build(wave)
        except EFFFileNotFound as e:
            print(f"  {wave}: skipped ({e})")
            continue
        except ValueError as e:                       # member rule violated: do not write junk
            print(f"  {wave}: FAILED — {e}", file=sys.stderr)
            continue
        out = TEMP_DIR / f"members_{wave}.parquet"
        df.write_parquet(out)
        built += 1
        per_imp = len(df) // len(IMPLICATES)
        hh = df["hh_id"].n_unique()
        print(f"  {wave}: {len(df):>8,} rows  {per_imp:>7,} members/implicate  "
              f"{hh:>6,} households  mean size {per_imp/hh:>4.2f}  {len(df.columns):>4} cols")

        # Section 6 has nine member slots; p1 reaches 10 in 2002 and 11 in 2022. Those members
        # were never recorded by the survey, so they cannot appear here. Say so.
        other, _, _ = read_other(wave, 1)
        oversized = other.filter(pl.col(MEMBER_COUNT) > MAX_MEMBERS)
        if len(oversized):
            lost = int((oversized[MEMBER_COUNT] - MAX_MEMBERS).sum())
            print(f"        {len(oversized)} household(s) with p1 > {MAX_MEMBERS}: "
                  f"{lost} member(s) absent from section 6 in the source data")
    if not built:
        print("\nnothing built", file=sys.stderr)
        return 1
    print(f"\n  -> {TEMP_DIR}/members_<wave>.parquet   ({built} waves)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
