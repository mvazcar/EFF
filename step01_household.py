"""
Step 01 — household table, one row per (wave, household, implicate).

Stacks the five implicates of `otras_secciones` for each wave and adds the canonical identifier
columns. Everything the questionnaire asked outside section 6 lives here: demographics of the
reference person, real assets and their debts, other debts, financial assets, pensions and
insurance, means of payment, consumption and saving.

Five implicates x ~6,300 households = ~31,500 rows per wave. The `implicate` column is what
makes that legitimate: it is NOT a sample five times larger. Any estimate must be computed once
per implicate and combined with Rubin's rules (mi.py).

INPUT   raw/<wave>/otras_secciones_<wave>_imp{1..5}.dta      (other_sections_* in 2002)
OUTPUT  temp/household_<wave>.parquet                        one row per (household, implicate)
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

import polars as pl

from config import DEFAULT_FORMAT, PANEL_FLAG, TEMP_DIR, WAVES, XSEC_WEIGHT
from harmonise import canonical
from readers import EFFFileNotFound, stack_implicates


def build(wave: int, fmt: str = DEFAULT_FORMAT) -> pl.DataFrame:
    df, _, _ = stack_implicates(wave, "other", fmt)
    df = canonical(df, wave)
    return df.sort("implicate", "hh_id")


def main() -> int:
    sys.stdout.reconfigure(encoding="utf-8")
    print("=" * 74)
    print("STEP 01 — household table (otras_secciones, five implicates stacked)")
    print("=" * 74)
    built = 0
    for wave in WAVES:
        try:
            df = build(wave)
        except EFFFileNotFound as e:
            print(f"  {wave}: skipped ({e})")
            continue
        out = TEMP_DIR / f"household_{wave}.parquet"
        df.write_parquet(out)
        built += 1
        hh = df["hh_id"].n_unique()
        panel = df.filter(pl.col("implicate") == 1)[PANEL_FLAG].sum()
        pop = df.filter(pl.col("implicate") == 1)[XSEC_WEIGHT].sum()
        print(f"  {wave}: {len(df):>7,} rows  {hh:>6,} households x {len(df.columns):>5} cols"
              f"   panel={panel:>5,}   pop={pop:>12,.0f}")
    if not built:
        print("\nnothing built — run download.py && unpack.py first", file=sys.stderr)
        return 1
    print(f"\n  -> {TEMP_DIR}/household_<wave>.parquet   ({built} waves)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
