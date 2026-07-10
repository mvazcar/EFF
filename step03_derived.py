"""
Step 03 — derived variables (`databol`), one row per (wave, household, implicate).

`databol1..databol5` are the constructed variables Banco de España builds its own published
tables from: net wealth (`riquezanet`), gross wealth, real and financial assets, debt, and the
breakdown variables (`bage`, `percrent`, `percriq`, `nsitlabdom`, `neducdom`). Using them rather
than reconstructing the aggregates from the questionnaire is what makes `validate.py` able to
reproduce Cuadro 1.A and 1.B to the published decimal.

The one column that cannot be pooled as-is is total household income: it is renamed in every
wave and expressed in that wave's own euros — see harmonise.py. It comes out here as
`renthog_eur`, alongside `income_year` and `euro_base_year` so the rebasing is explicit and
someone else's deflator can be applied later.

Note the source filenames carry no year: every wave ships `databol1.dta`..`databol5.dta`. They
are kept apart by living in raw/<wave>/.

INPUT   raw/<wave>/databol{1..5}.dta                         no year in the filename; see below
OUTPUT  temp/derived_<wave>.parquet                          the 54-column cross-wave core
        temp/derived_full_<wave>.parquet                     the same rows, every databol column
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

import polars as pl

from config import DEFAULT_FORMAT, TEMP_DIR, WAVES
from harmonise import canonical, harmonise_derived, income_column
from readers import EFFFileNotFound, stack_implicates


def build(wave: int, fmt: str = DEFAULT_FORMAT) -> tuple[pl.DataFrame, pl.DataFrame]:
    df, _, _ = stack_implicates(wave, "derived", fmt)
    full = canonical(df, wave).sort("implicate", "hh_id")
    core = harmonise_derived(df, wave).sort("implicate", "hh_id")
    return core, full


def main() -> int:
    sys.stdout.reconfigure(encoding="utf-8")
    print("=" * 74)
    print("STEP 03 — derived variables (databol, five implicates stacked)")
    print("=" * 74)
    built = 0
    for wave in WAVES:
        try:
            core, full = build(wave)
        except EFFFileNotFound as e:
            print(f"  {wave}: skipped ({e})")
            continue
        core.write_parquet(TEMP_DIR / f"derived_{wave}.parquet")
        full.write_parquet(TEMP_DIR / f"derived_full_{wave}.parquet")
        built += 1
        col, yi, ye = income_column(full.columns)
        base = f"{ye} euros" if ye else "euro base undocumented"
        print(f"  {wave}: {len(core):>7,} rows  core {len(core.columns):>3} cols / "
              f"full {len(full.columns):>3}   income {col!r} -> {base}")
    if not built:
        print("\nnothing built", file=sys.stderr)
        return 1
    print(f"\n  -> {TEMP_DIR}/derived_<wave>.parquet   ({built} waves)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
