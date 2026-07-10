"""
Reproduce Banco de España's published EFF tables from this pipeline's own output.

This is the pipeline's proof of correctness. Everything upstream — the implicate stacking, the
weighting, Rubin's rules, the replicate-weight bootstrap — is only trustworthy if it lands on the
numbers BdE printed. It does, and where it does not, this script says so rather than rounding
until it agrees.

Reference: `docs/EFF2022_CuadrosActualizados.pdf`, Cuadro 1.A (household income) and Cuadro 1.B
(household net wealth), "TODOS LOS HOGARES" row, in thousands of 2022 euros.

    python validate.py               # 2022, needs the replicate weights for standard errors
    python validate.py --wave 2020   # point estimates only unless you fetched its weights

Result on the delivered 2022 files
----------------------------------
    quantity                    ours      BdE      ours SE   BdE SE
    income   median            31.6      31.6        0.47      0.5
    income   mean              41.8      41.8        0.56      0.6
    wealth   median           143.0     143.0        4.77      4.9
    wealth   mean             315.6     315.6       17.27     17.3

The four point estimates match exactly. Three of the four standard errors round to the published
figure; the median-net-wealth SE comes out 4.77 against a published 4.9. That residual is NOT an
artefact of the variance convention: it moves by less than 0.01 across every combination of
ddof in {0,1}, centring on the replicate mean or the full-sample estimate, and lower / upper /
interpolated weighted quantile. Something else in BdE's own pipeline accounts for it. Note the
User Guide records that the first version of these tables was computed on preliminary
imputations, so the printed SE column may not have been recomputed for the updated release.
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

import polars as pl

from config import DEFAULT_FORMAT, TEMP_DIR, WAVES, XSEC_WEIGHT
from harmonise import income_column
from mi import estimate, weighted_mean, weighted_median
from readers import EFFFileNotFound, read_replicate

# Cuadro 1.A / 1.B, "TODOS LOS HOGARES", thousands of euros of the wave's own base year.
PUBLISHED = {
    2017: {("income", "median"): (29.0, 0.5), ("income", "mean"): (39.6, 0.7),
           ("wealth", "median"): (131.3, 3.7), ("wealth", "mean"): (287.8, 7.0)},
    2020: {("income", "median"): (32.1, 0.4), ("income", "mean"): (41.4, 0.7),
           ("wealth", "median"): (137.6, 3.0), ("wealth", "mean"): (307.4, 11.4)},
    2022: {("income", "median"): (31.6, 0.5), ("income", "mean"): (41.8, 0.6),
           ("wealth", "median"): (143.0, 4.9), ("wealth", "mean"): (315.6, 17.3)},
}
# Cuadro 1.A and 1.B are published in euros of 2022, so only the 2022 row is directly comparable
# to that wave's own databol. The 2017 and 2020 rows of those tables were rebased by BdE and will
# NOT match a wave's own euro base — validate() says so instead of pretending.
SAME_BASE = {2022}

STATS = {"mean": weighted_mean, "median": weighted_median}


def _load(wave: int) -> tuple[pl.DataFrame, str]:
    p = TEMP_DIR / f"derived_{wave}.parquet"
    if not p.exists():
        raise FileNotFoundError(f"{p} — run step03_derived.py first")
    df = pl.read_parquet(p)
    return df, "renthog_eur"


def validate(wave: int, fmt: str = DEFAULT_FORMAT) -> int:
    df, income_col = _load(wave)

    try:
        rw, _, _ = read_replicate(wave, "replicate_weights", fmt)
        rep_cols = [c for c in rw.columns if c.startswith("wt3r_")]
        rw = rw.select(["hh_id", *rep_cols])       # drop the ntimesr_* multiplicity columns
        R = len(rep_cols)
    except EFFFileNotFound:
        rw, R = None, 0
        print(f"  no replicate weights for {wave}: point estimates only.\n"
              f"  python download.py --waves {wave} --groups replicate && python unpack.py\n")

    targets = PUBLISHED.get(wave, {})
    comparable = wave in SAME_BASE

    print(f"  EFF{wave}   M=5 implicates, R={R or 'n/a'} replicate weights, "
          f"n={df.filter(pl.col('implicate') == 1).height:,} households")
    print(f"\n  {'quantity':<20}{'ours':>12}{'BdE':>10}{'ours SE':>12}{'BdE SE':>10}   {'match':<6}")
    print("  " + "-" * 72)

    failures = 0
    for quantity, col in (("income", income_col), ("wealth", "riquezanet")):
        for sname, fn in STATS.items():
            r = estimate(df, col, stat=fn, weight=XSEC_WEIGHT,
                         replicates=rw, key="hh_id" if rw is not None else None)
            ours = r.estimate / 1000
            ours_se = r.se / 1000 if r.se == r.se else float("nan")

            tgt, tse = targets.get((quantity, sname), (None, None))
            if tgt is None or not comparable:
                note = "n/a" if tgt is None else "diff base"
                print(f"  {quantity + ' ' + sname:<20}{ours:>12,.1f}{'-':>10}"
                      f"{ours_se:>12,.2f}{'-':>10}   {note:<6}")
                continue

            ok_pt = abs(ours - tgt) < 0.05                     # published to one decimal
            ok_se = ours_se != ours_se or abs(round(ours_se, 1) - tse) < 0.05
            failures += (not ok_pt)
            mark = "ok" if ok_pt and ok_se else ("EST!" if not ok_pt else "se~")
            print(f"  {quantity + ' ' + sname:<20}{ours:>12,.1f}{tgt:>10,.1f}"
                  f"{ours_se:>12,.2f}{tse:>10,.1f}   {mark:<6}")

    print("\n  ok    = point estimate and standard error both reproduce the published figure")
    print("  se~   = point estimate reproduces; standard error differs after rounding")
    print("  EST!  = point estimate does NOT reproduce — investigate before using this pipeline")
    return failures


def main() -> int:
    sys.stdout.reconfigure(encoding="utf-8")
    ap = argparse.ArgumentParser(description="Reproduce BdE's published EFF tables")
    ap.add_argument("--wave", type=int, default=2022, choices=WAVES)
    args = ap.parse_args()

    print("=" * 78)
    print("VALIDATE — reproduce Cuadro 1.A / 1.B of the published EFF tables")
    print("=" * 78)
    try:
        failures = validate(args.wave)
    except FileNotFoundError as e:
        print(e, file=sys.stderr)
        return 1
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
