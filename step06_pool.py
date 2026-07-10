"""
Step 06 — pool the derived core across waves into one analysis frame.

One row per (wave, household, implicate), carrying the 54 `databol` variables that exist in all
eight waves plus the harmonised income column and the cross-wave `panel_id` from step 05.

This is the frame to do cross-wave work on. It deliberately does NOT include the ~4,000
questionnaire variables: a shared column name across waves is not a promise of a shared meaning
(the occupation coding changed between 2008 and 2011), and silently stacking them would produce
a frame that looks poolable and is not. Those live in the per-wave `temp/household_<wave>.parquet`
and you pool them yourself, having checked catalog/presence.csv.

Two columns everyone gets wrong, so they are carried explicitly:

  euro_base_year   each wave's monetary values are in that wave's own euros (2009's, for the
                   2008 wave). `riquezanet` in 2005 and in 2022 are not the same unit. Nothing
                   here is deflated; harmonise.deflate() will do it with an index you choose.
  n_waves          how many waves this household is observed in, 1..4. The EFF panel rotates,
                   so no household spans all eight — the longest observed spell is four.

INPUT   temp/derived_<wave>.parquet  (every wave)            the 54-column cross-wave core
        output/panel_bridge.parquet                          for panel_id / n_waves
OUTPUT  output/eff_derived_panel.parquet                     one row per (wave, household, implicate)
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

import polars as pl

from config import OUTPUT, TEMP_DIR, WAVES


def build() -> pl.DataFrame:
    frames = []
    for wave in WAVES:
        p = TEMP_DIR / f"derived_{wave}.parquet"
        if not p.exists():
            print(f"  {wave}: no derived parquet — run step03 first")
            continue
        frames.append(pl.read_parquet(p))
    if not frames:
        return pl.DataFrame()

    pooled = pl.concat(frames, how="diagonal_relaxed")

    bridge_path = OUTPUT / "panel_bridge.parquet"
    if bridge_path.exists():
        bridge = pl.read_parquet(bridge_path).select(
            ["wave", "hh_id", "panel_id", "n_waves", "first_wave", "last_wave"])
        pooled = pooled.join(bridge, on=["wave", "hh_id"], how="left")
    else:
        print("  no panel_bridge.parquet — run step05 first; pooling without panel_id")

    return pooled.sort("wave", "implicate", "hh_id")


def main() -> int:
    sys.stdout.reconfigure(encoding="utf-8")
    print("=" * 74)
    print("STEP 06 — pooled derived panel")
    print("=" * 74)
    df = build()
    if df.is_empty():
        print("nothing to pool", file=sys.stderr)
        return 1

    out = OUTPUT / "eff_derived_panel.parquet"
    df.write_parquet(out)

    per = (df.filter(pl.col("implicate") == 1)
             .group_by("wave")
             .agg(pl.len().alias("households"), pl.col("euro_base_year").first())
             .sort("wave"))
    print(f"\n  {'wave':<6}{'households':>12}{'euro base':>16}")
    for w, h, e in zip(per["wave"], per["households"], per["euro_base_year"]):
        base = str(e) if e is not None else "undocumented"
        print(f"  {w:<6}{h:>12,}{base:>16}")

    if "n_waves" in df.columns:
        spells = (df.filter(pl.col("implicate") == 1).group_by("n_waves")
                    .agg(pl.col("panel_id").n_unique().alias("households")).sort("n_waves"))
        print("\n  distinct households by spell length: "
              f"{dict(zip(spells['n_waves'], spells['households']))}")

    print(f"\n  -> {out}  ({len(df):,} rows x {len(df.columns)} cols)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
