"""
Walk the extracted EFF files and write an inventory of what is actually present.

    python build_catalog.py

Emits, into catalog/:
  master_inventory.csv   one row per (wave, file): rows, cols, format, path
  wave_summary.csv       one row per wave: households, implicates present, replicates, panel share
  presence.csv           variable x wave presence matrix

`presence.csv` is the practical answer to "which waves have variable X?". The EFF questionnaire
is revised every wave: 2002 has 813 columns in `other_sections`, 2022 has 1,346. Questions are
added, split and dropped, and a variable name is reused only when the question is unchanged —
mostly. Check this matrix before pooling.
"""
from __future__ import annotations

import csv
import sys
from collections import defaultdict

import polars as pl

from config import (
    CATALOG, DEFAULT_FORMAT, IMPLICATES, PANEL_FLAG, WAVES, XSEC_WEIGHT,
    core_basenames, derived_basenames, hh_key, prev_key, shadow_basename,
)
from readers import EFFFileNotFound, header, n_replicates, read_other, resolve


def main() -> int:
    sys.stdout.reconfigure(encoding="utf-8")
    CATALOG.mkdir(parents=True, exist_ok=True)
    fmt = DEFAULT_FORMAT

    inventory: list[dict] = []
    presence: dict[str, dict[int, int]] = defaultdict(dict)
    summary: list[dict] = []

    for wave in WAVES:
        files: list[tuple[str, str]] = []
        for imp in IMPLICATES:
            for part, base in core_basenames(wave, imp, fmt).items():
                files.append((f"{part}_imp{imp}", base))
            files.append((f"derived_imp{imp}", derived_basenames(wave, fmt)[imp - 1]))
        files.append(("shadow", shadow_basename(wave, fmt)))
        for kind in ("replicate_weights", "replicate_pan1weights", "replicate_pan2weights"):
            files.append((kind, f"{kind}_{wave}.{fmt}"))

        present = 0
        for role, base in files:
            try:
                path = resolve(wave, base)
            except EFFFileNotFound:
                continue
            cols = header(wave, base, fmt)
            present += 1
            inventory.append({"wave": wave, "role": role, "file": path.name, "format": fmt,
                              "cols": len(cols), "bytes": path.stat().st_size, "path": str(path)})
            # Only the questionnaire files define the variable universe; the 1,000 replicate
            # weight columns would swamp the presence matrix.
            if role.startswith(("other", "section6", "derived")):
                for c in cols:
                    presence[c][wave] = 1

        # Wave-level facts that need the data, not just the header.
        try:
            df, _, _ = read_other(wave, 1, fmt)
        except EFFFileNotFound:
            print(f"  {wave}: no data extracted")
            continue
        key = hh_key(wave)
        rows = len(df)
        panel = (df[PANEL_FLAG].sum() if PANEL_FLAG in df.columns else 0)
        try:
            R = n_replicates(wave, "replicate_weights", fmt)
        except EFFFileNotFound:
            R = None
        summary.append({
            "wave": wave, "households": rows, "hh_key": key, "prev_key": prev_key(wave) or "",
            "implicates": sum(1 for r in inventory if r["wave"] == wave and r["role"].startswith("other")),
            "replicates": R if R is not None else "not downloaded",
            "panel_households": int(panel),
            "panel_share_pct": round(100 * panel / rows, 1) if rows else 0.0,
            "sum_facine3": int(df[XSEC_WEIGHT].sum()) if XSEC_WEIGHT in df.columns else 0,
            "files_present": present,
        })
        print(f"  {wave}  {rows:>6,} households  {present:>2d} files  "
              f"R={summary[-1]['replicates']}  panel={summary[-1]['panel_share_pct']:>5.1f}%  "
              f"pop={summary[-1]['sum_facine3']:>12,}")

    if not inventory:
        print("nothing extracted — run download.py && unpack.py first", file=sys.stderr)
        return 1

    with (CATALOG / "master_inventory.csv").open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(inventory[0]))
        w.writeheader()
        w.writerows(inventory)

    with (CATALOG / "wave_summary.csv").open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(summary[0]))
        w.writeheader()
        w.writerows(summary)

    with (CATALOG / "presence.csv").open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["variable"] + [str(x) for x in WAVES] + ["n_waves"])
        for var in sorted(presence):
            row = [presence[var].get(x, "") for x in WAVES]
            w.writerow([var] + row + [sum(1 for c in row if c)])

    n_all = sum(1 for v in presence if len(presence[v]) == len(WAVES))
    print(f"\n  files      : {len(inventory)}")
    print(f"  variables  : {len(presence):,} distinct; {n_all:,} present in all {len(WAVES)} waves")
    for p in ("master_inventory.csv", "wave_summary.csv", "presence.csv"):
        print(f"  -> {CATALOG / p}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
