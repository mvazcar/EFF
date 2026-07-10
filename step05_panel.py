"""
Step 05 — the cross-wave household bridge.

Why this step exists
--------------------
The EFF has no persistent household identifier. Each wave numbers its households from scratch:
`h_2020 = 4` and `h_2022 = 4` are unrelated. Linking waves by id value produces nonsense.

What each wave does carry, from 2005 on, is a second column holding the PREVIOUS wave's id
(`h_2020` inside the 2022 file), plus `hogarpanel` flagging the households that appear in both.
The User Guide documents only this adjacent link, and describes the two-wave panel as the object
of study. It is quieter about the fact that the links compose: the 2008 file carries `h_2005`,
the 2011 file carries `h_2008`, and so on without a gap, back to 2005's `h_number` — which is
the 2002 wave's household id under its old name.

So the whole 2002-2022 history chains. This step verifies that and materialises it.

Method
------
Treat each (wave, household) as a node and each `hogarpanel` link as an edge, then take
connected components with a union-find. The component root becomes `panel_id`, a synthetic
identifier stable across every wave in which the household was interviewed.

Union-find rather than a fold of pairwise joins because the graph is not a simple chain: a wave's
back-link can point two households at the same predecessor, when a household splits in two
between waves. It happens twice in the delivered data, once at the 2005 -> 2008 transition and
once at 2014 -> 2017. Components handle that; a fold of pairwise joins would silently duplicate
rows. `_diagnostics` prints whichever splits your copy of the data contains — the record
identifiers are not repeated here, since they are microdata.

What a "panel household" is not
-------------------------------
The User Guide's warning is worth repeating: a household linked across waves is the household
living at that address in the panel sample, and its composition may have changed completely.
`pan_1..pan_9` map member slots between adjacent waves (member x in this wave was member y in
the last), so a member-level link is possible for adjacent waves only, and this step does not
attempt to chain those.

Weights
-------
`pesopan_1` and `pesopan_2` are calibrated for the two-wave panel of ONE adjacent pair, to the
previous and current wave's population respectively. There is no weight calibrated for a
household observed in five waves. If you use `panel_id` across a long window you are doing
unweighted or self-weighted analysis; say so.

INPUT   temp/household_<wave>.parquet  (every wave)          hh_id, hh_id_prev, hogarpanel
OUTPUT  output/panel_bridge.parquet                          one row per (wave, hh_id), + panel_id
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

import polars as pl

from config import OUTPUT, PANEL_FLAG, PREV_WAVE, TEMP_DIR, WAVES


class UnionFind:
    def __init__(self) -> None:
        self.parent: dict[tuple[int, int], tuple[int, int]] = {}

    def find(self, x):
        self.parent.setdefault(x, x)
        root = x
        while self.parent[root] != root:
            root = self.parent[root]
        while self.parent[x] != root:                       # path compression
            self.parent[x], x = root, self.parent[x]
        return root

    def union(self, a, b) -> None:
        ra, rb = self.find(a), self.find(b)
        if ra != rb:
            # Root on the EARLIER wave, so panel_id is the household's first appearance.
            lo, hi = (ra, rb) if ra < rb else (rb, ra)
            self.parent[hi] = lo


def _edges() -> tuple[list[tuple], list[tuple]]:
    """(nodes, edges) read off the per-wave household parquets."""
    nodes: list[tuple[int, int]] = []
    edges: list[tuple[tuple, tuple]] = []
    for wave in WAVES:
        p = TEMP_DIR / f"household_{wave}.parquet"
        if not p.exists():
            print(f"  {wave}: no household parquet — run step01 first")
            continue
        df = (pl.read_parquet(p, columns=["implicate", "hh_id", "hh_id_prev", PANEL_FLAG])
                .filter(pl.col("implicate") == 1))
        nodes += [(wave, int(h)) for h in df["hh_id"]]

        prev = PREV_WAVE.get(wave)
        if prev is None:
            continue
        linked = df.filter((pl.col(PANEL_FLAG) == 1) & pl.col("hh_id_prev").is_not_null()
                           & (pl.col("hh_id_prev") > 0))
        edges += [((wave, int(a)), (prev, int(b)))
                  for a, b in zip(linked["hh_id"], linked["hh_id_prev"])]
    return nodes, edges


def build() -> pl.DataFrame:
    nodes, edges = _edges()
    if not nodes:
        return pl.DataFrame()

    uf = UnionFind()
    for n in nodes:
        uf.find(n)
    for a, b in edges:
        uf.union(a, b)

    rows = [{"wave": w, "hh_id": h, "panel_wave0": uf.find((w, h))[0],
             "panel_hh0": uf.find((w, h))[1]} for w, h in nodes]
    df = pl.DataFrame(rows)
    # panel_id: first wave seen * 10^7 + that wave's household number. Readable and stable.
    df = df.with_columns(
        (pl.col("panel_wave0").cast(pl.Int64) * 10_000_000 + pl.col("panel_hh0")).alias("panel_id")
    )
    # n_waves counts DISTINCT waves, not rows. A component can hold two households from the same
    # wave when one household splits in two between waves — it happens twice in 2008 — and
    # counting rows would report that household as spanning one wave more than it does.
    counts = df.group_by("panel_id").agg(
        pl.col("wave").n_unique().alias("n_waves"),
        pl.len().alias("n_records"),
        pl.col("wave").min().alias("first_wave"),
        pl.col("wave").max().alias("last_wave"),
    )
    return df.join(counts, on="panel_id").sort("wave", "hh_id")


def _diagnostics(df: pl.DataFrame) -> None:
    comps = df.group_by("panel_id").agg(
        pl.col("wave").n_unique().alias("n_waves"),
        pl.len().alias("n_records"),
        pl.col("wave").min().alias("first_wave"),
        pl.col("wave").max().alias("last_wave"),
    )
    spell = comps.group_by("n_waves").agg(pl.len().alias("households")).sort("n_waves")
    total = spell["households"].sum()

    print("\n  households observed in exactly N waves")
    for n, h in zip(spell["n_waves"], spell["households"]):
        print(f"    {n} wave{'s' if n > 1 else ' '}: {h:>6,}  ({100*h/total:>4.1f}%)")
    print(f"    total distinct households: {total:,}")

    # The EFF panel is rotating: a spell is bounded by design, not by attrition alone. Report the
    # actual observed windows rather than assuming the chain can run 2002 -> 2022.
    longest = comps.filter(pl.col("n_waves") == comps["n_waves"].max())
    print(f"\n  longest spell: {comps['n_waves'].max()} waves "
          f"({len(longest):,} households)")
    windows = (comps.filter(pl.col("n_waves") >= 3)
                    .group_by(["first_wave", "last_wave"]).agg(pl.len().alias("n"))
                    .sort("n", descending=True).head(5))
    print("  most common (first, last) windows among 3+-wave households:")
    for a, b, n in zip(windows["first_wave"], windows["last_wave"], windows["n"]):
        print(f"    {a} -> {b}: {n:>5,}")

    # Does any wave point two households at the same predecessor (a household that split)?
    split = comps.filter(pl.col("n_records") > pl.col("n_waves"))
    print(f"\n  components with two households in one wave (a split household): {len(split)}")
    for pid in split["panel_id"].sort().to_list():
        rows = df.filter(pl.col("panel_id") == pid).sort("wave", "hh_id")
        pairs = list(zip(rows["wave"].to_list(), rows["hh_id"].to_list()))
        seen = [w for w in dict.fromkeys(w for w, _ in pairs)
                if sum(1 for x, _ in pairs if x == w) > 1]
        print(f"    panel_id {pid}: {pairs}  -> splits in {seen}")
    if len(split):
        print("    union-find is therefore necessary: a fold of pairwise joins would have "
              "duplicated these rows silently.")
    else:
        print("    none — every link is one-to-one, so a chain of joins would also have worked.")

    per_wave = df.group_by("wave").agg(pl.len().alias("households")).sort("wave")
    print(f"\n  rows per wave: {dict(zip(per_wave['wave'], per_wave['households']))}")


def main() -> int:
    sys.stdout.reconfigure(encoding="utf-8")
    print("=" * 74)
    print("STEP 05 — cross-wave household bridge")
    print("=" * 74)
    df = build()
    if df.is_empty():
        print("nothing built — run step01_household.py first", file=sys.stderr)
        return 1

    out = OUTPUT / "panel_bridge.parquet"
    df.write_parquet(out)
    _diagnostics(df)
    print(f"\n  -> {out}  ({len(df):,} rows x {len(df.columns)} cols)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
