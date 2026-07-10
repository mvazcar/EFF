"""
Harmonise EFF variables across waves.

Pooling EFF waves is not a matter of stacking frames. Three things move under you.

1. The household key is renamed every wave
------------------------------------------
`h_number` in 2002, then `h_2005`, `h_2008`, ... `h_2022`. Every wave from 2005 on also carries
the PREVIOUS wave's key as a second column. This module maps both onto `hh_id` and `hh_id_prev`
so that a stacked frame has one identifier column, and `step05_panel.py` can chain them.

The ids are NOT comparable across waves by value. `h_2020 = 4` and `h_2022 = 4` are different
households. Only the explicit `h_<prev>` column links them.

2. The derived income variable is renamed every wave, and rebased
-----------------------------------------------------------------
`databol` is the file behind BdE's published tables. Its total-household-income column is:

      2002  renthog                nominal, base year undocumented
      2005  renthog04_€05          2004 income, 2005 euros
      2008  renthog07_€09          2007 income, 2009 euros    <- not 2008 euros
      2011  renthog10_€11          2010 income, 2011 euros
      2014  renthog13_eur14        2013 income, 2014 euros
      2017  renthog16_eur17        2016 income, 2017 euros
      2020  renthog19_eur20        2019 income, 2020 euros
      2022  renthog21_eur22        2021 income, 2022 euros

Note the `€` sign literally inside the 2005-2011 column names, replaced by `eur` from 2014.
That is an encoding trap for the csv build: `€` is not representable in ISO-8859-1, so a reader
that assumes latin-1 will mangle the header. The .dta build carries it as UTF-8 and pyreadstat
returns it intact.

More importantly, each wave's income is expressed in that wave's own euros (2009's, for the 2008
wave). So `riquezanet` and `renthog*` are NOT comparable across waves without deflating to a
common base. This module extracts the two years from the column name and records them; it does
not deflate, because that requires a price index this repo has no business inventing. Use
`deflate()` with an index you have chosen and can cite.

`otras_secciones` also carries a `renthog`, in every wave, under that stable name. It is the
NOMINAL total household income of the reference year — not the deflated one in `databol`. The
two differ. `income_column()` is about `databol`.

3. The questionnaire is revised every wave
------------------------------------------
2,148 of 4,142 variable names appear in all eight waves (catalog/presence.csv), but a shared
name is not a promise of a shared meaning: the occupation coding moved from CNO-1994 to CNO-2011
between 2008 and 2011, so `p6_3_<m>` means different things either side of that break.

This module therefore harmonises only what it can verify: the identifiers, the wave metadata,
and the derived-variable core of `databol` (55 columns present in all eight waves). Everything
else is passed through untouched and under its original name. That is deliberate: an EFF column
this code has not checked should look unharmonised, so you check it yourself.
"""
from __future__ import annotations

import re

import polars as pl

from config import PANEL_FLAG, WAVES, XSEC_WEIGHT, hh_key, prev_key

# `renthog04_€05`, `renthog13_eur14`. Two-digit income year, two-digit euro base year.
INCOME_RE = re.compile(r"^renthog(\d{2})_(?:€|eur)(\d{2})$", re.IGNORECASE)

# The 54 derived variables of `databol` present in ALL eight waves, and meaning the same thing in
# each because BdE constructs them itself for the published tables. This is the exact set
# intersection of the eight column lists, not a hand-picked selection. `p2_71` looks like it
# belongs and does not: the 2008 wave omits it. `renthog*` is excluded because its name and its
# euro base both move (see income_column). Keys and facine3 are re-added by harmonise_derived.
DERIVED_CORE = [
    "actfinanc", "actreales", "adeuda", "alim", "allf", "bage", "cuentas", "deuhipv",
    "deuoprop", "dpdte", "dpdtehipo", "dvivpral", "gimpvehic", "gvehic", "havenegval", "hipo",
    "neducdom", "nnumadtrab", "nodur", "np1", "np2_1", "np4_18", "np4_5", "nsitlabdom",
    "odeuhog", "otrasd", "otraspr", "p2_69", "p2_70", "p2_84", "p4_15", "p4_24", "p4_35",
    "p4_7_3", "pagodeuda", "penseg", "percrent", "percriq", "perso", "phipo", "potrasd",
    "pperso", "riquezabr", "riquezanet", "salcuentas", "sideuda", "tiene", "tienefin",
    "tienereal", "timpvehic", "tvehic", "valhog", "valpenseg", "vdeuda",
]

# Breakdown variables and their code lists, as used by BdE's own published Python example.
# These are the only value labels this repo can state, because they are the only ones BdE has
# ever published in machine-readable form. Everything else: read the questionnaire.
BREAKDOWNS = {
    "bage":       {1: "Under 35", 2: "35-44", 3: "45-54", 4: "55-64", 5: "65-74", 6: "Over 75"},
    "percrent":   {1: "< P20", 2: "P20-P40", 3: "P40-P60", 4: "P60-P80", 5: "P80-P90", 6: "> P90"},
    "percriq":    {1: "< P25", 2: "P25-P50", 3: "P50-P75", 4: "P75-P90", 5: "> P90"},
    "nsitlabdom": {1: "Employee", 2: "Self-employed", 3: "Retired",
                   4: "Other inactive or unemployed"},
    "neducdom":   {1: "Below secondary", 2: "Secondary", 3: "University"},
    "np2_1":      {1: "Ownership", 2: "Other"},
    "nnumadtrab": {0: "None", 1: "One", 2: "Two", 3: "Three or more"},
    # BdE's example maps only np1 == 5 -> "5 or more"; the lower codes are literal counts. The
    # column is top-coded at 5 in every wave (observed range 1..5), so spelling them all out is
    # faithful, and lets label_breakdown() use replace_strict without nulling codes 1..4.
    "np1":        {1: "One", 2: "Two", 3: "Three", 4: "Four", 5: "Five or more"},
}


def income_column(columns: list[str]) -> tuple[str | None, int | None, int | None]:
    """
    Find `databol`'s total-household-income column and decode its two years.

    Returns (column, income_year, euro_base_year). For 2002 the column is the bare `renthog`
    and neither year is encoded in the name, so both come back None rather than guessed.
    """
    for c in columns:
        m = INCOME_RE.match(c)
        if m:
            yy_income, yy_euro = (int(g) for g in m.groups())
            return c, 2000 + yy_income, 2000 + yy_euro
    return ("renthog", None, None) if "renthog" in columns else (None, None, None)


def canonical(df: pl.DataFrame, wave: int) -> pl.DataFrame:
    """
    Add `wave`, `hh_id` and (where it exists) `hh_id_prev`, without dropping the originals.

    The original `h_<year>` columns are kept: they are what the replicate-weight and shadow
    files join on, and silently removing them would break those joins.
    """
    key, pkey = hh_key(wave), prev_key(wave)
    out = df.with_columns(
        pl.lit(wave, dtype=pl.Int16).alias("wave"),
        pl.col(key).cast(pl.Int64).alias("hh_id"),
    )
    if pkey and pkey in df.columns:
        out = out.with_columns(pl.col(pkey).cast(pl.Int64).alias("hh_id_prev"))
    else:
        out = out.with_columns(pl.lit(None, dtype=pl.Int64).alias("hh_id_prev"))
    if PANEL_FLAG not in out.columns:
        # 2002 is the first wave: no household in it was in a previous one.
        out = out.with_columns(pl.lit(0, dtype=pl.Int8).alias(PANEL_FLAG))
    return out


def harmonise_derived(df: pl.DataFrame, wave: int) -> pl.DataFrame:
    """
    Reduce one wave's stacked `databol` to the cross-wave core.

    The income column is renamed to `renthog_eur` and its two years recorded as columns, so a
    pooled frame carries the information needed to deflate but is not silently deflated.
    """
    col, y_income, y_euro = income_column(df.columns)
    if col is None:
        raise ValueError(f"no renthog-like income column in databol {wave}: {df.columns[:8]}")

    keep = [c for c in DERIVED_CORE if c in df.columns]
    missing = [c for c in DERIVED_CORE if c not in df.columns]
    if missing:
        print(f"    {wave}: databol lacks {len(missing)} core columns: {missing[:6]}")

    out = canonical(df, wave).select(
        ["wave", "hh_id", "implicate", XSEC_WEIGHT, *keep,
         pl.col(col).cast(pl.Float64).alias("renthog_eur")]
    )
    return out.with_columns(
        pl.lit(y_income, dtype=pl.Int16).alias("income_year"),
        pl.lit(y_euro, dtype=pl.Int16).alias("euro_base_year"),
    )


def deflate(df: pl.DataFrame, columns: list[str], index: dict[int, float],
            base_year: int, year_col: str = "euro_base_year") -> pl.DataFrame:
    """
    Rebase monetary columns to `base_year` euros using a price index you supply.

    `index` maps year -> index level (any base). Rows whose `year_col` is null — the 2002 wave,
    whose euro base BdE never documented — are left as NaN rather than deflated by a guess.

    No price index ships with this repo. The obvious choice is INE's annual average CPI, but the
    right one depends on whether you are deflating income (CPI) or wealth (house prices, or
    nothing at all), and that is a modelling decision, not a data-cleaning one.
    """
    if base_year not in index:
        raise ValueError(f"base_year {base_year} is not in the supplied index")
    factor = pl.col(year_col).replace_strict(
        {y: index[base_year] / v for y, v in index.items()}, default=None, return_dtype=pl.Float64
    )
    return df.with_columns([(pl.col(c) * factor).alias(c) for c in columns])


def label_breakdown(df: pl.DataFrame, column: str) -> pl.DataFrame:
    """Attach BdE's published English category names to a breakdown column."""
    if column not in BREAKDOWNS:
        raise KeyError(f"{column} has no published code list; see cuestionario_<year>.pdf")
    mapping = {float(k): v for k, v in BREAKDOWNS[column].items()}
    return df.with_columns(
        pl.col(column).cast(pl.Float64).replace_strict(mapping, default=None)
        .alias(f"{column}_label")
    )


def income_columns_by_wave() -> dict[int, tuple[str | None, int | None, int | None]]:
    """Diagnostic: what `income_column` finds in each wave's databol. Needs the data unpacked."""
    from readers import header

    out = {}
    for wave in WAVES:
        try:
            out[wave] = income_column(header(wave, "databol1.dta"))
        except FileNotFoundError:
            pass
    return out
