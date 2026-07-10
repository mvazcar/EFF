"""
Readers for the extracted EFF data files.

One reader per file type, resolving the right file for a (wave, implicate) and coping with two
decades of Banco de España conventions.

Why .dta and not .csv
---------------------
The two builds carry identical values, but only the .dta carries variable labels and value
labels. Those labels are not decoration: they are the only machine-readable statement of what
a column such as `p6_13_4_2` means, and section 6's member reshape below is *validated* against
them. `read_dta` therefore returns the labels alongside the frame. Pass `fmt="csv"` if you
only downloaded the csv build; the label maps come back empty.

Reading .csv correctly needs three non-defaults: the separator is `;`, the encoding is latin-1
(not utf-8 — accented Spanish labels in the header would mojibake), and dtype inference must be
off. A column that looks integer in the first N rows can hold a special code further down, so
every column is read as Utf8 and then cast losslessly: it becomes Int64/Float64 only if the
cast introduces no new nulls, else it stays Utf8. This is the same discipline as ECV's readers.

The member index is the FIRST suffix, not the last
--------------------------------------------------
The User Guide says `ps_nn_m` has "position m ... when question ps_nn is asked several times.
For example, when the same question is asked to each household member." The "for example" is
load-bearing: `m` is a repetition index whose meaning depends on the question.

  p1_1_1      section 1, question 1, MEMBER 1        -> sex of member 1
  p1_12_1     section 1, question 12, FIRST CHILD    -> age of the 1st child living outside
  p2_52_2_1   section 2, question 52, SECOND PROPERTY, first loan
  p6_13_4_2   section 6, question 13, MEMBER 4, second employee job

So a blanket "reshape every `_<d>` column to member-long" silently mixes members with
properties, loans and children. It is wrong for `otras_secciones` and right only for section 6.

Section 6 is uniformly member-indexed, and the member is the first index after the question
number and any multiple-answer code:

    p6_<q>[a-z]? ([cszv]<k>)? _<member> (_<repetition>)?

That claim is checked, not assumed. Across the eight waves the regex classifies 1,458-2,205
columns per wave into exactly nine equal member blocks, and for the seven waves that ship
Spanish labels the captured member index agrees with the "Miem. N." in the label for every
single column: 11,330 labelled columns checked, 0 disagreements. 2002 ships English labels
("v0643c: employee") with no member marker, so there the name rule stands alone — but its member
blocks are still exactly uniform (245 columns x 9).

`section6_members()` uses that rule, and drops the padding: a household with p1=3 members has
columns for members 4..9 filled with missing, and those rows must not become observations.
"""
from __future__ import annotations

import functools
import re
from pathlib import Path

import polars as pl

from config import (
    CSV_ENCODING, CSV_SEPARATOR, DEFAULT_FORMAT, IMPLICATES, MAX_MEMBERS, MEMBER_COUNT,
    REPLICATE_SETS, WAVES, core_basenames, derived_basenames, hh_key, raw_dir,
    replicate_basename, shadow_basename,
)

# Section-6 column grammar. Group 3 is the household member, 1..9.
SECTION6_COL = re.compile(r"^p6_(\d+[a-z]?)((?:[cszv]\d+)?)_([1-9])(?:_(\d+))?$")

# A label such as "p.6.13. Miem. 4. Cta aje 2. Tipo de contrato" or "p1.1.1. Miembro 1. Sexo".
MEMBER_LABEL = re.compile(r"(?i)\bmiem(?:bro)?\.?\s*(\d)\b")


class EFFFileNotFound(FileNotFoundError):
    pass


# ── file resolution ───────────────────────────────────────────────────────
def resolve(wave: int, basename: str) -> Path:
    """Path of one extracted data file, matched case-insensitively."""
    d = raw_dir(wave)
    if not d.exists():
        raise EFFFileNotFound(f"{d} does not exist — run download.py && unpack.py")
    have = {p.name.lower(): p for p in d.iterdir() if p.is_file()}
    p = have.get(basename.lower())
    if p is None:
        raise EFFFileNotFound(f"{basename} not in {d}; found {sorted(have)[:8]}")
    return p


# ── low-level readers ─────────────────────────────────────────────────────
def _autocast(df: pl.DataFrame) -> pl.DataFrame:
    """
    Cast each Utf8 column to the narrowest type that loses nothing.

    A cast is accepted only when it produces no new nulls, so a column holding a stray
    non-numeric code anywhere in the file stays Utf8 rather than silently nulling those rows.
    """
    out = []
    for name, dtype in df.schema.items():
        col = pl.col(name)
        if dtype != pl.Utf8:
            out.append(col)
            continue
        s = df[name]
        nn = s.len() - s.null_count()
        as_i = s.cast(pl.Int64, strict=False)
        if as_i.len() - as_i.null_count() == nn:
            out.append(col.cast(pl.Int64, strict=False))
            continue
        as_f = s.cast(pl.Float64, strict=False)
        if as_f.len() - as_f.null_count() == nn:
            out.append(col.cast(pl.Float64, strict=False))
            continue
        out.append(col)
    return df.select(out)


def read_csv(path: Path) -> tuple[pl.DataFrame, dict, dict]:
    df = pl.read_csv(
        path,
        separator=CSV_SEPARATOR,
        has_header=True,
        infer_schema=False,               # everything as Utf8; see _autocast
        null_values=["", "."],
        encoding="utf8-lossy",
        truncate_ragged_lines=True,
    )
    df.columns = [c.strip().strip('"') for c in df.columns]
    df = df.with_columns(
        pl.col(n).str.strip_chars().replace("", None)
        for n, t in df.schema.items() if t == pl.Utf8
    )
    return _autocast(df), {}, {}


def read_dta(path: Path) -> tuple[pl.DataFrame, dict[str, str], dict[str, dict]]:
    """Returns (frame, variable_labels, value_labels). Needs pyreadstat."""
    import pyreadstat

    pdf, meta = pyreadstat.read_dta(str(path), apply_value_formats=False)
    df = pl.from_pandas(pdf)
    var_labels = {c: (lab or "") for c, lab in zip(meta.column_names, meta.column_labels)}
    val_labels = {
        col: meta.value_labels.get(fmt, {})
        for col, fmt in meta.variable_to_label.items()
        if fmt in meta.value_labels
    }
    return df, var_labels, val_labels


def read_file(wave: int, basename: str, fmt: str = DEFAULT_FORMAT):
    path = resolve(wave, basename.rsplit(".", 1)[0] + f".{fmt}")
    return read_dta(path) if fmt == "dta" else read_csv(path)


# ── high-level readers ────────────────────────────────────────────────────
def read_other(wave: int, imp: int, fmt: str = DEFAULT_FORMAT):
    """otras_secciones — every questionnaire section except 6, one row per household."""
    return read_file(wave, core_basenames(wave, imp, fmt)["other"], fmt)


def read_section6(wave: int, imp: int, fmt: str = DEFAULT_FORMAT):
    """seccion6 — labour and income, one row per household, columns suffixed by member."""
    return read_file(wave, core_basenames(wave, imp, fmt)["section6"], fmt)


def read_shadow(wave: int, fmt: str = DEFAULT_FORMAT):
    """sombra — one row per household, j<var> telling whether <var> was imputed and why.
    Common to all five implicates (the imputation flags do not vary by implicate)."""
    return read_file(wave, shadow_basename(wave, fmt), fmt)


def read_derived(wave: int, imp: int, fmt: str = DEFAULT_FORMAT):
    """databol<imp> — the constructed variables behind BdE's own published tables."""
    return read_file(wave, derived_basenames(wave, fmt)[imp - 1], fmt)


def read_replicate(wave: int, kind: str = "replicate_weights", fmt: str = DEFAULT_FORMAT):
    """
    One replicate-weight file. `kind` is a key of config.REPLICATE_SETS.

    An `hh_id` alias is added alongside the wave's own key (`h_2022`, or `h_number` in 2002), so
    that the frame joins onto anything this pipeline produces without the caller having to know
    which wave renamed the identifier.
    """
    if kind not in REPLICATE_SETS:
        raise ValueError(f"kind must be one of {sorted(REPLICATE_SETS)}, got {kind!r}")
    df, var_labels, val_labels = read_file(wave, replicate_basename(kind, wave, fmt), fmt)
    key = hh_key(wave)
    if key in df.columns and "hh_id" not in df.columns:
        df = df.with_columns(pl.col(key).cast(pl.Int64).alias("hh_id"))
    return df, var_labels, val_labels


@functools.lru_cache(maxsize=None)
def n_replicates(wave: int, kind: str = "replicate_weights", fmt: str = DEFAULT_FORMAT) -> int:
    """
    How many bootstrap replicates a wave ships, read off the file rather than assumed.

    It is 999 in 2002 and 1000 in 2022; the User Guides that state it disagree with each other
    for 2014 (1000 in section 1.2, 999 in section 6), so a hardcoded constant would be a
    coin flip. Counting columns costs one header read.
    """
    cols = header(wave, replicate_basename(kind, wave, fmt), fmt)
    prefix = REPLICATE_SETS[kind][0]
    return sum(c.startswith(prefix) for c in cols)


def header(wave: int, basename: str, fmt: str = DEFAULT_FORMAT) -> list[str]:
    """Column names of a file without materialising its body."""
    path = resolve(wave, basename.rsplit(".", 1)[0] + f".{fmt}")
    if fmt == "csv":
        with open(path, "r", encoding=CSV_ENCODING) as f:
            return [c.strip().strip('"') for c in f.readline().rstrip("\r\n").split(CSV_SEPARATOR)]
    import pyreadstat
    _, meta = pyreadstat.read_dta(str(path), metadataonly=True)
    return list(meta.column_names)


# ── implicate stacking ────────────────────────────────────────────────────
def stack_implicates(wave: int, part: str = "other", fmt: str = DEFAULT_FORMAT,
                     implicates=IMPLICATES) -> tuple[pl.DataFrame, dict[str, str], dict[str, dict]]:
    """
    Read all five implicates of one file and stack them, tagging each with `implicate`.

    This is the single dataset BdE's own Stata/R/Python examples build before doing anything
    else, and the layout `mi import flong` expects. Estimation must still be done implicate by
    implicate and combined with Rubin's rules (mi.py) — stacking is a storage convention, not a
    licence to treat 5N rows as N independent observations. The one exception BdE sanctions is
    a mean or share, where dividing facine3 by five over the stacked frame gives the MI point
    estimate directly (but not its standard error).
    """
    reader = {"other": read_other, "section6": read_section6, "derived": read_derived}[part]
    frames, var_labels, val_labels = [], {}, {}
    for i in implicates:
        df, vl, vv = reader(wave, i, fmt)
        frames.append(df.with_columns(pl.lit(i, dtype=pl.Int8).alias("implicate")))
        var_labels = var_labels or vl
        val_labels = val_labels or vv
    out = pl.concat(frames, how="vertical_relaxed")
    return out, var_labels, val_labels


# ── section 6: household-wide -> member-long ──────────────────────────────
def section6_member_map(columns: list[str]) -> dict[str, list[tuple[str, int]]]:
    """
    Group section-6 columns into member-indexed families.

    Returns {stem: [(column, member), ...]}, where `stem` is the column name with the member
    index removed — the name the variable will have in the member-long frame.
    """
    fams: dict[str, list[tuple[str, int]]] = {}
    for c in columns:
        m = SECTION6_COL.match(c)
        if not m:
            continue
        q, multi, member, rep = m.groups()
        stem = f"p6_{q}{multi}" + (f"_{rep}" if rep else "")
        fams.setdefault(stem, []).append((c, int(member)))
    return fams


def check_member_map(columns: list[str], var_labels: dict[str, str]) -> tuple[int, int, list[str]]:
    """
    Validate the member rule against the Stata labels.

    Returns (agreements, disagreements, examples). Waves whose labels are English (2002) yield
    0 agreements and 0 disagreements — the rule is then unverifiable, not violated.
    """
    agree = disagree = 0
    examples: list[str] = []
    for c in columns:
        m = SECTION6_COL.match(c)
        if not m:
            continue
        lm = MEMBER_LABEL.search(var_labels.get(c, "") or "")
        if lm is None:
            continue
        if lm.group(1) == m.group(3):
            agree += 1
        else:
            disagree += 1
            if len(examples) < 5:
                examples.append(f"{c} :: {var_labels.get(c, '')[:70]}")
    return agree, disagree, examples


def section6_members(wave: int, imp: int, fmt: str = DEFAULT_FORMAT,
                     n_members: pl.DataFrame | None = None) -> pl.DataFrame:
    """
    Reshape one implicate of section 6 into one row per (household, member).

    `n_members` is an optional frame with the household key and `p1` (the member count) taken
    from otras_secciones; when given, members beyond p1 are dropped. Without it the frame keeps
    all nine member slots, most of which are entirely missing for small households.

    The truncation runs the other way too, and the survey does it, not this code: `p1` reaches 10
    in 2002 and 11 in 2022, but section 6 has only nine member slots. Members 10 and 11 of those
    households were never recorded. It affects at most two households per wave, so the member
    table's mean household size (2.5272 in 2022) sits a few ten-thousandths below `mean(p1)`
    (2.5276). step02_members.py reports the count rather than letting it pass unnoticed.
    """
    df, var_labels, _ = read_section6(wave, imp, fmt)
    key = hh_key(wave)
    fams = section6_member_map(df.columns)

    agree, disagree, examples = check_member_map(df.columns, var_labels)
    if disagree:
        raise ValueError(
            f"section 6 member rule violated in {wave} imp{imp}: {disagree} columns whose "
            f"label names a different member than their name does. Examples: {examples}"
        )

    # Household-level section-6 columns (p6_59c1, p6_60_frec, ...) are carried on every row.
    member_cols = {c for fam in fams.values() for c, _ in fam}
    hh_cols = [c for c in df.columns if c not in member_cols and c != key]

    frames = []
    for member in range(1, MAX_MEMBERS + 1):
        sel = {stem: col for stem, fam in fams.items() for col, m in fam if m == member}
        if not sel:
            continue
        frames.append(
            df.select([pl.col(key), *[pl.col(c) for c in hh_cols],
                       *[pl.col(c).alias(stem) for stem, c in sel.items()]])
              .with_columns(pl.lit(member, dtype=pl.Int8).alias("member"))
        )
    long = pl.concat(frames, how="diagonal_relaxed").with_columns(
        pl.lit(imp, dtype=pl.Int8).alias("implicate")
    )

    if n_members is not None:
        long = (long.join(n_members.select([key, MEMBER_COUNT]), on=key, how="inner")
                    .filter(pl.col("member") <= pl.col(MEMBER_COUNT)))
    return long.sort(key, "member")


# ── diagnostics ───────────────────────────────────────────────────────────
def available(fmt: str = DEFAULT_FORMAT) -> dict[int, list[str]]:
    """Which waves currently have their files extracted."""
    out: dict[int, list[str]] = {}
    for wave in WAVES:
        got = []
        for part, base in (("other", core_basenames(wave, 1, fmt)["other"]),
                           ("section6", core_basenames(wave, 1, fmt)["section6"]),
                           ("shadow", shadow_basename(wave, fmt)),
                           ("derived", derived_basenames(wave, fmt)[0])):
            try:
                resolve(wave, base)
                got.append(part)
            except EFFFileNotFound:
                pass
        if got:
            out[wave] = got
    return out
