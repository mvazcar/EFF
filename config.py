"""
Configuration for the EFF (Encuesta Financiera de las Familias) pipeline.

Mirrors the structure of mvazcar/MCVL, mvazcar/PanelHogares and mvazcar/ECV — a single
config.py, a readers.py of wave-aware readers, numbered stepNN_*.py modules writing parquet
checkpoints, a pipeline.py orchestrator and a thin run.py CLI.

What the EFF is
---------------
Banco de España's triennial (biennial from 2020) survey of household finances. Eight waves:
2002, 2005, 2008, 2011, 2014, 2017, 2020, 2022. The sampling unit is the HOUSEHOLD, wealthy
households are oversampled from a wealth-tax stratum, and the survey is an overlapping panel.

Three features make the EFF unlike the other pipelines in this family, and they drive the design:

  1. MULTIPLE IMPUTATION. Item non-response is imputed five times. Each wave ships FIVE
     complete data sets ("implicates"). A point estimate is the average of the five; its
     variance needs Rubin's rules (see mi.py). Analysing implicate 1 alone is wrong.

  2. REPLICATE WEIGHTS. Stratum and cluster identifiers are withheld for confidentiality.
     Instead each wave ships 999 (2002) or 1000 bootstrap replicate weights, which are the
     ONLY way to get sampling variances that respect the design.

  3. NO PUBLIC DOWNLOAD for most waves. Microdata is released "para uso científico" behind a
     registration wall: you give an email at
       https://app.bde.es/gnt_seg/controlAccesoEmail.jsp?pas=eff&lang=es&p1=<wave>
     and Banco de España mails back a one-time link that opens a session on pas.bde.es.
     download.py can drive that session — see EFF_ACCESS_URL below. Waves 2017 and 2020 are,
     inconsistently, served from the public www.bde.es file server and need no session at all.

Access conditions (from the registration form, paraphrased): research use only, no transfer to
third parties, no commercial use, cite the source but do not implicate Banco de España.

File model, per wave
--------------------
  eff_<year>_imp<i>_<ext>.zip     the i-th implicate, i = 1..5
      otras_secciones_<year>_imp<i>.<ext>   every questionnaire section EXCEPT 6
      seccion6_<year>_imp<i>.<ext>          section 6 (labour and income, per member)
      etiquetas_*.do                        Stata label programs (csv zips only)
  sombra_<year>_<ext>.zip         shadow variables: which values were imputed, and why
  replicate_weights_<year>_<ext>.zip
      replicate_weights_<year>.<ext>        cross-section replicates  wt3r_1..R, ntimesr_1..R
      replicate_pan1weights_<year>.<ext>    panel replicates, previous-wave population
      replicate_pan2weights_<year>.<ext>    panel replicates, current-wave population
  databol_<year>_<ext>.zip        databol1..5: the constructed variables behind BdE's own tables
  definitions_<year>.doc[x]       Word definitions of those constructed variables

Both `dta` (Stata) and `csv` (semicolon-delimited, dot decimal) are published, with identical
content. The .dta carries variable and value labels; the .csv carries none, and ships the
labels as Stata .do programs instead. This pipeline reads .dta by default for that reason.

Wave quirks (all observed in the delivered archives, not inferred from the User Guides)
---------------------------------------------------------------------------------------
  2002   household id is `h_number`, NOT `h_2002`.
  2002   the zip is named `effe_2002_imp<i>_<ext>.zip` on the Spanish page too, and its inner
         files are the ENGLISH build: `other_sections_2002_imp<i>` / `section6_2002_imp<i>`,
         with English variable labels. Every other wave's Spanish zip is `eff_<year>_...`
         containing `otras_secciones_` / `seccion6_`.
  2002   999 replicate weights, not 1000. No panel replicate weights (it is the first wave).
  2002   no panel variables at all: no hogarpanel, no pesopan_1/2, no pan_1..9.
  2008   additionally ships the ECB HFCS "UDB version 1.8" release (imp<i>_version1.8_*.zip,
         W_version1.8_*.zip, labels.zip). Different variable names; not part of this pipeline.
  2002-2011  ship an alternative weight set calibrated to the 2001 Census
         (`*_census2001.zip`). The default files use the 2011-Census base. Do not mix them.
  2002-2014  the English-labelled downloads carry an `_en` suffix (sombra_2002_dta_en.zip);
         from 2017 the sombra / replicate / databol zips are language-neutral.
  2017-2020  served from the PUBLIC www.bde.es/f/ path; all other waves need the session.
  2002-2014  definitions ship as .doc; 2017+ as .docx.
  databol   the constructed household income variable is renamed in EVERY wave, and rebased:
         `renthog` (2002), `renthog04_€05`, `renthog07_€09`, `renthog10_€11`,
         `renthog13_eur14`, `renthog16_eur17`, `renthog19_eur20`, `renthog21_eur22`. Only 2002
         carries the bare name. The `€` is literal, and the 2008 wave's income is in 2009 euros.
         (`otras_secciones` does carry a stable `renthog` — a different, nominal, quantity.)
         See harmonise.income_column.

Identifiers and linkage
-----------------------
Every wave from 2005 on carries BOTH its own household id and the PREVIOUS wave's id, so the
whole 2002-2022 history chains pairwise. The User Guides describe only the adjacent link, and
the 2008 guide is silent about it, but `h_2005` is present in the 2008 file: the chain is
unbroken. There is no single stable household key — step05_panel.py builds one.
"""
from __future__ import annotations

import os
import re
from pathlib import Path

# ── Paths ──────────────────────────────────────────────────────────────────
# Machine-specific paths are overridable by environment variable so the repo runs
# unchanged anywhere:
#   EFF_PROJECT     repo root                          [default: this file's dir]
#   EFF_RAW_DIR     scratch dir for extracted data     [default: <root>/raw]
#   EFF_WAVES       restrict the waves                 [also set by run.py --waves]
#   EFF_ACCESS_URL  the one-time link BdE emails you   [required by download.py, gated waves]
ROOT       = Path(os.environ.get("EFF_PROJECT", Path(__file__).resolve().parent))
RAW_ZIPPED = ROOT / "raw_zipped"                                # archives AS RECEIVED, flat
RAW_DIR    = Path(os.environ.get("EFF_RAW_DIR", ROOT / "raw"))  # extracted .dta/.csv (unpack.py)
TEMP_DIR   = ROOT / "temp"                                      # per-step parquet checkpoints
OUTPUT     = ROOT / "output"                                    # final parquets
CATALOG    = ROOT / "catalog"                                   # schemas / labels / inventory
DOCS       = ROOT / "docs"                                      # definitions_*.doc[x], cuadros pdf
LOG_DIR    = ROOT / "logs"
for _d in (RAW_ZIPPED, RAW_DIR, TEMP_DIR, OUTPUT, CATALOG, DOCS, LOG_DIR):
    try:
        _d.mkdir(parents=True, exist_ok=True)
    except OSError:
        pass

# ── Source ─────────────────────────────────────────────────────────────────
PUBLIC_HOST  = "https://www.bde.es"     # serves the 2017 and 2020 archives, and all the PDFs
PRIVATE_HOST = "https://pas.bde.es"     # serves the rest, behind the emailed session
LANDING      = f"{PRIVATE_HOST}/privbde/es/pas/eff-datos/"

# The gate that mails you the access link. p1 selects the wave you are granted.
ACCESS_FORM = "https://app.bde.es/gnt_seg/controlAccesoEmail.jsp?pas=eff&lang=es&p1={wave}"

# The emailed link. Never commit it: it embeds your email and an auth ticket.
ACCESS_URL = os.environ.get("EFF_ACCESS_URL")

# ── Waves ──────────────────────────────────────────────────────────────────
# ALL_WAVES is the survey; WAVES is what this run processes. Only WAVES is narrowed by
# EFF_WAVES / `run.py --waves`, so wave_of() below keeps resolving filenames of waves the
# current run happens to skip.
ALL_WAVES = [2002, 2005, 2008, 2011, 2014, 2017, 2020, 2022]
WAVES = list(ALL_WAVES)

# Waves whose archives sit on the public file server (no session needed).
PUBLIC_WAVES = {2017, 2020}

IMPLICATES = (1, 2, 3, 4, 5)          # every wave imputes five times
N_IMPLICATES = len(IMPLICATES)

# Optional wave override (set by run.py --waves; propagates to step subprocesses)
_wenv = os.environ.get("EFF_WAVES")
if _wenv:
    _sel = {int(w) for w in _wenv.replace(",", " ").split()}
    WAVES = [w for w in WAVES if w in _sel]

DEFAULT_FORMAT = "dta"                # .dta carries the labels; .csv does not
FORMATS = ("dta", "csv")

# ── Identifiers ────────────────────────────────────────────────────────────
def hh_key(wave: int) -> str:
    """Household identifier column. 2002 predates the h_<year> convention."""
    return "h_number" if wave == 2002 else f"h_{wave}"


# The previous wave whose id each wave carries. Verified column-by-column in the .dta files:
# 2008 does carry h_2005, so the chain 2002-2022 is complete.
PREV_WAVE = {2005: 2002, 2008: 2005, 2011: 2008, 2014: 2011, 2017: 2014, 2020: 2017, 2022: 2020}


def prev_key(wave: int) -> str | None:
    """Column holding the PREVIOUS wave's household id, or None for 2002."""
    p = PREV_WAVE.get(wave)
    return None if p is None else hh_key(p)


MEMBER_COUNT = "p1"     # number of household members. Reaches 10 in 2002 and 11 in 2022.
MAX_MEMBERS = 9         # ... but section 6 has only nine member slots, so members 10 and 11 of
                        # those (one or two per wave) households were never recorded. Not a cap
                        # this code imposes: see readers.section6_members.

# ── Weights ────────────────────────────────────────────────────────────────
XSEC_WEIGHT = "facine3"               # cross-sectional household weight, every wave
PANEL_WEIGHTS = ("pesopan_1", "pesopan_2")   # calibrated to prev-wave / current-wave population
PANEL_FLAG = "hogarpanel"             # 1 if the household was also in the previous wave
MEMBER_LINK = tuple(f"pan_{i}" for i in range(1, MAX_MEMBERS + 1))

# Replicate-weight column prefixes. R is 999 in 2002 and 1000 from 2005 on -- but only 2002 and
# 2022 were counted directly, so readers.n_replicates() reads it off the file rather than
# trusting a constant. The two User Guides that state it contradict each other for 2014.
REPLICATE_SETS = {
    # file stem                        weight prefix     multiplicity prefix
    "replicate_weights":      ("wt3r_",     "ntimesr_"),
    "replicate_pan1weights":  ("wtpan1r_",  "ntimespan1r_"),
    "replicate_pan2weights":  ("wtpan2r_",  "ntimespan2r_"),
}
N_REPLICATES_OBSERVED = {2002: 999, 2022: 1000}    # counted in the delivered headers

# ── Shadow variables ───────────────────────────────────────────────────────
# Every questionnaire variable p... has a shadow twin j... recording the original state.
SHADOW_PREFIX = ("p", "j")            # p2_5 -> j2_5
SHADOW_CODES = {
    1: "complete observation",
    0: "true missing, implied by an earlier answer",
    2050: "imputed: answer was 'Don't know'",
    2051: "imputed: answer was 'No answer'",
    2052: "imputed: no answer to preceding variables",
    2053: "imputed: answered but incorrect",
    2055: "imputed: not asked, CAPI or interviewer error",
}
IMPUTED_FROM = 2050                   # shadow >= 2050 means the value was imputed


# ── Archive naming ─────────────────────────────────────────────────────────
def imputed_zip(wave: int, imp: int, ext: str = DEFAULT_FORMAT) -> str:
    """Spanish-build implicate archive. 2002 alone uses the `effe_` prefix."""
    stem = "effe" if wave == 2002 else "eff"
    return f"{stem}_{wave}_imp{imp}_{ext}.zip"


def core_basenames(wave: int, imp: int, ext: str = DEFAULT_FORMAT) -> dict[str, str]:
    """Inner data files of one implicate archive. 2002 ships the English build."""
    if wave == 2002:
        return {"other": f"other_sections_{wave}_imp{imp}.{ext}",
                "section6": f"section6_{wave}_imp{imp}.{ext}"}
    return {"other": f"otras_secciones_{wave}_imp{imp}.{ext}",
            "section6": f"seccion6_{wave}_imp{imp}.{ext}"}


def shadow_basename(wave: int, ext: str = DEFAULT_FORMAT) -> str:
    return f"sombra_{wave}.{ext}"


def derived_basenames(wave: int, ext: str = DEFAULT_FORMAT) -> list[str]:
    """databol1..databol5 — note the filenames carry no year, so they must not share a dir."""
    return [f"databol{i}.{ext}" for i in IMPLICATES]


def replicate_basename(kind: str, wave: int, ext: str = DEFAULT_FORMAT) -> str:
    return f"{kind}_{wave}.{ext}"


def raw_dir(wave: int) -> Path:
    """Directory holding one wave's extracted data files: raw/2022/"""
    return RAW_DIR / str(wave)


# raw_zipped/ holds the archives exactly as Banco de España serves them, in one flat directory
# (the MCVL convention). Every filename carries its wave, so unpack.py can route them without a
# manifest — with two classes of exception, both handled below rather than by a nested layout.
_YEAR_RE = re.compile(r"(20\d{2})")

# The 2008 wave alone also ships the ECB HFCS "UDB version 1.8" re-release. Those filenames carry
# no year at all (`imp1_version1.8_dta.zip`, `W_version1.8_csv.zip`, `labels.zip`,
# `UDB1-HFCSdescription.pdf`, `Notes_comparability_EFF_HFCS.doc`). They are only ever published on
# the 2008 page, so the mapping is a fact about the delivery, not a guess.
HFCS_ONLY_WAVE = 2008
_HFCS_RE = re.compile(r"(version1\.8|^labels(_en)?\.zip$|UDB1|Notes_comparability)", re.I)


def wave_of(filename: str) -> int | None:
    """
    Which wave an archive in raw_zipped/ belongs to, from its name alone.

    `databol_2008_dta_census2001.zip` contains two four-digit years; 2001 is not a wave, so
    filtering the candidates against WAVES disambiguates it without a special case. Files whose
    wave cannot be established return None and unpack.py skips them, loudly.
    """
    if _HFCS_RE.search(filename):
        return HFCS_ONLY_WAVE
    cands = {int(y) for y in _YEAR_RE.findall(filename)} & set(ALL_WAVES)
    return cands.pop() if len(cands) == 1 else None


# ── Download groups ────────────────────────────────────────────────────────
# download.py classifies every asset on a wave page into exactly one group, so that
# `--groups imputed derived` fetches 20 MB instead of 700 MB of replicate weights.
# `labels` must be tested before `imputed`: the label .do programs ship inside the *first*
# implicate's csv archive, which also matches the `imputed` pattern.
GROUP_ORDER = ("census2001", "hfcs", "labels", "imputed", "shadow", "derived", "replicate", "docs")
GROUPS = {
    "labels":     r"^(eff|effe)_\d{4}_imp1_csv\.zip$",
    "imputed":    r"^(eff|effe)_\d{4}_imp\d_(dta|csv)\.zip$",
    "shadow":     r"^sombra_\d{4}_(dta|csv)\.zip$",
    "derived":    r"^databol_\d{4}_(dta|csv)\.zip$",
    "replicate":  r"^replicate_weights_\d{4}_(dta|csv)\.zip$",
    "census2001": r"_census2001(_en)?\.zip$",
    "hfcs":       r"(version1\.8|^labels\.zip$|UDB1|Notes_comparability)",
    "docs":       r"\.(pdf|docx?)$",
}
# Groups whose members exist in both builds, and so are filtered by --formats. `labels` is not
# among them: it is always the first implicate's csv archive, whatever build you asked for. That
# archive carries the etiquetas_*.do label programs (2011+ only) and gives labels.py a second,
# independent copy of the variable labels to diff the .dta against. It is also what a csv-only
# user needs. Neither build ships value labels — see labels.py.
FORMAT_FILTERED = ("imputed", "shadow", "derived", "replicate")

# Sensible default: everything needed to build the panel, minus the ~700 MB of replicate
# weights (add `--groups replicate` when you need design-correct standard errors) and minus
# the superseded 2001-Census weights and the HFCS re-release.
DEFAULT_GROUPS = ("imputed", "shadow", "derived", "labels", "docs")

# ── Reading conventions ────────────────────────────────────────────────────
# The .csv files are semicolon-delimited with a dot decimal separator and a header row.
# The .dta files are Stata 11+; pyreadstat reads them with variable and value labels.
CSV_SEPARATOR = ";"
CSV_ENCODING = "latin-1"              # BdE writes cp1252/latin-1, not utf-8
