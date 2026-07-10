"""
Extract the EFF variable labels into a machine-readable catalogue, and cross-check the two
places Banco de España stores them.

What the EFF actually ships
---------------------------
Variable labels — "p.6.13. Miem. 4. Cta aje 2. Tipo de contrato laboral" — are distributed
twice, identically:

  * inside the .dta files, as Stata variable labels;
  * as `label var` lines in `etiquetas_otras_secciones_<year>.do` and
    `etiquetas_seccion6_<year>.do`, which are packed inside `eff_<year>_imp1_csv.zip`.

Value labels — the code lists, 1 = "Indefinido", 2 = "Temporal" — are distributed NOWHERE.
Not in the .dta (every wave reports zero value-label sets) and not in the .do (which contain
`label var` lines and nothing else: zero `label define`, zero `label values`). This is worth
stating plainly because it is the opposite of what the ECV and Panel de Hogares deliveries do,
where the "Diseño de Registro" workbooks carry full code lists. To decode an EFF categorical
you must read `cuestionario_<year>.pdf` or `definitions_<year>.doc`, both prose documents.
`harmonise.py` therefore writes its recodes out by hand, with the questionnaire page cited.

(2008's `labels.zip` is not a counterexample. It labels the ECB HFCS "UDB version 1.8"
re-release of the 2008 wave, whose variables are named `DA1110` and the like, not the EFF's.)

The .do files exist for 2011-2022 only. The 2002, 2005 and 2008 csv archives hold the two data
files and nothing else. Since the .dta carries the same labels, nothing is lost — which `main()`
verifies rather than assumes, by diffing the two sources wave by wave.

Where the two disagree, the .dta wins
-------------------------------------
The diff is not empty, and the differences are not cosmetic. Three kinds, all real:

  1. Copy-paste errors in the .do. 2020's `p4_103s1_2`, `_3`, `_4` are all labelled "Neg 1" in
     the .do; the .dta correctly labels them "Neg 2", "Neg 3", "Neg 4". 100 such labels in 2020.

  2. Substantive contradictions. For 2011's `p1_14o_1` (father's occupation) the .dta says
     "Clasificación a un dígito CNO-2011" and the .do says "a dos dígitos". These cannot both
     be true, and it changes how the variable is read. The data settles it: the column's
     non-missing values are 0..9 (plus a -3 code), so it is ONE digit and the .dta is right.
     Same for `p6_3o_*`. 15 such labels in 2011, 3 in 2014, 5 in 2017.

  3. Whitespace. 2022's `p4_6s1_1` differs only by a doubled space.

In every case checked against the data the .dta was correct, so `for_wave` prefers it, and
`main()` writes the full diff to catalog/label_disagreements.csv rather than hiding it.

There is no global missing-value sentinel
-----------------------------------------
Do not recode 96/97/98/99 or negatives wholesale. After imputation a missing value is simply
absent. The codes that look like sentinels are question-specific and documented only in the
questionnaire: 96 means "does not use this banking service" in `p8_30v3` and is an ordinary
household id in `h_2022`; -3 appears in 44 columns of the 2022 wave, 73 cells in all.

    python labels.py

Emits, into catalog/:
  variable_labels.json        wave -> {variable: label}   (.dta preferred)
  variables.csv               flat inventory for browsing / grep
  label_disagreements.csv     every variable whose .dta and .do labels differ
"""
from __future__ import annotations

import csv
import json
import re
import sys
from collections import Counter

from config import CATALOG, DEFAULT_FORMAT, WAVES, core_basenames
from readers import EFFFileNotFound, resolve

# BdE writes the abbreviated form: `label var h_2022  "Identificador de hogar"`.
# Stata allows `label variable` too, so accept both.
DO_VAR = re.compile(r'^\s*label\s+var(?:iable)?\s+(\S+)\s+"(.*)"\s*$', re.M)
# Present only defensively: no delivered .do has ever contained these.
DO_DEFINE = re.compile(r"^\s*label\s+def(?:ine)?\s+(\S+)\s+(.*)$", re.M)
DO_ASSIGN = re.compile(r"^\s*label\s+val(?:ues)?\s+(\S+)\s+(\S+)\s*$", re.M)


def from_dta(wave: int) -> dict[str, str]:
    """Variable labels carried by the two core .dta files of implicate 1."""
    import pyreadstat

    out: dict[str, str] = {}
    for part in ("other", "section6"):
        try:
            path = resolve(wave, core_basenames(wave, 1, "dta")[part])
        except EFFFileNotFound:
            continue
        _, meta = pyreadstat.read_dta(str(path), metadataonly=True)
        for c, lab in zip(meta.column_names, meta.column_labels):
            if lab:
                out[c] = lab
    return out


def from_do(wave: int) -> dict[str, str]:
    """Variable labels parsed from the etiquetas_*.do programs, when the wave ships them."""
    do_dir = CATALOG / "labels" / str(wave)
    if not do_dir.exists():
        return {}
    out: dict[str, str] = {}
    for do in sorted(do_dir.glob("*.do")):
        text = do.read_text(encoding="latin-1", errors="replace")
        for name, lab in DO_VAR.findall(text):
            out[name] = lab
    return out


def value_labels(wave: int) -> dict[str, dict]:
    """Any value labels the wave ships. Empirically always empty; kept so that a future
    labelled release is picked up rather than silently ignored."""
    import pyreadstat

    out: dict[str, dict] = {}
    for part in ("other", "section6"):
        try:
            path = resolve(wave, core_basenames(wave, 1, "dta")[part])
        except EFFFileNotFound:
            continue
        _, meta = pyreadstat.read_dta(str(path), metadataonly=True)
        for col, fmt in meta.variable_to_label.items():
            if fmt in meta.value_labels:
                out[col] = {str(k): v for k, v in meta.value_labels[fmt].items()}

    do_dir = CATALOG / "labels" / str(wave)
    if not out and do_dir.exists():
        defines: dict[str, dict[str, str]] = {}
        assigns: dict[str, str] = {}
        for do in sorted(do_dir.glob("*.do")):
            text = do.read_text(encoding="latin-1", errors="replace")
            for name, body in DO_DEFINE.findall(text):
                defines.setdefault(name, {}).update(dict(re.findall(r'(-?\d+)\s+"([^"]*)"', body)))
            assigns.update(dict(DO_ASSIGN.findall(text)))
        out = {v: defines[n] for v, n in assigns.items() if n in defines}
    return out


def for_wave(wave: int, fmt: str = DEFAULT_FORMAT) -> dict[str, str]:
    """Variable labels from whichever source the downloaded build carries."""
    dta = from_dta(wave) if fmt == "dta" else {}
    return {**from_do(wave), **dta}


def main() -> int:
    sys.stdout.reconfigure(encoding="utf-8")
    CATALOG.mkdir(parents=True, exist_ok=True)

    all_labels: dict[str, dict[str, str]] = {}
    n_values = Counter()
    disagreements: list[tuple] = []
    print(f"  {'wave':<6}{'.dta':>8}{'.do':>8}{'agree':>8}{'differ':>8}{'value labels':>14}")
    for wave in WAVES:
        try:
            dta, do = from_dta(wave), from_do(wave)
        except EFFFileNotFound as e:
            print(f"  {wave}: skipped ({e})")
            continue
        if not dta and not do:
            continue

        # Cross-check: where both sources name a variable, do they agree? The .dta wins — see
        # the module docstring for the three cases and why.
        shared = set(dta) & set(do)
        differ = sorted(v for v in shared if dta[v] != do[v])
        disagreements += [(wave, v, dta[v], do[v]) for v in differ]
        vv = value_labels(wave)
        n_values[wave] = len(vv)
        all_labels[str(wave)] = {**do, **dta}
        print(f"  {wave:<6}{len(dta):>8}{len(do) or '-':>8}{len(shared) - len(differ):>8}"
              f"{len(differ):>8}{len(vv):>14}")

    if not all_labels:
        print("no labels extracted — run download.py && unpack.py first", file=sys.stderr)
        return 1

    (CATALOG / "variable_labels.json").write_text(
        json.dumps(all_labels, ensure_ascii=False, indent=1), encoding="utf-8")

    with (CATALOG / "variables.csv").open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["wave", "variable", "label"])
        for wave, vs in all_labels.items():
            for name, lab in sorted(vs.items()):
                w.writerow([wave, name, lab])

    with (CATALOG / "label_disagreements.csv").open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["wave", "variable", "label_dta_used", "label_do_discarded"])
        w.writerows(disagreements)

    total_values = sum(n_values.values())
    print(f"\n  waves             : {len(all_labels)}")
    print(f"  distinct variables: {len({v for vs in all_labels.values() for v in vs}):,}")
    print(f"  .dta/.do label disagreements: {len(disagreements)} (the .dta label is kept)")
    if total_values == 0:
        print("  value labels      : none, in any wave or any build — the EFF does not ship code\n"
              "                      lists. Decode categoricals from cuestionario_<year>.pdf.")
    for p in ("variable_labels.json", "variables.csv", "label_disagreements.csv"):
        print(f"  -> {CATALOG / p}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
