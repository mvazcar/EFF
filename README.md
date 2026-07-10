# EFF — Encuesta Financiera de las Familias, data processing pipeline (Python/Polars)

Builds analysis-ready tables from Banco de España's **Encuesta Financiera de las Familias
(EFF)** microdata: all eight waves (2002–2022), all five imputation implicates, the section-6
member reshape, the 1,000 bootstrap replicate weights, and a cross-wave household bridge.

Same design philosophy as [`mvazcar/MCVL`](https://github.com/mvazcar/MCVL),
[`mvazcar/PanelHogares`](https://github.com/mvazcar/PanelHogares) and
[`mvazcar/ECV`](https://github.com/mvazcar/ECV): a single `config.py`, a `readers.py` of
wave-aware readers, numbered `stepNN_*.py` modules each writing a parquet checkpoint, a
`pipeline.py` orchestrator and a thin `run.py` CLI — plus a `download.py` that drives Banco de
España's registration-gated download area, and an `mi.py` that does the statistics the EFF
cannot be used without.

## Why the EFF needs its own pipeline

Three properties make it unlike the other surveys in this family, and they are not optional
details — get any of them wrong and every number you produce is wrong.

**Five implicates.** Item non-response is multiply imputed five times. Each wave ships five
complete datasets. A point estimate is the mean of the five; its variance needs Rubin's rules.
Analysing implicate 1 alone is not a conservative shortcut, it is a mistake.

**Replicate weights, not strata.** Stratum and cluster identifiers are withheld. The design
variance is recoverable only from the 999 (2002) or 1,000 (later waves) bootstrap replicate
weights BdE ships instead. A standard error computed without them is arbitrary, so `mi.py`
returns `NaN` rather than a plausible-looking number.

**A gated download.** Microdata is released for scientific use behind an email registration.
There is no public URL for most waves — but the flow *is* automatable once you hold the link
BdE mails you. See below.

## Quick start

```bash
pip install -r requirements.txt
```

### 1. Obtain the data

The pipeline's contract starts at `raw_zipped/`, holding the archives exactly as Banco de España
names them, in one flat directory — the same convention as `mvazcar/MCVL`. Put them there by
hand from the browser, or let `download.py` do it:

```bash
# Register once (free) at
#   https://app.bde.es/gnt_seg/controlAccesoEmail.jsp?pas=eff&lang=es&p1=2022
# accepting the research-use-only conditions. BdE emails you an access link.
export EFF_ACCESS_URL='https://pas.bde.es/iae/veriurl/?mail=...&ticket=...'

python download.py                          # 80 archives, ~135 MB  -> raw_zipped/
python download.py --waves 2017 2020        # these two are public: no access link needed
python download.py --groups replicate       # +700 MB, only needed for standard errors
```

`EFF_ACCESS_URL` embeds your email address and an auth ticket. It is read from the environment
so it never reaches the repo, a log, or your shell history. Treat it as a credential.

### 2. Unpack

Dry run first, as in MCVL:

```bash
python unpack.py                # dry run -- shows exactly what would be written
python unpack.py --execute      # raw_zipped/ -> raw/<wave>/, catalog/labels/, docs/
```

### 3. Build

```bash
python run.py --list            # print the input -> output contract and exit
python run.py --catalog         # full build, and rebuild catalog/
python run.py --resume 05       # rerun from step 05, reusing the temp/ parquets
python run.py --steps 03 06     # just these
python run.py --waves 2020 2022 # restrict the waves
python run.py --serial          # one step at a time
```

### 4. Check it

```bash
python validate.py              # reproduces BdE's published Cuadro 1.A / 1.B
```

## Pipeline: input → output

```
raw_zipped/*.zip                       archives as received, flat        [download.py, optional]
    |
    +-- unpack.py --execute
            +--> raw/<wave>/*.dta                data, one directory per wave
            +--> catalog/labels/<wave>/*.do      Stata label programs (2011+)
            +--> docs/*.pdf, *.doc[x]            BdE documentation
```

| Step | Module | Input | Output |
|---|---|---|---|
| 01 | `step01_household.py` | `raw/<wave>/otras_secciones_<wave>_imp{1..5}.dta` | `temp/household_<wave>.parquet` |
| 02 | `step02_members.py` | `raw/<wave>/seccion6_*` + `p1` from `otras_secciones` | `temp/members_<wave>.parquet` |
| 03 | `step03_derived.py` | `raw/<wave>/databol{1..5}.dta` | `temp/derived_<wave>.parquet` |
| 04 | `step04_weights.py` | `raw/<wave>/replicate_*_<wave>.dta` *(optional)* | `temp/weights_<wave>_<kind>.parquet` |
| 05 | `step05_panel.py` | `temp/household_<wave>.parquet`, all waves | `output/panel_bridge.parquet` |
| 06 | `step06_pool.py` | `temp/derived_<wave>.parquet` + the bridge | `output/eff_derived_panel.parquet` |

Steps 01–04 read `raw/` only, so they are independent and run as parallel subprocesses. Step 05
needs every wave's household parquet; step 06 needs every wave's derived parquet and the bridge.

```
01 household ─┬────────────► 05 panel ──┐
02 members    │                         ├─► 06 pool
03 derived  ──┴─────────────────────────┘
04 weights   (independent; only needed for standard errors)
```

`pipeline.py`'s `STEPS` table is the single source of truth for that graph: `run.py --list`
prints it, the execution order is derived from it, and nothing else hardcodes a step id. Step 04
is the one step whose input may legitimately be missing, so it reports "skipping" and exits 0
rather than failing the build. Every other non-zero exit is a real failure and `run.py`
propagates it.

## Repository structure

```
EFF/
  config.py            paths, waves, archive naming, identifiers, weights, per-wave quirks
  download.py          step 0, optional: drives the gated download area -> raw_zipped/
  unpack.py            raw_zipped/*.zip -> raw/<wave>/, catalog/labels/, docs/  (dry run by default)
  readers.py           .dta / .csv readers, implicate stacking, the section-6 member reshape
  mi.py                Rubin's rules + the replicate-weight bootstrap  <- the statistical core
  harmonise.py         cross-wave identifiers, the renamed income variable, the derived core
  labels.py            variable labels -> catalog/, and the .dta-vs-.do cross-check
  build_catalog.py     inventory + variable x wave presence matrix

  step01_household.py  step02_members.py  step03_derived.py
  step04_weights.py    step05_panel.py    step06_pool.py

  pipeline.py          STEPS table (the DAG) + orchestrator
  run.py               CLI: --list / --resume / --steps / --waves / --download / --unpack
  validate.py          reproduces Cuadro 1.A / 1.B of BdE's published tables
  VARIABLES.md         complete variable documentation

  raw_zipped/          archives as received, FLAT                        [gitignored]
  raw/2002/ .. 2022/   extracted .dta, one directory per wave            [gitignored]
  catalog/             labels, inventory, presence matrix                [gitignored]
  docs/                BdE user guides, questionnaires, definitions      [gitignored]
  temp/                per-step parquet checkpoints                      [gitignored]
  output/              eff_derived_panel.parquet, panel_bridge.parquet   [gitignored]
```

## Archive routing quirks (why `unpack.py` exists)

`raw_zipped/` is flat and `raw/` is per-wave. Getting from one to the other is not a `unzip *`:

| Case | Example | What `unpack.py` does |
|---|---|---|
| Wave in the name | `eff_2022_imp1_dta.zip` | → `raw/2022/` |
| Two years in the name | `databol_2008_dta_census2001.zip` | 2001 is not a wave, so the candidates resolve to 2008 |
| No year at all | `imp1_version1.8_dta.zip`, `labels.zip`, `W_version1.8_csv.zip` | the ECB HFCS re-release, published on the 2008 page only → `raw/2008/` |
| Colliding contents | every wave ships `databol1.dta` … `databol5.dta`, no year inside | a flat `raw/` would have eight waves overwrite each other — hence one directory per wave |
| 2002's odd names | `effe_2002_imp1_dta.zip` → `other_sections_2002_imp1.dta` | the Spanish page serves the *English* build for 2002 alone |
| Not data | `etiquetas_*.do`, `definitions_*.doc[x]`, `*cuadros*.pdf` | → `catalog/labels/<wave>/` and `docs/` |
| Unresolvable | anything else | skipped, and named in the output rather than silently dropped |

## Pipeline ↔ `mvazcar/MCVL` correspondence

| `mvazcar/MCVL` | this pipeline |
|---|---|
| `raw_zipped/1145{YY}{S,T}.zip` (flat, as received) | `raw_zipped/eff_{YEAR}_imp{i}_dta.zip` (flat, as received) |
| `normalize_filenames.py` (dry run, `--execute`) | `unpack.py` (dry run, `--execute`) + `download.py` |
| `raw/{YEAR}/MCVL{YEAR}{TYPE}{N}_CDF.TXT` | `raw/{WAVE}/otras_secciones_{WAVE}_imp{i}.dta` |
| `config.py` / `readers.py` | `config.py` / `readers.py` |
| `step01_panels` … `step07_final` | `step01_household` … `step06_pool` |
| `pipeline.py` / `run.py --resume N` | `pipeline.py` (`STEPS` table) / `run.py --resume ID` |
| `temp/` checkpoints, `output/` deliverable | same |
| — | `mi.py` (multiple imputation + replicate weights) |
| — | `validate.py` (reproduces BdE's published tables) |

The two additions are forced by the survey rather than chosen. MCVL is an administrative census
extract: no imputation, no sampling weights, no design variance. The EFF is a stratified,
clustered, multiply-imputed sample, so `mi.py` is not a utility module — it is the only correct
way to read a number off this data.

## Does it work?

`validate.py` recomputes BdE's own published headline table from this pipeline's output —
five implicates, `facine3` weights, 1,000 replicate weights, Rubin's rules — and compares.
Reference: `docs/EFF2022_CuadrosActualizados.pdf`, Cuadro 1.A and 1.B, "TODOS LOS HOGARES",
thousands of 2022 euros.

| quantity | ours | BdE | our SE | BdE SE |
|---|---|---|---|---|
| income, median | 31.6 | 31.6 | 0.47 | 0.5 |
| income, mean | 41.8 | 41.8 | 0.56 | 0.6 |
| net wealth, median | 143.0 | 143.0 | 4.77 | 4.9 |
| net wealth, mean | 315.6 | 315.6 | 17.27 | 17.3 |

All four point estimates reproduce exactly. Three of four standard errors round to the published
figure. The median-net-wealth SE lands at 4.77 against a published 4.9; that gap moves by less
than 0.01 across every combination of `ddof ∈ {0,1}`, centring on the replicate mean or on the
full-sample estimate, and lower / upper / interpolated weighted quantile — so it is not a choice
this code is making. The User Guide notes the first version of these tables was computed on
preliminary imputations.

## What the data actually looks like

Established by reading the delivered files, not by trusting the User Guides.

| wave | households | hh key | prev-wave key | implicates | replicates | panel share |
|---|---|---|---|---|---|---|
| 2002 | 5,143 | `h_number` | — | 5 | 999 | — |
| 2005 | 5,962 | `h_2005` | `h_number` | 5 | 1000 | 43.3% |
| 2008 | 6,197 | `h_2008` | `h_2005` | 5 | 1000 | 64.0% |
| 2011 | 6,106 | `h_2011` | `h_2008` | 5 | 1000 | 60.8% |
| 2014 | 6,120 | `h_2014` | `h_2011` | 5 | 1000 | 50.0% |
| 2017 | 6,413 | `h_2017` | `h_2014` | 5 | 1000 | 56.7% |
| 2020 | 6,313 | `h_2020` | `h_2017` | 5 | 1000 | 60.7% |
| 2022 | 6,385 | `h_2022` | `h_2020` | 5 | 1000 | 62.2% |

(Replicate counts other than 2002 and 2022 are read off the header by `readers.n_replicates()`
when the weights are present; the two guides that state the 2014 figure contradict each other.)

## Findings that contradict the documentation

These cost real time to establish. They are documented at length in the module that depends on
each one.

**The panel chains further than two waves.** The User Guide describes only the adjacent link,
and the 2008 guide is silent about a back-link. But `h_2005` *is* present in the 2008 file, and
the chain runs unbroken back to 2005's `h_number` — the 2002 wave's id under its old name.
`step05_panel.py` composes them with a union-find: 23,882 distinct households, of which 4,698
are observed in **four** waves (the longest spell the rotating design allows). Two households
split in two — one between 2005 and 2008, one between 2014 and 2017 — which is why it is a
union-find and not a chain of joins, since a join would have duplicated those rows silently.

**The member index is the first suffix, not the last.** `p6_13_4_2` is question 13 for member 4's
*second* job, not member 2. And `_m` does not always mean "member" at all: `p2_52_2_1` is the
first loan on the *second property*, `p1_12_3` is the age of the *third child living outside the
home*. A blanket melt of `_<digit>` columns silently mixes members, properties, loans and
children. `readers.py` reshapes section 6 only — where the rule holds — and *checks* it against
the Stata labels before reshaping: across the eight waves the name-derived member index agrees
with the label's "Miem. N." for all 11,330 labelled columns, zero disagreements. (2002's labels
are English and carry no member marker, so there the name rule stands alone — but its member
blocks are still exactly uniform, 245 columns × 9.)

**The EFF ships no value labels. Anywhere.** Not in the `.dta` (every wave reports zero
value-label sets) and not in the `etiquetas_*.do` (which contain `label var` lines only: zero
`label define`). Code lists exist solely in `cuestionario_<year>.pdf` and
`definitions_<year>.doc`, as prose. `harmonise.BREAKDOWNS` therefore hard-codes the only code
lists BdE has ever published machine-readably — the eight breakdown variables in its own Python
example — and nothing else is guessed.

**The `.dta` and `.do` variable labels disagree, and the `.dta` is right.** 127 labels across
five waves. Some are copy-paste bugs (2020's `p4_103s1_2/3/4` are all labelled "Neg 1" in the
`.do`). One is substantive: for 2011's `p1_14o_1` the `.dta` says the occupation code is
*un dígito* CNO-2011 and the `.do` says *dos dígitos*. The data settles it — the column's values
run 0..9 — and the `.dta` wins. `catalog/label_disagreements.csv` lists all 127.

**The derived income variable is renamed and rebased every wave.**
`renthog` → `renthog04_€05` → `renthog07_€09` → `renthog10_€11` → `renthog13_eur14` → … →
`renthog21_eur22`. Note the literal `€` in the 2005–2011 column names (an encoding trap for the
csv build: `€` has no ISO-8859-1 representation), replaced by `eur` from 2014. Note also that
the 2008 wave's income is in **2009** euros, not 2008's. `harmonise.income_column()` decodes both
years from the name; nothing is deflated, because choosing a price index is a modelling decision.

**2002 is the odd wave out.** Its household key is `h_number`. Its Spanish-page archive is named
`effe_2002_...` (every other wave is `eff_...`) and contains the *English* build:
`other_sections_2002_imp1.dta`, with English variable labels. It has 999 replicates, not 1,000,
and no panel weights at all — it is the first wave.

**`p2_71` is not in all eight waves.** It looks like it belongs to the derived core and the 2008
`databol` omits it. `harmonise.DERIVED_CORE` is the exact 54-column set intersection, computed,
not curated.

## Using it

```python
import polars as pl
from mi import estimate, weighted_median
from readers import read_replicate

df = pl.read_parquet("temp/derived_2022.parquet")      # 5 implicates, one row per (hh, implicate)
rw, _, _ = read_replicate(2022, "replicate_weights")   # 1,000 bootstrap weights

r = estimate(df, "riquezanet", stat=weighted_median, replicates=rw, key="hh_id")
print(r)                    #  143,030.80  se= 4,773.06  df=66913.1  fmi=0.008  M=5  R=1000
print(r.ci(0.95))           #  (133676.4, 152385.2) — t interval on Rubin's degrees of freedom
```

`estimate()` without `replicates=` still returns the correct point estimate, and an `se` of
`NaN`. That is deliberate.

## Output

| file | grain | rows | cols |
|---|---|---|---|
| `output/eff_derived_panel.parquet` | wave × household × implicate | 243,195 | 65 |
| `output/panel_bridge.parquet` | wave × household | 48,639 | 9 |
| `temp/household_<wave>.parquet` | household × implicate | ~31,000 | 818–1,360 |
| `temp/members_<wave>.parquet` | household × member × implicate | ~80,000 | 209–288 |

Full build ≈ 40 seconds after unpack, on a multi-core box. See `VARIABLES.md`.

## License

The code in this repository is in the public domain — [Unlicense](LICENSE). This licence covers the
code and nothing else; see **Data** below for the terms of the underlying microdata.

## Data

The EFF microdata is released by Banco de España for scientific research use only: it may not be
transferred to third parties and may not be used commercially. This repository ships **only code**; `.gitignore` excludes all of the data,
and also the generated `catalog/` and the `docs/` PDFs, since both are derived from that delivery.
Obtain the data through the official channel above.

Publications using the EFF must cite Banco de España as the source and must not implicate it in
the results.

## Related pipelines

Same design, other Spanish microdata sources:

- [`mvazcar/MCVL`](https://github.com/mvazcar/MCVL) — Muestra Continua de Vidas Laborales (Social Security)
- [`mvazcar/PanelHogares`](https://github.com/mvazcar/PanelHogares) — AEAT/IEF Panel de Declarantes de IRPF
- [`mvazcar/ECV`](https://github.com/mvazcar/ECV) — INE, Encuesta de Condiciones de Vida (EU-SILC)
- [`mvazcar/EFF`](https://github.com/mvazcar/EFF) — this repository

Built with Claude Code.
