# Variable reference

Reference for the tables this pipeline produces. Definitions of the derived variables come from
Banco de España's own `definitions_<year>.doc[x]` (a Stata program), which `unpack.py` places in
`docs/`. Definitions of the questionnaire variables come from `cuestionario_<year>.pdf`.

Run `python run.py --catalog` to regenerate `catalog/`, which holds the machine-readable
inventory: `variables.csv` (4,067 variable labels across the eight waves), `presence.csv`
(variable × wave matrix), `wave_summary.csv`, `master_inventory.csv`, and
`label_disagreements.csv`.

---

## Naming convention of the questionnaire variables

    ps_nn            question nn of section s, asked once of the household
    ps_nn_m          ... asked several times; m is the repetition index
    ps_nn_m_r        ... and r is a second repetition inside m
    ps_nnXk_m        X ∈ {c, s, z, v}: a multiple-answer question, k-th alternative
                       c = one dummy per possible answer
                       s = up to five chosen options, unordered (s1..s5)
                       z = one per future scenario
                       v = one per banking service rated

**`m` is not always "household member".** It is whatever the question repeats over.

| variable | `m` means | label |
|---|---|---|
| `p1_1_1` | member 1 | sex of member 1 |
| `p1_12_3` | third child | age of the 3rd child living outside the home |
| `p2_52_2_1` | second property | first loan on the second property |
| `p6_13_4_2` | member 4 | member 4's second employee job, type of contract |

In **section 6 only**, `m` is always the household member, and it is the *first* index after the
question number and any multiple-answer code — `p6_13_4_2` is member 4, job 2. `readers.py`
validates this against the Stata labels before reshaping (11,330 labelled columns, zero
disagreements) and refuses to reshape a wave that breaks it.

## Shadow variables

Every questionnaire variable `p...` has a twin `j...` in `sombra_<wave>.dta`, recording the
original state of the value before imputation. One file per wave, common to all five implicates.

| code | meaning |
|---|---|
| `1` | complete observation |
| `0` | true missing, implied by an earlier answer |
| `2050` | imputed: the answer was "Don't know" |
| `2051` | imputed: the answer was "No answer" |
| `2052` | imputed: no answer to preceding variables |
| `2053` | imputed: answered but incorrect |
| `2055` | imputed: not asked, CAPI or interviewer error |

`shadow >= 2050` means the value in the five implicates was imputed. Use this to restrict to
observed values, or to compute the fraction of missing information by hand.

There is **no global missing-value sentinel.** After imputation a missing value is simply absent.
Codes that look like sentinels are question-specific: `96` means "does not use this banking
service" in `p8_30v3`, and is an ordinary household id in `h_2022`.

---

## `output/eff_derived_panel.parquet`

One row per (wave, household, implicate). 243,195 rows × 65 columns. The 54 `databol` variables
present in all eight waves, plus keys, weights and the harmonised income column.

### Keys, weights, metadata

| column | type | description |
|---|---|---|
| `wave` | Int16 | 2002, 2005, 2008, 2011, 2014, 2017, 2020, 2022 |
| `hh_id` | Int64 | household id **within its wave**; `h_number` in 2002, `h_<wave>` after |
| `implicate` | Int8 | 1..5, the imputation implicate. Never pool these as if they were rows |
| `facine3` | Float64 | cross-sectional household weight; sums to the number of Spanish households |
| `panel_id` | Int64 | synthetic id, stable across waves (from `output/panel_bridge.parquet`) |
| `n_waves` | UInt32 | how many waves this household is observed in, 1..4 |
| `first_wave`, `last_wave` | Int16 | the observed window |
| `income_year` | Int16 | the year the income refers to (null in 2002) |
| `euro_base_year` | Int16 | the year whose euros it is expressed in (null in 2002) |

### Income and wealth aggregates

Monetary, in **each wave's own euros** — see `euro_base_year`. Nothing is deflated;
`harmonise.deflate()` will rebase them with a price index you choose and can cite.

| column | description |
|---|---|
| `renthog_eur` | total annual household income. Named `renthog21_eur22` in 2022, `renthog07_€09` in 2008, plain `renthog` in 2002 |
| `riquezanet` | net wealth = gross wealth − total debt |
| `riquezabr` | gross wealth = real assets + financial assets |
| `actreales` | real assets |
| `actfinanc` | financial assets |
| `vdeuda` | total debt = `dvivpral + deuoprop + phipo + pperso + potrasd + ptmos_tarj` |
| `adeuda` | share of households with debt |
| `valhog` | value of the main residence |
| `dvivpral` | debt outstanding on the main residence |
| `deuoprop` | debt on other real-estate property |
| `hipo`, `phipo` | other mortgage debt: holds / amount |
| `perso`, `pperso` | personal loans: holds / amount |
| `otrasd`, `potrasd` | other debt: holds / amount |
| `dpdte`, `dpdtehipo`, `deuhipv` | outstanding balances, mortgage detail |
| `cuentas`, `salcuentas` | bank accounts: holds / balance |
| `penseg`, `valpenseg` | pension and insurance products: holds / value |
| `otraspr` | other real-estate properties held |
| `tienereal` | has any real asset — `np2_1==1 \| np2_32==1 \| np2_82==1 \| havenegval==1` |
| `tienefin` | has any financial asset |
| `tiene` | has any asset |
| `sideuda` | has debt |
| `havenegval` | owns a business run by the household |
| `pagodeuda` | annual debt service |
| `alim`, `nodur` | food / non-durable spending |
| `gvehic`, `gimpvehic`, `tvehic`, `timpvehic` | vehicles: spending, imputed spending, holdings |
| `allf`, `odeuhog` | other financial assets / other household debt |

### Breakdown variables

The only categorical code lists Banco de España publishes machine-readably. `harmonise.BREAKDOWNS`
holds them; `harmonise.label_breakdown()` attaches the names. Everything else: read the
questionnaire.

| column | codes |
|---|---|
| `bage` | age of the household head: 1 `<35`, 2 `35–44`, 3 `45–54`, 4 `55–64`, 5 `65–74`, 6 `>74` |
| `percrent` | income percentile: 1 `<P20`, 2 `P20–40`, 3 `P40–60`, 4 `P60–80`, 5 `P80–90`, 6 `>P90` |
| `percriq` | net-wealth percentile: 1 `<P25`, 2 `P25–50`, 3 `P50–75`, 4 `P75–90`, 5 `>P90` |
| `nsitlabdom` | 1 employee, 2 self-employed, 3 retired, 4 other inactive or unemployed |
| `neducdom` | 1 below secondary, 2 secondary, 3 university |
| `np2_1` | main residence: 1 owned, 2 other tenure |
| `nnumadtrab` | working members: 0 none, 1 one, 2 two, 3 three or more |
| `np1` | household size, top-coded at 5: 1 one … 5 five or more (observed range 1..5 in every wave) |

"Household head" (*cabeza de familia*) is BdE's own construct, not a survey variable: the
reference person if male, otherwise their male partner if he lives in the household.

---

## `output/panel_bridge.parquet`

One row per (wave, household). 48,639 rows.

| column | description |
|---|---|
| `wave`, `hh_id` | the household, in its own wave |
| `panel_id` | `first_wave × 10^7 + that wave's hh_id`. Stable across waves |
| `n_waves` | distinct waves in which this household appears, 1..4 |
| `n_records` | rows in the component. Exceeds `n_waves` for the two households that split |
| `first_wave`, `last_wave` | the observed window |

Built by union-find over the `hogarpanel` back-links. 23,882 distinct households; 11,469 seen
once, 4,769 twice, 2,946 three times, 4,698 four times. No household spans more than four waves:
the panel rotates.

**A "panel household" is the household at that address, not the same people.** Its composition
may have changed entirely. `pan_1..pan_9` map member slots between *adjacent* waves only
(member x this wave was member y last wave); this pipeline does not chain them.

**Longitudinal weights.** `pesopan_1` and `pesopan_2` are calibrated for the two-wave panel of one
adjacent pair — to the previous and to the current wave's population respectively. There is no
weight calibrated for a household observed across four waves. Analysis over a long window is
unweighted, and you should say so.

---

## `temp/household_<wave>.parquet`

One row per (household, implicate). 818 columns in 2002, 1,360 in 2020. Everything the
questionnaire asked outside section 6: demographics of the reference person (section 1), real
assets and their debts (2), other debts (3), financial assets (4), pensions and insurance (5),
means of payment (7), consumption and saving (8, 9).

Carries `hh_id`, `hh_id_prev`, `wave`, `hogarpanel`, `pan_1..pan_9`, `facine3`, `pesopan_1/2`,
`p1` (household size), `renthog` (nominal income) and `mrenthog` (income in the interview month)
alongside the original `h_<wave>` column, which the shadow and replicate-weight files join on.

2,148 of the 4,142 distinct variable names appear in all eight waves — but a shared name is not
a shared meaning. The occupation classification moved from CNO-1994 to CNO-2011 between 2008 and
2011, so `p6_3_<m>` means different things either side of that break. Check `catalog/presence.csv`
and the questionnaires before pooling anything from this file.

## `temp/members_<wave>.parquet`

One row per (household, member, implicate). Section 6 — labour-market status and labour income —
reshaped from its household-wide layout. ~16,000 members per implicate, mean household size 2.53
in 2022 (2.74 in 2002).

Padding is dropped: a three-person household has no rows for member slots 4..9. The member count
comes from `p1`, which lives in `otras_secciones`, so step 02 joins the two files.

The truncation also runs the other way, and the survey does it, not this pipeline. `p1` reaches
10 in 2002 and 11 in 2022, but section 6 has only **nine** member slots, so members 10 and 11
were never recorded. One or two households per wave are affected (five members lost in 2011, the
worst case), which is why the member table's mean household size sits a few ten-thousandths below
`mean(p1)`: 2.5272 against 2.5276 in 2022. `step02_members.py` prints the count.

## `temp/weights_<wave>_<kind>.parquet`

One row per household, `R` weight columns.

| kind | columns | calibration |
|---|---|---|
| `replicate_weights` | `wt3r_1..R` | cross-sectional |
| `replicate_pan1weights` | `wtpan1r_1..R` | panel, to the previous wave's population |
| `replicate_pan2weights` | `wtpan2r_1..R` | panel, to this wave's population |

`R` = 999 in 2002, 1,000 afterwards — read off the header, because the two User Guides that state
it for 2014 contradict each other. 2002 has no panel replicate files.

The `ntimesr_*` multiplicity columns (how many times a household was drawn into replicate *i*)
are dropped by default; `wt3r_i` already embeds the multiplicity. `step04_weights.py --keep-ntimes`
keeps them.
