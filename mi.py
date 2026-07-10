"""
Multiple imputation and design-based variance for the EFF.

Nothing in the EFF can be estimated correctly without this module. The survey imputes item
non-response five times and withholds the stratum and cluster identifiers, so both the point
estimate and its standard error are combinations, not single numbers:

    point estimate      average of the five implicate estimates
    sampling variance   bootstrap variance of each implicate, from the replicate weights
    imputation variance the spread of the five implicate estimates
    total variance      Rubin's rules over the two

Rubin's rules (User Guide 2022, section 5)
------------------------------------------
With M = 5 implicates, per-implicate estimates Q_i and per-implicate sampling variances V_i:

    Q̄ = (1/M) Σ Q_i                              MI point estimate
    W  = (1/M) Σ V_i                              within-imputation (sampling) variance
    B  = 1/(M-1) Σ (Q_i - Q̄)²                     between-imputation (imputation) variance
    T  = W + (1 + 1/M) B                          total variance

For M = 5 the last line is the User Guide's `T = W + (6/5)B`. This module keeps the general
(1 + 1/M) so it stays right if BdE ever changes M.

Reference degrees of freedom follow Rubin (1987): with r = (1 + 1/M)B / W the relative increase
in variance due to non-response,

    df  = (M-1)(1 + 1/r)²
    fmi = (r + 2/(df+3)) / (r + 1)                fraction of missing information

df is not "M-1". It grows without bound as B -> 0, and for EFF headline aggregates B is tiny:
for the 2022 mean of net wealth the imputation component contributes sqrt(1.2B) = 0.9 k€ against
a sampling component sqrt(W) = 17.3 k€, giving fmi = 0.002 and df in the hundreds of thousands.
There the t interval is indistinguishable from the normal one. For a variable that is heavily
imputed — a self-employed business valuation, say — fmi rises, df collapses towards M-1 = 4, and
the t quantile matters a great deal. `MIResult.ci()` therefore always uses t on `df`, and `fmi`
is reported so you can see which regime you are in.

Sampling variance from replicate weights (User Guide 2022, section 6)
--------------------------------------------------------------------
BdE ships R = 1000 bootstrap replicate weights (999 in 2002) drawn under the true stratified,
clustered design. For a statistic θ evaluated once per replicate weight, the do-file BdE
publishes computes the R replicate estimates and then calls Stata's `summarize`, whose reported
standard deviation is

    V_i = 1/(R-1) Σ_r (θ_r - θ̄)²        θ̄ = (1/R) Σ_r θ_r

so that is this module's default (`ddof=1`, centred on the replicate mean). Stata's
`svyset [pw=facine3], bsrweight(wt3r_*) vce(bootstrap)` instead divides by R and, under the
`mse` option, centres on the full-sample estimate. At R = 1000 the three conventions differ by
under 0.2%, but they are not the same number, so `replicate_variance` exposes both knobs rather
than picking one silently.

The companion `ntimesr_i` columns record how many times each household was drawn into replicate
i. They are not needed for weighted statistics — `wt3r_i` already embeds the multiplicity — and
are provided for estimators that must rebuild the resample explicitly.

Weighted statistics
-------------------
`weighted_quantile` reproduces the lower weighted quantile used in BdE's own published Python
example: sort by value, accumulate weights, take the first observation whose cumulative weight
reaches q of the total. It returns an observed data value, never an interpolation, which is
what makes the published EFF medians reproducible to the euro.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Sequence

import numpy as np
import polars as pl

from config import N_IMPLICATES, XSEC_WEIGHT

Statistic = Callable[[np.ndarray, np.ndarray], float]


# ── weighted statistics ───────────────────────────────────────────────────
def weighted_mean(x: np.ndarray, w: np.ndarray) -> float:
    m = ~np.isnan(x)
    if not m.any():
        return float("nan")
    return float(np.average(x[m], weights=w[m]))


def weighted_quantile(x: np.ndarray, w: np.ndarray, q: float = 0.5) -> float:
    """
    Lower weighted quantile, matching BdE's published `weighted_median`.

    Sorts by x, accumulates w, and returns the first x whose cumulative weight reaches q of
    the total. No interpolation: the result is always an observed value.
    """
    m = ~np.isnan(x)
    if not m.any():
        return float("nan")
    x, w = x[m], w[m]
    order = np.argsort(x, kind="stable")
    cum = np.cumsum(w[order])
    i = int(np.searchsorted(cum, q * cum[-1]))
    i = min(i, len(x) - 1)
    return float(x[order[i]])


def weighted_median(x: np.ndarray, w: np.ndarray) -> float:
    return weighted_quantile(x, w, 0.5)


def weighted_share(x: np.ndarray, w: np.ndarray) -> float:
    """Weighted share of a 0/1 (or boolean) indicator, as a percentage."""
    m = ~np.isnan(x)
    if not m.any():
        return float("nan")
    return 100.0 * float(np.average(x[m].astype(float), weights=w[m]))


def weighted_total(x: np.ndarray, w: np.ndarray) -> float:
    m = ~np.isnan(x)
    return float(np.sum(x[m] * w[m]))


# ── Rubin's rules ─────────────────────────────────────────────────────────
@dataclass(frozen=True)
class MIResult:
    """The combined estimate and its variance decomposition."""
    estimate: float
    within: float                  # W, sampling variance
    between: float                 # B, imputation variance
    total_var: float               # T = W + (1 + 1/M) B
    se: float
    df: float                      # Rubin (1987) reference degrees of freedom
    fmi: float                     # fraction of missing information
    n_implicates: int
    n_replicates: int | None
    per_implicate: tuple[float, ...]

    def ci(self, level: float = 0.95) -> tuple[float, float]:
        """Two-sided t interval on `df` degrees of freedom."""
        from scipy import stats                                  # optional; see requirements

        t = float(stats.t.ppf(0.5 + level / 2, max(self.df, 1e-9)))
        return self.estimate - t * self.se, self.estimate + t * self.se

    def __str__(self) -> str:
        se = "      n/a" if np.isnan(self.se) else f"{self.se:>9,.2f}"
        return (f"{self.estimate:>14,.2f}  se={se}  df={self.df:>7.1f}  "
                f"fmi={self.fmi:>5.3f}  M={self.n_implicates}"
                + (f"  R={self.n_replicates}" if self.n_replicates else ""))


def combine(estimates: Sequence[float], variances: Sequence[float] | None = None,
            n_replicates: int | None = None) -> MIResult:
    """
    Apply Rubin's rules to M implicate estimates and (optionally) their sampling variances.

    With `variances=None` only the between-imputation component is known: the estimate is still
    correct, but `se` is NaN rather than a number that pretends the design was simple random
    sampling. That refusal is deliberate — an EFF standard error computed without the replicate
    weights is not conservative, it is arbitrary.
    """
    Q = np.asarray(estimates, dtype=float)
    M = len(Q)
    if M < 1:
        raise ValueError("no implicate estimates")
    Qbar = float(np.mean(Q))
    B = float(np.var(Q, ddof=1)) if M > 1 else 0.0

    if variances is None:
        return MIResult(Qbar, float("nan"), B, float("nan"), float("nan"),
                        float("nan"), float("nan"), M, n_replicates, tuple(Q))

    W = float(np.mean(np.asarray(variances, dtype=float)))
    T = W + (1.0 + 1.0 / M) * B

    # The two degenerate branches are opposites, and collapsing them would report the wrong
    # regime. r = (1 + 1/M)B / W is the relative increase in variance due to non-response:
    #   B = 0  ->  r = 0:   no missing information. fmi = 0, df unbounded.
    #   W = 0  ->  r = inf: ALL variance is imputation variance. fmi = 1, df = M - 1.
    # W = 0 cannot arise from real replicate weights, but it does arise from a constant
    # statistic, and reporting fmi = 0 there would say the opposite of the truth.
    if B <= 0:
        df, fmi = float("inf"), 0.0
    elif W <= 0:
        df, fmi = float(M - 1), 1.0
    else:
        r = (1.0 + 1.0 / M) * B / W
        df = (M - 1) * (1.0 + 1.0 / r) ** 2
        fmi = (r + 2.0 / (df + 3.0)) / (r + 1.0)

    return MIResult(Qbar, W, B, T, float(np.sqrt(T)), df, fmi, M, n_replicates, tuple(Q))


# ── bootstrap variance from replicate weights ─────────────────────────────
def replicate_variance(x: np.ndarray, rep_w: np.ndarray, stat: Statistic,
                       theta_full: float | None = None,
                       ddof: int = 1, center: str = "mean") -> tuple[float, np.ndarray]:
    """
    Sampling variance of `stat` from an (n, R) matrix of replicate weights.

    center="mean"  -> centred on the mean of the R replicate estimates   (BdE's do-file)
    center="full"  -> centred on `theta_full`, the full-sample estimate  (Stata's `mse` option)

    Returns (variance, the R replicate estimates).
    """
    if rep_w.ndim != 2:
        raise ValueError(f"rep_w must be (n, R), got shape {rep_w.shape}")
    if rep_w.shape[0] != x.shape[0]:
        raise ValueError(f"rep_w has {rep_w.shape[0]} rows, x has {x.shape[0]}")

    theta = np.array([stat(x, rep_w[:, r]) for r in range(rep_w.shape[1])], dtype=float)
    good = ~np.isnan(theta)
    if good.sum() < 2:
        return float("nan"), theta

    if center == "full":
        if theta_full is None:
            raise ValueError('center="full" needs theta_full')
        centre = theta_full
    elif center == "mean":
        centre = float(np.mean(theta[good]))
    else:
        raise ValueError('center must be "mean" or "full"')

    R = int(good.sum())
    var = float(np.sum((theta[good] - centre) ** 2) / max(R - ddof, 1))
    return var, theta


# ── the estimator users actually call ─────────────────────────────────────
def estimate(df: pl.DataFrame, var: str, stat: Statistic = weighted_mean,
             weight: str = XSEC_WEIGHT, implicate_col: str = "implicate",
             replicates: pl.DataFrame | None = None, key: str | None = None,
             rep_prefix: str = "wt3r_", **rep_kwargs) -> MIResult:
    """
    MI point estimate and, when `replicates` is given, its design-correct standard error.

    `df` is the stacked five-implicate frame (readers.stack_implicates). `replicates` is the
    wave's replicate-weight frame, joined on `key`; supply it and `key` to get a standard error.

    The replicate weights are joined per implicate rather than once up front, because the join
    must not reorder rows relative to the values being weighted.
    """
    imps = sorted(df[implicate_col].unique().to_list())
    if len(imps) != N_IMPLICATES:
        # Not fatal: a user may deliberately pass a subset for exploration.
        print(f"  note: {len(imps)} implicates present, expected {N_IMPLICATES}")

    rep_cols: list[str] = []
    if replicates is not None:
        if key is None:
            raise ValueError("`key` is required when `replicates` is given")
        rep_cols = [c for c in replicates.columns
                    if c.startswith(rep_prefix) and c[len(rep_prefix):].isdigit()]
        rep_cols.sort(key=lambda c: int(c[len(rep_prefix):]))
        if not rep_cols:
            raise ValueError(f"no columns matching {rep_prefix}<int> in the replicate frame")

    Q, V = [], []
    for i in imps:
        sub = df.filter(pl.col(implicate_col) == i)
        if replicates is not None:
            sub = sub.join(replicates, on=key, how="inner")
        x = sub[var].cast(pl.Float64).to_numpy()
        w = sub[weight].cast(pl.Float64).to_numpy()
        q = stat(x, w)
        Q.append(q)
        if rep_cols:
            rw = sub.select(rep_cols).to_numpy().astype(float)
            v, _ = replicate_variance(x, rw, stat, theta_full=q, **rep_kwargs)
            V.append(v)

    return combine(Q, V or None, n_replicates=len(rep_cols) or None)
