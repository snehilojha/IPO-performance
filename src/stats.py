"""
Rank-based statistics with bootstrap confidence intervals.

Listing gains and returns are heavily right-skewed, and SME listing gains are cut off at a ceiling in two
periods, so everything here works on medians and ranks rather than means. Every bootstrap uses a fixed
seed, so notebook numbers are reproducible.
"""

from dataclasses import dataclass

import numpy as np
import pandas as pd
from scipy.stats import rankdata

SEED = 20260215
N_BOOT = 10_000


@dataclass(frozen=True)
class Estimate:
    """A point estimate with its percentile bootstrap confidence interval"""
    estimate: float
    ci_low: float
    ci_high: float
    n: int

    def __str__(self):
        return f'{self.estimate:+.2f} [{self.ci_low:+.2f}, {self.ci_high:+.2f}] (n={self.n})'

    @property
    def excludes_zero(self) -> bool:
        return self.ci_low > 0 or self.ci_high < 0


def _paired(x, y) -> tuple[np.ndarray, np.ndarray]:
    x, y = np.asarray(x, dtype=float), np.asarray(y, dtype=float)
    keep = np.isfinite(x) & np.isfinite(y)
    return x[keep], y[keep]


def _rowwise_spearman(x: np.ndarray, y: np.ndarray) -> np.ndarray:
    """Spearman correlation of each row of x with the same row of y (average ranks for ties)"""
    rx, ry = rankdata(x, axis=-1), rankdata(y, axis=-1)
    rx -= rx.mean(axis=-1, keepdims=True)
    ry -= ry.mean(axis=-1, keepdims=True)
    with np.errstate(invalid='ignore', divide='ignore'):
        return (rx * ry).sum(axis=-1) / np.sqrt((rx ** 2).sum(axis=-1) * (ry ** 2).sum(axis=-1))


def _interval(point: float, draws: np.ndarray, n: int, ci: float) -> Estimate:
    tail = (1 - ci) / 2 * 100
    low, high = np.nanpercentile(draws, [tail, 100 - tail])
    return Estimate(float(point), float(low), float(high), int(n))


def spearman(x, y) -> float:
    x, y = _paired(x, y)
    return float(_rowwise_spearman(x, y))


def bootstrap_spearman(x, y, n_boot: int = N_BOOT, seed: int = SEED, ci: float = 0.95) -> Estimate:
    """Spearman rank correlation of x and y (pairs with a missing value dropped) with a bootstrap CI"""
    x, y = _paired(x, y)
    idx = np.random.default_rng(seed).integers(0, len(x), size=(n_boot, len(x)))
    return _interval(spearman(x, y), _rowwise_spearman(x[idx], y[idx]), len(x), ci)


def bootstrap_spearman_diff(x1, x2, y, n_boot: int = N_BOOT, seed: int = SEED, ci: float = 0.95) -> Estimate:
    """
    rho(x1, y) - rho(x2, y) on the rows where all three are present, resampling rows together so the CI
    accounts for x1 and x2 being correlated with each other.
    """
    x1, x2, y = (np.asarray(v, dtype=float) for v in (x1, x2, y))
    keep = np.isfinite(x1) & np.isfinite(x2) & np.isfinite(y)
    x1, x2, y = x1[keep], x2[keep], y[keep]
    idx = np.random.default_rng(seed).integers(0, len(y), size=(n_boot, len(y)))
    point = spearman(x1, y) - spearman(x2, y)
    draws = _rowwise_spearman(x1[idx], y[idx]) - _rowwise_spearman(x2[idx], y[idx])
    return _interval(point, draws, len(y), ci)


def bootstrap_median_diff(a, b, n_boot: int = N_BOOT, seed: int = SEED, ci: float = 0.95) -> Estimate:
    """median(a) - median(b) for two independent groups, each resampled separately; n is len(a) + len(b)"""
    a, b = (np.asarray(v, dtype=float) for v in (a, b))
    a, b = a[np.isfinite(a)], b[np.isfinite(b)]
    rng = np.random.default_rng(seed)
    draws = (np.median(a[rng.integers(0, len(a), size=(n_boot, len(a)))], axis=1)
             - np.median(b[rng.integers(0, len(b), size=(n_boot, len(b)))], axis=1))
    return _interval(np.median(a) - np.median(b), draws, len(a) + len(b), ci)


def bootstrap_median(values, n_boot: int = N_BOOT, seed: int = SEED, ci: float = 0.95) -> Estimate:
    values = np.asarray(values, dtype=float)
    values = values[np.isfinite(values)]
    idx = np.random.default_rng(seed).integers(0, len(values), size=(n_boot, len(values)))
    return _interval(np.median(values), np.median(values[idx], axis=1), len(values), ci)


def stratified_spearman(df: pd.DataFrame, x: str, y: str, strata: str | list[str], min_n: int = 10,
                        n_boot: int = N_BOOT, seed: int = SEED, ci: float = 0.95) -> Estimate:
    """
    Average of the Spearman correlations inside each stratum (e.g. listing year), weighted by stratum size,
    so a relationship driven only by differences between strata doesn't count. Strata with fewer than
    min_n complete rows are left out; the bootstrap resamples within each stratum. n counts rows used.
    """
    point, draws, n = _stratified_draws(df, x, y, strata, min_n, n_boot, np.random.default_rng(seed))
    return _interval(point, draws, n, ci)


def stratified_spearman_group_diff(df_a: pd.DataFrame, df_b: pd.DataFrame, x: str, y: str,
                                   strata: str | list[str], min_n: int = 10, n_boot: int = N_BOOT,
                                   seed: int = SEED, ci: float = 0.95) -> Estimate:
    """
    stratified_spearman(df_a) - stratified_spearman(df_b), e.g. within-year correlation in hot years minus
    cold years. Strata are treated as fixed: the interval covers sampling of IPOs within strata, not which
    strata happened to fall in each group.
    """
    rng = np.random.default_rng(seed)
    point_a, draws_a, n_a = _stratified_draws(df_a, x, y, strata, min_n, n_boot, rng)
    point_b, draws_b, n_b = _stratified_draws(df_b, x, y, strata, min_n, n_boot, rng)
    return _interval(point_a - point_b, draws_a - draws_b, n_a + n_b, ci)


def _stratified_draws(df, x, y, strata, min_n, n_boot, rng) -> tuple[float, np.ndarray, int]:
    points, draws, sizes = [], [], []
    strata_columns = [strata] if isinstance(strata, str) else list(strata)
    for _, group in df[[x, y, *strata_columns]].groupby(strata_columns, observed=True):
        gx, gy = _paired(group[x], group[y])
        if len(gx) < min_n:
            continue
        idx = rng.integers(0, len(gx), size=(n_boot, len(gx)))
        points.append(spearman(gx, gy))
        draws.append(_rowwise_spearman(gx[idx], gy[idx]))
        sizes.append(len(gx))
    if not sizes:
        raise ValueError(f'No stratum of {strata} has {min_n} complete rows')
    weights = np.array(sizes) / sum(sizes)
    point = float(np.dot(weights, points))
    # A resample with no variation in a small stratum gives NaN; average over the strata that have a value
    stacked = np.vstack(draws)
    valid = np.isfinite(stacked)
    combined = (np.where(valid, stacked, 0) * weights[:, None]).sum(axis=0) / (valid * weights[:, None]).sum(axis=0)
    return point, combined, sum(sizes)


def decile_table(df: pd.DataFrame, by: str, value: str, q: int = 10, at_ceiling: str | None = None) -> pd.DataFrame:
    """
    Split rows into q equal-count groups on `by` and summarise `value` in each: median and IQR, share above
    zero and, if a boolean column is named in at_ceiling, the share sitting at a ceiling (where the median
    is only a lower bound). Ties on `by` are split by row order so the groups stay equal-sized.
    """
    data = df[df[by].notna() & df[value].notna()]
    groups = pd.qcut(data[by].rank(method='first'), q, labels=range(1, q + 1))
    grouped = data.groupby(groups, observed=True)
    table = pd.DataFrame({
        'n': grouped.size(),
        f'{by}_min': grouped[by].min(),
        f'{by}_median': grouped[by].median(),
        f'{by}_max': grouped[by].max(),
        'median': grouped[value].median(),
        'q25': grouped[value].quantile(0.25),
        'q75': grouped[value].quantile(0.75),
        'share_positive': grouped[value].apply(lambda v: (v > 0).mean()),
    })
    if at_ceiling is not None:
        table['share_at_ceiling'] = grouped[at_ceiling].mean()
    return table.rename_axis('group')
