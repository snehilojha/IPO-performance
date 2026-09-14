"""src/stats.py on small synthetic samples with known answers."""

import numpy as np
import pandas as pd
import pytest
from scipy.stats import spearmanr
from sklearn.metrics import brier_score_loss, roc_auc_score

from src.stats import (
    bootstrap_auc,
    bootstrap_auc_diff,
    bootstrap_brier_diff,
    bootstrap_median,
    bootstrap_median_diff,
    bootstrap_spearman,
    bootstrap_spearman_diff,
    decile_table,
    roc_auc,
    spearman,
    stratified_spearman,
    stratified_spearman_group_diff,
)

N_BOOT = 2_000


@pytest.fixture
def rng():
    return np.random.default_rng(0)


def test_spearman_matches_scipy_with_ties_and_missing(rng):
    x = rng.integers(0, 5, 200).astype(float)
    y = x + rng.normal(size=200)
    x[:3] = np.nan
    keep = ~np.isnan(x)
    assert spearman(x, y) == pytest.approx(spearmanr(x[keep], y[keep]).statistic)


def test_bootstrap_spearman_ci_brackets_estimate_and_is_reproducible(rng):
    x = rng.normal(size=300)
    y = x + rng.normal(size=300)
    first = bootstrap_spearman(x, y, n_boot=N_BOOT)
    assert first == bootstrap_spearman(x, y, n_boot=N_BOOT)
    assert first.ci_low < first.estimate < first.ci_high
    assert first.excludes_zero and first.n == 300


def test_bootstrap_spearman_independent_data_includes_zero(rng):
    result = bootstrap_spearman(rng.normal(size=300), rng.normal(size=300), n_boot=N_BOOT)
    assert not result.excludes_zero


def test_spearman_diff_detects_stronger_predictor(rng):
    y = rng.normal(size=400)
    strong = y + rng.normal(scale=0.3, size=400)
    weak = y + rng.normal(scale=3, size=400)
    result = bootstrap_spearman_diff(strong, weak, y, n_boot=N_BOOT)
    assert result.estimate == pytest.approx(spearman(strong, y) - spearman(weak, y))
    assert result.ci_low > 0


def test_median_diff_recovers_shift(rng):
    a = rng.exponential(size=500) + 10
    b = rng.exponential(size=400)
    result = bootstrap_median_diff(a, b, n_boot=N_BOOT)
    assert result.estimate == pytest.approx(np.median(a) - np.median(b))
    assert result.ci_low < result.estimate < result.ci_high
    assert result.estimate == pytest.approx(10, abs=0.3) and result.ci_high - result.ci_low < 1
    assert result.n == 900
    assert bootstrap_median([1, 2, 3, np.nan], n_boot=N_BOOT).estimate == 2


def test_stratified_spearman_removes_between_strata_trend(rng):
    # Within each year y falls with x, but later years have both higher x and higher y (Simpson's paradox)
    frames = []
    for year in range(5):
        x = rng.normal(size=60)
        frames.append(pd.DataFrame({'year': year, 'x': x + 5 * year, 'y': -x + 5 * year + rng.normal(scale=0.3, size=60)}))
    df = pd.concat(frames, ignore_index=True)
    assert spearman(df['x'], df['y']) > 0.5
    result = stratified_spearman(df, 'x', 'y', 'year', n_boot=N_BOOT)
    assert result.estimate < -0.8 and result.ci_high < 0
    assert result.n == 300


def test_stratified_group_diff_compares_within_strata(rng):
    def years(first, noise):
        frames = []
        for year in range(first, first + 3):
            x = rng.normal(size=80)
            # Each year sits at a different level; only the within-year link differs between the groups
            frames.append(pd.DataFrame({'year': year, 'x': x + 3 * year,
                                        'y': x + noise * rng.normal(size=80) + 10 * year}))
        return pd.concat(frames, ignore_index=True)

    tight, loose = years(0, 0.2), years(3, 5.0)
    result = stratified_spearman_group_diff(tight, loose, 'x', 'y', 'year', n_boot=N_BOOT)
    expected = (stratified_spearman(tight, 'x', 'y', 'year', n_boot=N_BOOT).estimate
                - stratified_spearman(loose, 'x', 'y', 'year', n_boot=N_BOOT).estimate)
    assert result.estimate == pytest.approx(expected)
    assert result.ci_low > 0.5 and result.n == 480
    same = stratified_spearman_group_diff(tight, tight, 'x', 'y', 'year', n_boot=N_BOOT)
    assert same.estimate == pytest.approx(0) and same.ci_low < 0 < same.ci_high


def test_stratified_spearman_skips_small_strata(rng):
    df = pd.DataFrame({'g': ['a'] * 30 + ['b'] * 5, 'x': rng.normal(size=35)})
    df['y'] = df['x']
    assert stratified_spearman(df, 'x', 'y', 'g', min_n=10, n_boot=N_BOOT).n == 30
    with pytest.raises(ValueError):
        stratified_spearman(df, 'x', 'y', 'g', min_n=100, n_boot=N_BOOT)


def test_roc_auc_matches_sklearn_with_ties_and_missing(rng):
    y = rng.integers(0, 2, 300).astype(float)
    score = np.round(y + rng.normal(size=300), 1)  # rounding creates ties
    score[:4] = np.nan
    keep = ~np.isnan(score)
    assert roc_auc(y, score) == pytest.approx(roc_auc_score(y[keep], score[keep]))
    result = bootstrap_auc(y, score, n_boot=N_BOOT)
    assert result.estimate == pytest.approx(roc_auc_score(y[keep], score[keep]))
    assert result.ci_low < result.estimate < result.ci_high and result.n == 296


def test_auc_and_brier_diffs_detect_the_better_forecast(rng):
    y = rng.integers(0, 2, 500).astype(float)
    sharp = np.clip(0.5 + 0.8 * (y - 0.5) + rng.normal(scale=0.15, size=500), 0.01, 0.99)
    vague = np.clip(0.5 + 0.1 * (y - 0.5) + rng.normal(scale=0.15, size=500), 0.01, 0.99)
    auc = bootstrap_auc_diff(y, sharp, vague, n_boot=N_BOOT)
    assert auc.estimate == pytest.approx(roc_auc_score(y, sharp) - roc_auc_score(y, vague))
    assert auc.ci_low > 0
    brier = bootstrap_brier_diff(y, sharp, vague, n_boot=N_BOOT)
    assert brier.estimate == pytest.approx(brier_score_loss(y, sharp) - brier_score_loss(y, vague))
    assert brier.ci_high < 0
    same = bootstrap_auc_diff(y, sharp, sharp, n_boot=N_BOOT)
    assert same.estimate == 0 and same.ci_low == 0 == same.ci_high


def test_decile_table_groups_and_summaries():
    df = pd.DataFrame({'sub': np.arange(1, 21, dtype=float), 'gain': np.arange(-9, 11, dtype=float)})
    df['capped'] = df['gain'] >= 9
    df.loc[0, 'gain'] = np.nan
    table = decile_table(df, 'sub', 'gain', q=4, at_ceiling='capped')
    # The row with a missing gain is dropped: 19 rows, top group is sub 16-20 with gains 6-10
    assert table['n'].tolist() == [5, 5, 4, 5]
    assert table['sub_min'].tolist() == [2, 7, 12, 16]
    assert table.loc[4, 'median'] == 8
    assert table.loc[1, 'share_positive'] == 0
    assert table.loc[4, 'share_at_ceiling'] == 0.4
