"""Findings: the notebook verdicts with their charts. The text is written for one snapshot and says so."""

from datetime import date

import streamlit as st

from app.common import REPO_URL, load_data
from src.config import FIGURES_DIR

FINDINGS_SNAPSHOT = date(2026, 2, 15)
NOTEBOOK_02 = f'{REPO_URL}/blob/main/notebooks/02_subscription_vs_returns.ipynb'
NOTEBOOK_03 = f'{REPO_URL}/blob/main/notebooks/03_listing_gain_model.ipynb'


def figure(name: str, caption: str):
    path = FIGURES_DIR / f'{name}.png'
    if path.exists():
        st.image(str(path), caption=caption, width='stretch')
    else:
        st.caption(f'Chart {name}.png not found; run the notebook to create it.')


data = load_data()

st.title('Findings')
st.markdown(
    f'Answers from [notebook 02]({NOTEBOOK_02}) and [notebook 03]({NOTEBOOK_03}), for the snapshot of '
    f'**{FINDINGS_SNAPSHOT:%d %B %Y}**. Mainboard is NSE IPOs from 2016, SME from 2018. Brackets are 95% bootstrap '
    'intervals; ρ is a rank (Spearman) correlation.'
)
if data.snapshot != FINDINGS_SNAPSHOT:
    st.warning(f'The dashboard is showing the {data.snapshot:%d %b %Y} snapshot, but these findings were written for '
               f'{FINDINGS_SNAPSHOT:%d %b %Y}. Re-run the notebooks before relying on the numbers below.')

st.header('1. Does subscription predict the listing-day gain?')
st.markdown(
    '**Yes, strongly, in both segments.**\n'
    '- ρ = 0.76 [0.72, 0.79] for mainboard and 0.70 [0.65, 0.74] for SME; within a single year, 0.72 and 0.60, '
    'so it is not just hot years against cold ones.\n'
    '- Mainboard IPOs subscribed 3.5× or less opened at a median of −1%. Those subscribed 79× or more opened at '
    '+45%, and every one of them opened above issue.\n'
    '- For SME the top-quartile median sits at the +90% ceiling, so its gap over the bottom quartile (+89 pp) is a '
    'lower bound.\n'
    '- Each doubling of subscription adds about 7 pp to the median listing gain after year and issue-size controls.'
)
figure('02_listing_gain_by_subscription_decile', 'Median listing gain by subscription decile, with the middle half shaded.')

st.header('2. Is QIB demand a stronger signal than retail or NII?')
st.markdown(
    '**For mainboard, stronger than retail. For SME, weaker than both.**\n'
    '- *Mainboard:* ρ is 0.72 for QIB and 0.59 for retail, a difference of +0.13 [+0.07, +0.19]. QIB and NII '
    'demand can\'t be told apart.\n'
    '- *SME* (issues with a QIB portion, almost all from 2023): QIB is the weakest of the three, 0.65 against 0.72 '
    'for retail, a difference of −0.07 [−0.12, −0.03]. This is borderline once the number of tests is accounted for.'
)
figure('02_category_correlations', 'Rank correlation of each category\'s subscription with the listing gain.')

st.header('3. Do listing pops persist or fade?')
st.markdown(
    '**Neither, for anyone buying on listing day.**\n'
    '- Neither the listing gain nor subscription ranks a listing-day buyer\'s next-year return over NIFTY '
    '(|ρ| ≤ 0.07 at one year).\n'
    '- The typical listing-day buyer trailed NIFTY over that year: −7.9 pp [−12.2, −0.4] for mainboard, which is '
    'borderline after adjustment, and −15.8 pp [−23.2, −9.9] for SME.\n'
    '- Allottees kept the pop and beat NIFTY on median: +16.4 pp and +10.5 pp.'
)
figure('02_listing_gain_vs_1y_return', 'One-year return over NIFTY by listing-gain quartile: allottee vs listing-day buyer.')
figure('02_fade_by_horizon', 'Correlation with the listing-day buyer\'s return at 1 month to 3 years.')

st.header('4. Does it hold in hot and cold IPO years?')
st.markdown(
    '**Mainboard: yes. SME: weaker in cold years, but that can\'t be separated from the early SME market.**\n'
    '- *Mainboard:* within-year ρ is 0.74 in hot years and 0.70 in cold years (difference +0.05 [−0.05, +0.15]).\n'
    '- *SME:* 0.70 in hot years and 0.44 in cold years (+0.26 [+0.13, +0.40]). Three of the four cold years are '
    '2018–2021, when the SME market was small.\n'
    '- The lack of any link after listing day holds in both kinds of year.'
)
figure('02_correlation_by_year', 'Rank correlation of subscription with listing gain, by listing year.')

st.header('Can a small model do better than subscription?')
st.markdown(
    '**Only slightly, and only for SME.** One logistic regression per segment, trained on listings to 2023 and tested on 2024–25, predicts '
    'whether an IPO opens above its issue price.\n'
    '- It clearly beats the training base rate and the rule "subscribed more than 10×".\n'
    '- *Mainboard:* no better than total subscription on its own (AUC 0.835 vs 0.831, difference +0.004 '
    '[−0.012, +0.020]).\n'
    '- *SME:* slightly better probabilities, from NII demand (Brier score −0.006 [−0.012, −0.001]); ranking is not '
    'clearly better.\n'
    '- Its probabilities were too optimistic for 2024–25 (87% average forecast vs 78% positive for SME), because '
    'subscription multiples rose while a given multiple listed positive less often. It would need recalibrating on '
    'recent listings. No model runs in this dashboard.'
)
figure('03_roc_test_period', 'Test period (2024 onward): the model, subscription alone and the 10× rule.')
figure('03_calibration_test_period', 'Test period: predicted vs observed share opening above issue.')

st.header('What this doesn\'t show')
st.markdown(
    '- **Not a strategy.** The most subscribed issues are the hardest to be allotted, so their listing gains are '
    'not what an applicant earns. Final-day subscription is only partly known when an application has to be made.\n'
    '- **Not causation.** These are associations in observational data.\n'
    '- **Scope.** NSE listings only; SME QIB results describe 2023 onwards; returns are price returns without '
    'dividends, against the NIFTY 50 price index; Tickertape sectors are as of the snapshot date.\n'
    '- **Many comparisons.** 24 headline intervals were reported in notebook 02. The listing-day results, mainboard '
    'QIB vs retail, the SME listing-day buyer\'s shortfall and the SME hot-vs-cold difference survive a Bonferroni '
    'adjustment; the mainboard buyer\'s shortfall and the SME QIB comparisons are borderline.\n\n'
    'Educational only, not investment advice.'
)
