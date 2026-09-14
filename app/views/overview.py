"""Overview: headline numbers by segment, IPOs and base rates by year, and the join funnel."""

import pandas as pd
import plotly.express as px
import streamlit as st

from app.common import (
    PLOTLY_CONFIG,
    load_data,
    pct,
    pp,
    segment_colors,
    segment_label,
    share,
    with_segment_label,
)
from src.config import SEGMENTS
from src.dashboard import segment_kpis, yearly_base_rates

SMALL_COHORT = 20

data = load_data()
mart = data.mart

st.title('NSE IPOs: does subscription demand tell you anything?')
st.markdown(
    f"Every mainboard and SME IPO listed on NSE from **{mart['listing_dt'].min():%b %Y}** to "
    f"**{mart['listing_dt'].max():%b %Y}**, with subscription by investor category, the listing-day price "
    "and returns against NIFTY 50. **Short answer:** subscription strongly predicts the listing-day gain, "
    "and says nothing about the year after listing day. See **Findings**."
)

kpis = segment_kpis(mart)
columns = st.columns(len(SEGMENTS), gap='large')
for column, segment in zip(columns, SEGMENTS):
    row = kpis.loc[segment]
    with column:
        st.subheader(segment_label(segment))
        top = st.columns(2)
        top[0].metric('IPOs listed', f"{kpis.at[segment, 'ipos']:,}", border=True)
        top[1].metric('Opened above issue price', share(row['share_positive']), border=True,
                      help=f"Share of the {kpis.at[segment, 'with_listing_price']:,} IPOs with a listing-day price whose "
                           "listing-day open was above the issue price. Opening exactly at issue counts as not above.")
        bottom = st.columns(2)
        bottom[0].metric('Median listing gain', pct(row['median_listing_gain_pct']), border=True,
                         help='Listing-day open vs issue price.')
        bottom[1].metric('Median 1-year vs NIFTY', pp(row['median_excess_1y_pp']), border=True,
                         help=f"For allottees: return from issue price to one year after listing, minus NIFTY 50 over "
                              f"the same period. {kpis.at[segment, 'with_1y_return']:,} IPOs have a 1-year return; "
                              f"{share(row['share_beat_nifty_1y'])} beat the index.")

st.caption(
    'SME listing gains are cut off at a ceiling in two periods: +90% from 4 Jul 2024 (NSE pre-open cap) and an '
    'observed +20% before 26 Apr 2018. Returns are price returns, without dividends.'
)

st.header('By listing year')
yearly = with_segment_label(yearly_base_rates(mart))
left, right = st.columns(2)
with left:
    fig = px.bar(yearly, x='listing_year', y='ipos', color='Segment', barmode='group',
                 color_discrete_map=segment_colors(),
                 labels={'listing_year': 'Listing year', 'ipos': 'IPOs with a listing price'},
                 title='IPOs listed')
    fig.update_layout(legend_title_text='', margin={'t': 50})
    st.plotly_chart(fig, config=PLOTLY_CONFIG)
with right:
    fig = px.line(yearly, x='listing_year', y='share_positive', color='Segment', markers=True,
                  color_discrete_map=segment_colors(),
                  hover_data={'ipos': True, 'median_listing_gain_pct': ':+.1f'},
                  labels={'listing_year': 'Listing year', 'share_positive': 'Opened above issue price',
                          'ipos': 'IPOs', 'median_listing_gain_pct': 'Median listing gain (%)'},
                  title='Share opening above issue price')
    for trace in fig.data:  # open markers for small cohorts; px keeps each segment's rows in order
        cohort_sizes = yearly.loc[yearly['Segment'] == trace.name, 'ipos']
        trace.marker.symbol = ['circle-open' if n < SMALL_COHORT else 'circle' for n in cohort_sizes]
        trace.marker.size = 8
    fig.update_yaxes(tickformat='.0%', range=[0, 1.02])
    fig.update_layout(legend_title_text='', margin={'t': 50})
    st.plotly_chart(fig, config=PLOTLY_CONFIG)
partial_year = ''
if mart['listing_year'].max() == data.snapshot.year:
    partial_year = f' {data.snapshot.year} only runs to the snapshot date.'
st.caption(f'Open markers: years with under {SMALL_COHORT} IPOs in that segment. Hover for the median listing gain.'
           + partial_year)

st.header('How the data was joined')
st.markdown(
    'Row counts at each stage of the build, from the raw NSE feed to the mart. From step 5 on, each count is a '
    'subset of the mart: IPOs are never dropped for missing subscription or market data, they are flagged instead. '
    'SME subscription figures effectively start in 2018.'
)
funnel = data.funnel.rename(columns={'step': 'Step', 'description': 'What is counted', 'ALL': 'All',
                                     'MAINBOARD': 'Mainboard', 'SME': 'SME'})
counts = ['Mainboard', 'SME', 'All']
# Step 1 (the raw feed) has no segment split; show a dash rather than None
funnel[counts] = funnel[counts].apply(lambda col: col.map(lambda n: '–' if pd.isna(n) else f'{int(n):,}'))
st.dataframe(funnel[['Step', 'What is counted', *counts]], hide_index=True, height=36 * (len(funnel) + 1),
             column_config={c: st.column_config.TextColumn(c, width='small') for c in counts})
st.caption(
    'Sources: NSE past issues and daily bhavcopy (prices, NIFTY 50, corporate actions), Chittorgarh (subscription), '
    'Tickertape (sector). Sector is as listed at the snapshot date. Educational only, not investment advice.'
)
