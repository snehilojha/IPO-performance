"""Explore: filter IPOs and see subscription against listing gain, per segment."""

import numpy as np
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from plotly.subplots import make_subplots

from app.common import (
    PLOTLY_CONFIG,
    load_data,
    log_ticks,
    segment_colors,
    segment_label,
    tick_labels,
    times,
)
from src.config import SEGMENTS
from src.dashboard import (
    NO_SECTOR,
    filter_ipos,
    subscription_groups,
    subscription_sample,
)
from src.plotting import SEGMENT_COLORS
from src.stats import spearman

MIN_FOR_CORRELATION = 20
ISSUE_SIZE_STEPS = [0, 10, 25, 50, 100, 250, 500, 1_000, 2_500, 5_000, 10_000, 30_000]

data = load_data()
mart = data.mart

st.title('Explore')
st.markdown(
    'How does total subscription relate to the listing-day gain for the IPOs you pick? Each segment gets its own '
    'panel. Charts use IPOs with both a listing price and subscription figures; SME starts in 2018.'
)

with st.sidebar:
    st.header('Filters')
    segments = st.multiselect('Segment', SEGMENTS, default=list(SEGMENTS), format_func=segment_label)
    first_year, last_year = int(mart['listing_year'].min()), int(mart['listing_year'].max())
    years = st.slider('Listing year', first_year, last_year, (first_year, last_year))
    sector_options = sorted(set(mart['sector_label']) - {NO_SECTOR}) + [NO_SECTOR]
    sectors = st.multiselect('Sector', sector_options, placeholder='All sectors',
                             help='Tickertape sector at the snapshot date. Leave empty for all sectors.')
    size_low, size_high = st.select_slider('Issue size (₹ crore)', ISSUE_SIZE_STEPS,
                                           value=(ISSUE_SIZE_STEPS[0], ISSUE_SIZE_STEPS[-1]),
                                           format_func=lambda v: f'{v:,}')

filtered = filter_ipos(mart, segments=segments or [], years=years, sectors=sectors or None,
                       issue_size_cr=(size_low, size_high))
sample = filtered[subscription_sample(filtered)]

counts = ' · '.join(f"{segment_label(s)} {int((sample['segment'] == s).sum()):,}" for s in segments)
st.markdown(f'**{len(filtered):,} IPOs match the filters; {len(sample):,} can be charted** ({counts or "no segment"}).')
if sample.empty:
    st.info('No IPOs with a listing price and subscription figures match these filters.')
    st.stop()

shown = [s for s in SEGMENTS if (sample['segment'] == s).any()]

# Scatter: one panel per segment, log subscription on x
st.subheader('Subscription vs listing-day gain')
scatter = sample.assign(
    Segment=sample['segment'].astype(str).map(segment_label),
    Listing=np.where(sample['at_listing_ceiling'], 'At the SME listing ceiling', 'Below any ceiling'),
    listing_date=sample['listing_dt'].dt.strftime('%d %b %Y'),
)
fig = px.scatter(
    scatter, x='subscription_total_x', y='listing_gain_pct', color='Segment', symbol='Listing',
    facet_col='Segment', category_orders={'Segment': [segment_label(s) for s in shown]},
    color_discrete_map=segment_colors(),
    symbol_map={'Below any ceiling': 'circle', 'At the SME listing ceiling': 'triangle-up'},
    log_x=True, opacity=0.65,
    hover_name='company_name',
    hover_data={'ticker': True, 'listing_date': True, 'issue_size_cr': ':,.0f', 'subscription_total_x': ':,.2f',
                'listing_gain_pct': ':+.1f', 'Segment': False, 'Listing': False},
    labels={'subscription_total_x': 'Total subscription (×, log scale)', 'listing_gain_pct': 'Listing gain (%)',
            'issue_size_cr': 'Issue size (₹ cr)', 'listing_date': 'Listed', 'ticker': 'Ticker'},
    height=480,
)
ticks = log_ticks(sample['subscription_total_x'].min(), sample['subscription_total_x'].max())
fig.update_xaxes(tickvals=ticks, ticktext=tick_labels(ticks))
fig.add_hline(y=0, line_color='#9ca3af', line_width=1)
fig.for_each_annotation(lambda a: a.update(text=a.text.split('=')[-1]))
fig.update_layout(legend={'title_text': '', 'orientation': 'h', 'yanchor': 'top', 'y': -0.2}, margin={'t': 40})
st.plotly_chart(fig, config=PLOTLY_CONFIG)

notes = []
for segment in shown:
    seg = sample[sample['segment'] == segment]
    if len(seg) >= MIN_FOR_CORRELATION:
        rho = spearman(seg['subscription_total_x'], seg['listing_gain_pct'])
        notes.append(f'{segment_label(segment)}: rank correlation {rho:.2f} ({len(seg):,} IPOs)')
    else:
        notes.append(f'{segment_label(segment)}: under {MIN_FOR_CORRELATION} IPOs, no correlation shown')
st.caption(
    ' · '.join(notes) + '. Triangles opened at a listing ceiling, so their gain understates demand. '
    'The correlation here has no confidence interval; the notebooks give intervals and within-year checks.'
)

# Median and IQR by equal-count subscription group
groups = subscription_groups(sample)
st.subheader('Median listing gain by subscription group')
if groups.empty:
    st.info('Too few IPOs to split into subscription groups (at least 30 per segment are needed).')
    st.stop()

fig = make_subplots(rows=1, cols=len(shown), subplot_titles=[segment_label(s) for s in shown], shared_yaxes=True)
for i, segment in enumerate(shown, start=1):
    table = groups[groups['segment'] == segment]
    if table.empty:
        continue
    ranges = [f'{times(lo)} to {times(hi)}' for lo, hi in
              zip(table['subscription_total_x_min'], table['subscription_total_x_max'])]
    fig.add_trace(go.Scatter(
        x=table['group'], y=table['median'], mode='lines+markers', name=segment_label(segment),
        line={'color': SEGMENT_COLORS[segment]}, showlegend=False,
        error_y={'type': 'data', 'symmetric': False, 'array': table['q75'] - table['median'],
                 'arrayminus': table['median'] - table['q25'], 'thickness': 1.2},
        customdata=np.column_stack([ranges, table['n'], table['q25'], table['q75'],
                                    100 * table['share_positive'], 100 * table['share_at_ceiling']]),
        hovertemplate=('Group %{x}: %{customdata[0]}<br>%{customdata[1]} IPOs<br>'
                       'Median %{y:+.1f}% (middle half %{customdata[2]:+.1f}% to %{customdata[3]:+.1f}%)<br>'
                       'Opened above issue: %{customdata[4]:.0f}%<br>At a ceiling: %{customdata[5]:.0f}%'
                       '<extra></extra>'),
    ), row=1, col=i)
    fig.update_xaxes(title_text=f'Subscription group (1 = lowest of {int(table["groups"].iloc[0])})',
                     tickmode='linear', dtick=1, row=1, col=i)
fig.update_yaxes(title_text='Listing gain (%)', row=1, col=1)
fig.add_hline(y=0, line_color='#9ca3af', line_width=1)
fig.update_layout(height=420, margin={'t': 40})
st.plotly_chart(fig, config=PLOTLY_CONFIG)
st.caption(
    'Each group holds the same number of IPOs, ranked by total subscription: deciles when there are enough, '
    'fewer groups when a filter leaves fewer than 150 IPOs. Bars show the middle half of listing gains. '
    'Where many IPOs sit at a ceiling (hover), the median is a lower bound.'
)

with st.expander('Group table'):
    table = groups.assign(
        Segment=groups['segment'].map(segment_label),
        Subscription=[f'{times(lo)} to {times(hi)}' for lo, hi in
                      zip(groups['subscription_total_x_min'], groups['subscription_total_x_max'])],
    )
    st.dataframe(
        table[['Segment', 'group', 'Subscription', 'n', 'median', 'q25', 'q75', 'share_positive', 'share_at_ceiling']],
        hide_index=True,
        column_config={
            'group': st.column_config.NumberColumn('Group'),
            'n': st.column_config.NumberColumn('IPOs'),
            'median': st.column_config.NumberColumn('Median gain', format='%+.1f%%'),
            'q25': st.column_config.NumberColumn('25th pct', format='%+.1f%%'),
            'q75': st.column_config.NumberColumn('75th pct', format='%+.1f%%'),
            'share_positive': st.column_config.NumberColumn('Above issue', format='percent'),
            'share_at_ceiling': st.column_config.NumberColumn('At a ceiling', format='percent'),
        },
    )

with st.expander('IPOs in the charts'):
    listing = sample.sort_values('listing_dt', ascending=False)
    st.dataframe(
        listing[['company_name', 'ticker', 'segment', 'listing_dt', 'sector_label', 'issue_size_cr',
                 'subscription_qib_x', 'subscription_nii_x', 'subscription_retail_x', 'subscription_total_x',
                 'listing_gain_pct', 'at_listing_ceiling']].assign(segment=listing['segment'].astype(str)),
        hide_index=True,
        column_config={
            'company_name': 'Company', 'ticker': 'Ticker', 'segment': 'Segment',
            'listing_dt': st.column_config.DateColumn('Listed', format='YYYY-MM-DD'),
            'sector_label': 'Sector',
            'issue_size_cr': st.column_config.NumberColumn('Issue size (₹ cr)', format='%.0f'),
            'subscription_qib_x': st.column_config.NumberColumn('QIB ×', format='%.2f'),
            'subscription_nii_x': st.column_config.NumberColumn('NII ×', format='%.2f'),
            'subscription_retail_x': st.column_config.NumberColumn('Retail ×', format='%.2f'),
            'subscription_total_x': st.column_config.NumberColumn('Total ×', format='%.2f'),
            'listing_gain_pct': st.column_config.NumberColumn('Listing gain', format='%+.1f%%'),
            'at_listing_ceiling': 'At ceiling',
        },
    )
