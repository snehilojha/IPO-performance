"""IPO lookup: one company's subscription, listing day and returns against NIFTY."""

import pandas as pd
import plotly.express as px
import streamlit as st

from app.common import (
    PLOTLY_CONFIG,
    load_data,
    log_ticks,
    pct,
    pp,
    rupees,
    segment_label,
    share,
    tick_labels,
    times,
)
from src.dashboard import (
    lookup_labels,
    percentile_in_segment,
    return_history,
    subscription_breakdown,
)
from src.plotting import SEGMENT_COLORS

HORIZON_NAMES = {'1m': '1 month', '6m': '6 months', '1y': '1 year', '3y': '3 years'}

data = load_data()
mart = data.mart

st.title('IPO lookup')
labels = lookup_labels(mart)  # unique, so a label identifies one row
row_by_label = dict(zip(labels, labels.index))
options = list(row_by_label)

# ?ticker=SYMBOL opens that IPO (the latest listing if a symbol was reused), so a lookup can be shared as a link
linked = mart.loc[mart['ticker'] == st.query_params.get('ticker', '').upper()].sort_values('listing_dt')
default = options.index(labels.loc[linked.index[-1]]) if not linked.empty else None
choice = st.selectbox('Company', options, index=default, placeholder='Type a company name or ticker')
if choice is None:
    st.query_params.pop('ticker', None)
    st.info(f'Search any of the {len(mart):,} NSE-listed IPOs in the snapshot.')
    st.stop()

ipo = mart.loc[row_by_label[choice]]
st.query_params['ticker'] = ipo['ticker']
segment = str(ipo['segment'])
st.header(ipo['company_name'].strip())
details = [f"**{ipo['ticker']}**", segment_label(segment)]
if pd.notna(ipo['sector']):
    details.append(f"{ipo['sector']} (sector at the snapshot date)")
if pd.notna(ipo['current_ticker']) and ipo['current_ticker'] != ipo['ticker']:
    details.append(f"now trades as {ipo['current_ticker']}")
st.markdown(' · '.join(details))

issue = (f"Issue open {ipo['ipo_start_dt']:%d %b %Y} to {ipo['ipo_end_dt']:%d %b %Y}"
         if pd.notna(ipo['ipo_start_dt']) else f"Issue closed {ipo['ipo_end_dt']:%d %b %Y}")
listed = (f"Listed {ipo['listing_dt']:%d %b %Y}" if pd.notna(ipo['listing_dt'])
          else 'No NSE listing (listed on BSE only)')
st.caption(f"{issue} · {listed} · Issue size {rupees(ipo['issue_size_cr'], ' cr')}")

metrics = st.columns(4)
metrics[0].metric('Issue price', rupees(ipo['issue_price']), border=True)
metrics[1].metric('Listing-day open', rupees(ipo['listing_open']), border=True,
                  help='First price on NSE on the listing day.')
gain_rank = percentile_in_segment(mart, ipo, 'listing_gain_pct')
metrics[2].metric('Listing gain', pct(ipo['listing_gain_pct']), border=True,
                  help='Listing-day open vs issue price.'
                       + (f' Higher than {share(gain_rank)} of {segment_label(segment)} IPOs.' if pd.notna(gain_rank) else ''))
subscription_rank = percentile_in_segment(mart[mart['has_subscription_figures']], ipo, 'subscription_total_x')
metrics[3].metric('Total subscription', times(ipo['subscription_total_x']), border=True,
                  help=(f'Higher than {share(subscription_rank)} of {segment_label(segment)} IPOs with subscription figures.'
                        if pd.notna(subscription_rank) else 'No subscription figures for this IPO.'))
if ipo['at_listing_ceiling']:
    st.warning('This listing opened at the SME listing ceiling, so the listing gain understates demand.')
if pd.notna(ipo['listing_day_close_gain_pct']):
    st.caption(f"Listing-day close {rupees(ipo['listing_close'])} ({pct(ipo['listing_day_close_gain_pct'])} vs issue).")

left, right = st.columns([2, 3], gap='large')
with left:
    st.subheader('Subscription by investor category')
    breakdown = subscription_breakdown(ipo)
    reported = breakdown.dropna(subset=['times_subscribed'])
    if reported.empty:
        st.info('No subscription figures were found for this IPO.')
    else:
        fig = px.bar(reported, x='category', y='times_subscribed', log_y=True, text='times_subscribed',
                     labels={'category': '', 'times_subscribed': 'Times subscribed (log scale)'})
        fig.update_traces(marker_color=SEGMENT_COLORS[segment], texttemplate='%{y:,.2f}×', textposition='outside',
                          hovertemplate='%{x}: %{y:,.2f}×<extra></extra>')
        ticks = log_ticks(reported['times_subscribed'].min(), reported['times_subscribed'].max())
        fig.update_yaxes(tickvals=ticks, ticktext=tick_labels(ticks))
        fig.update_layout(height=340, margin={'t': 20}, uniformtext_minsize=9)
        st.plotly_chart(fig, config=PLOTLY_CONFIG)
    missing = breakdown[breakdown['note'] != '']
    if not missing.empty:
        st.caption(' · '.join(f'{r.category}: {r.note}' for r in missing.itertuples()))

with right:
    st.subheader('Returns against NIFTY 50')
    history = return_history(ipo)
    shown = history.assign(
        Horizon=history['horizon'].map(HORIZON_NAMES),
        **{
            'From issue price': history['return_pct'].map(pct),
            'NIFTY 50': history['nifty_return_pct'].map(pct),
            'Allottee vs NIFTY': history['excess_return_pp'].map(pp),
            'Listing-day buyer vs NIFTY': history['aftermarket_excess_return_pp'].map(pp),
            'Note': history['status'].where(history['status'] != 'ok', ''),
        },
    )
    st.dataframe(shown[['Horizon', 'From issue price', 'NIFTY 50', 'Allottee vs NIFTY', 'Listing-day buyer vs NIFTY',
                        'Note']], hide_index=True)
    st.caption(
        'Horizons count from the listing date. *Allottee:* return from the issue price, minus NIFTY 50 from issue '
        'close to the same date. *Listing-day buyer:* the same comparison from the listing-day close. '
        'Adjusted for splits and bonuses; dividends are not included.'
    )
    if ipo['corporate_action_count'] > 0:
        st.caption(f"{int(ipo['corporate_action_count'])} split or bonus adjustment(s) applied.")
    trading = ipo['is_trading_at_snapshot']
    if pd.notna(ipo['last_trade_dt']) and pd.notna(trading) and not trading:
        st.caption(f"Last NSE trade on {ipo['last_trade_dt']:%d %b %Y}; no longer trading at the snapshot date.")
