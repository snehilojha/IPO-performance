# What IPO subscription numbers can and can't tell you

*Indian IPOs listed on NSE, 2016 to January 2026. Data as of 15 February 2026.*

When a company goes public in India, investors apply for shares over a few days. The exchanges publish how many times over the issue was **subscribed**, overall and by investor type:
- **institutions (QIBs)**;
- **wealthy individuals (NIIs)**;
- **retail investors**.

An issue subscribed 50× received applications for fifty times the shares on offer. These figures are widely watched. We asked whether they actually predict how the shares do.

We looked at 1,143 IPOs and kept the two markets apart throughout:
- **Mainboard:** larger companies, 452 IPOs.
- **SME platform:** small companies, 691 IPOs.

## What we found

**1. Heavily subscribed IPOs do open higher on listing day.** The link is strong in both markets. Among mainboard IPOs:
- the least subscribed quarter (3.6× or less) opened at about their issue price;
- the most subscribed quarter (79× or more) opened a typical 45% above it;
- every one of that top quarter opened above its issue price.

This holds within individual years too, so it isn't just a matter of hot and cold markets.

**2. After listing day, subscription tells you nothing.** Suppose you bought shares at the first day's closing price. Over the following year, neither the subscription figure nor the size of the first-day jump said anything about how you would do against the NIFTY 50 index. The typical buyer did worse than the index:
- about 8 percentage points behind for mainboard IPOs;
- about 16 points behind for SME IPOs.

Investors who were *allotted* shares at the issue price did better, but mostly because they kept the first-day jump.

**3. Who is bidding matters, differently in each market.** For mainboard IPOs, strong institutional demand was a better guide to the first-day price than strong retail demand. For recent SME IPOs it was the other way round: institutional demand was the weakest guide of the three.

**4. A simple prediction model barely improves on the headline figure.** We trained a model on IPOs up to 2023 and tested it on 2024–25 listings. Its task was to predict whether an IPO would open above its issue price.
- **Mainboard:** it did no better than the subscription figure alone.
- **SME:** it did slightly better.
- **Both markets:** it was too optimistic in 2024–25. Subscription figures rose, but a given figure led to a first-day gain less often than before.

## Why this is not a trading strategy

- **You rarely get the shares.** The IPOs most likely to jump are the most oversubscribed, so an applicant usually receives few or no shares. The average first-day gain is not what an applicant earns.
- **The final numbers come too late.** The final-day subscription figures are only partly known when you have to decide whether to apply.
- **SME first-day gains hit a ceiling.** Since July 2024, NSE has capped how far an SME IPO can open above its issue price, at 90%. Before April 2018, no SME IPO opened more than 20% above.
- **Returns leave out dividends.** Companies that stopped trading have no returns after their last trade. Sector and valuation figures are as of February 2026, not the listing date.
- **This shows associations, not causes.**

## Where to look next

- **[Dashboard](https://snehilojha-ipo-performance-appstreamlit-app-ctowzl.streamlit.app/):** search any IPO, or filter by year, sector and size.
- **Full analysis with confidence intervals:** [notebook 02](../notebooks/02_subscription_vs_returns.ipynb) and [notebook 03](../notebooks/03_listing_gain_model.ipynb).

*For education only. Not investment advice.*
