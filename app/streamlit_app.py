"""
Dashboard over the mart export: overview, explore, IPO lookup and findings.
Run from the repo root: streamlit run app/streamlit_app.py
"""

import sys
from pathlib import Path

import streamlit as st

# Streamlit puts app/ on the path; the analysis helpers live in src/ at the repo root
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.common import load_data

st.set_page_config(page_title='NSE IPOs: subscription and returns', page_icon='📈', layout='wide')

VIEWS = PROJECT_ROOT / 'app' / 'views'
pages = st.navigation([
    st.Page(VIEWS / 'overview.py', title='Overview', default=True),
    st.Page(VIEWS / 'explore.py', title='Explore'),
    st.Page(VIEWS / 'lookup.py', title='IPO lookup'),
    st.Page(VIEWS / 'findings.py', title='Findings'),
])

data = load_data()
st.sidebar.caption(
    f"Snapshot {data.snapshot:%d %b %Y} · {len(data.mart):,} NSE-listed IPOs  \n"
    "Mainboard and SME are shown side by side, never pooled."
)
pages.run()
