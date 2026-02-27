"""
Streamlit dashboard — CFO-facing GST Reconciliation & ITC Risk Analysis.

Routes to either the CFO Dashboard or the legacy System Health / Database Tests pages.
"""

import sys
from pathlib import Path

# Ensure project root is on sys.path so 'frontend.cfo_dashboard' is importable
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import streamlit as st

# ── Page config (must be first Streamlit call) ────────
st.set_page_config(
    page_title="GST Reconciliation — CFO Dashboard",
    page_icon="📊",
    layout="wide",
)

# ── Import and launch the CFO Dashboard ───────────────
from frontend.cfo_dashboard import show_cfo_dashboard

show_cfo_dashboard()
