"""
Streamlit entry point — login-gated, role-based dashboard routing.

Authentication flow:
  1. Not authenticated → show login page
  2. Authenticated    → route to correct dashboard based on role
  3. Logout           → clear session, back to login

Roles:
  CFO       → cfo_dashboard   (own GSTIN only)
  CA        → ca_dashboard    (assigned clients only)
  INSPECTOR → inspector_dashboard (full access)
"""

import sys
from pathlib import Path

# Ensure project root is on sys.path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import streamlit as st

# ── Page config (must be first Streamlit call) ────────
st.set_page_config(
    page_title="GST Reconciliation System",
    page_icon="📊",
    layout="wide",
)

# ── Auth imports ──────────────────────────────────────
from frontend.login import show_login, is_authenticated, logout

# ── Gate: not logged in → show login ─────────────────
if not is_authenticated():
    show_login()
    st.stop()

# ── User is authenticated — build sidebar ─────────────
role = st.session_state.get("role", "")
name = st.session_state.get("name", st.session_state.get("username", ""))

st.sidebar.markdown(f"**Logged in as:** {name}")
st.sidebar.markdown(f"**Role:** {role}")
st.sidebar.divider()

if st.sidebar.button("Logout", use_container_width=True):
    logout()

st.sidebar.divider()

# ── Route to the correct dashboard ────────────────────
if role == "CFO":
    from frontend.cfo_dashboard import show_cfo_dashboard
    show_cfo_dashboard()
elif role == "CA":
    from frontend.ca_dashboard import show_ca_dashboard
    show_ca_dashboard()
elif role == "INSPECTOR":
    from frontend.inspector_dashboard import show_inspector_dashboard
    show_inspector_dashboard()
else:
    st.error(f"Unknown role: {role}")
    logout()

