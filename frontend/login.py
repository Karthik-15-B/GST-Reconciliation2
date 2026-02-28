"""
Streamlit Login UI — authenticates against POST /auth/login.

Sets st.session_state keys on success:
    authenticated  (bool)
    username       (str)
    role           (str)   — CFO | CA | INSPECTOR
    gstin          (str)   — only meaningful for CFO
    name           (str)
    clients        (list)  — only meaningful for CA
"""

import os
import requests
import streamlit as st
from dotenv import load_dotenv

load_dotenv()

BACKEND = os.getenv("BACKEND_URL", "http://localhost:8000")


def _init_session():
    """Ensure all auth keys exist in session_state."""
    defaults = {
        "authenticated": False,
        "username": "",
        "role": "",
        "gstin": "",
        "name": "",
        "clients": [],
    }
    for k, v in defaults.items():
        if k not in st.session_state:
            st.session_state[k] = v


def show_login():
    """Render the login page and handle authentication."""
    _init_session()

    # ── Custom CSS ──
    st.markdown(
        """
        <style>
        .login-header {
            text-align: center;
            padding: 2rem 0 1rem 0;
        }
        .login-header h1 {
            color: #1565c0;
            margin-bottom: 0.2rem;
        }
        .login-header p {
            color: #757575;
            font-size: 1rem;
        }
        div[data-testid="stForm"] {
            max-width: 420px;
            margin: 0 auto;
            padding: 2rem;
            border: 1px solid #e0e0e0;
            border-radius: 12px;
            background: #fafafa;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )

    # ── Header ──
    st.markdown(
        """
        <div class="login-header">
            <h1>GST Intelligence System</h1>
            <p>Knowledge Graph &amp; Reconciliation Platform</p>
        </div>
        """,
        unsafe_allow_html=True,
    )

    # ── Login form ──
    with st.form("login_form"):
        username = st.text_input("Username", placeholder="GSTIN / ca_demo / inspector_demo")
        password = st.text_input("Password", type="password", placeholder="Enter password")
        submitted = st.form_submit_button("Login", use_container_width=True)

    if submitted:
        if not username or not password:
            st.error("Please enter both username and password.")
            return

        try:
            resp = requests.post(
                f"{BACKEND}/auth/login",
                json={"username": username.strip(), "password": password},
                timeout=15,
            )
        except requests.exceptions.RequestException as e:
            st.error(f"Cannot reach backend: {e}")
            return

        if resp.status_code == 200:
            data = resp.json()
            st.session_state.authenticated = True
            st.session_state.username = data["username"]
            st.session_state.role = data["role"]
            st.session_state.gstin = data.get("gstin", "")
            st.session_state.name = data.get("name", "")
            st.session_state.clients = data.get("clients", [])
            st.rerun()
        elif resp.status_code == 401:
            detail = resp.json().get("detail", "Invalid credentials")
            st.error(f"Login failed: {detail}")
        else:
            st.error(f"Unexpected error ({resp.status_code})")

    # ── Help text ──
    st.markdown("---")
    with st.expander("Demo Credentials"):
        st.markdown(
            """
            | Role | Username | Password |
            |------|----------|----------|
            | **CFO** | Any GSTIN from Taxpayers | `demo@123` |
            | **CA** | `ca_demo` | `demo@123` |
            | **Inspector** | `inspector_demo` | `demo@123` |
            """
        )


def logout():
    """Clear all session state and rerun to show login."""
    for key in list(st.session_state.keys()):
        del st.session_state[key]
    st.rerun()


def is_authenticated() -> bool:
    """Check if user is currently authenticated."""
    return st.session_state.get("authenticated", False)


def require_role(allowed_role: str):
    """Block access if user's role doesn't match. Call at top of each dashboard."""
    if st.session_state.get("role") != allowed_role:
        st.error(f"Access denied. This dashboard requires **{allowed_role}** role.")
        st.stop()
