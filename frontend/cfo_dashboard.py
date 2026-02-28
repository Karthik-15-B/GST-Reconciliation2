"""
CFO Dashboard — Decision-focused GST compliance & payment risk view.

Answers three questions:
    1. Should I release payment?
    2. How much ITC is blocked?
    3. Which vendors are risky?

No graph visualisations. No fraud investigation. Read-only.

Entry point:  show_cfo_dashboard()
"""

import os
import requests
import pandas as pd
import streamlit as st
from dotenv import load_dotenv

load_dotenv()

BACKEND = os.getenv("BACKEND_URL", "http://localhost:8000")
TIMEOUT = 30


# ═══════════════════════════════════════════════════════
# API HELPER
# ═══════════════════════════════════════════════════════

def _get(path: str, timeout: int = TIMEOUT):
    """GET request to backend. Returns JSON dict or None on error."""
    try:
        r = requests.get(f"{BACKEND}{path}", timeout=timeout)
        r.raise_for_status()
        return r.json()
    except requests.exceptions.HTTPError as e:
        if e.response is not None and e.response.status_code == 404:
            return None
        st.error(f"Backend error: {e}")
        return None
    except requests.exceptions.RequestException as e:
        st.error(f"Cannot reach backend: {e}")
        return None


# ═══════════════════════════════════════════════════════
# SHOW_CFO_DASHBOARD — entry point
# ═══════════════════════════════════════════════════════

def show_cfo_dashboard():
    """Render the CFO Dashboard UI — payment decisions only."""
    from frontend.login import require_role, logout
    require_role("CFO")

    # ── CSS ──
    st.markdown(
        """
        <style>
        .cfo-banner {
            background: linear-gradient(135deg, #1a237e, #0d47a1);
            color: #ffffff;
            padding: 1.4rem 2rem;
            border-radius: 10px;
            margin-bottom: 1.2rem;
        }
        .cfo-banner h2 { color: #ffffff; margin: 0; }
        .cfo-banner p  { color: #bbdefb; margin: 0.25rem 0 0 0; font-size: 0.95rem; }
        div[data-testid="stMetric"] {
            background: #f0f2f6;
            border: 1px solid #e0e0e0;
            border-radius: 8px;
            padding: 10px 14px;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )

    # ══════════════════════════════════════════════════
    # SIDEBAR
    # ══════════════════════════════════════════════════
    st.sidebar.title("CFO Dashboard")
    st.sidebar.caption("GST Compliance & Payment Decisions")
    st.sidebar.divider()

    gstin = st.session_state.get("gstin", "")
    if not gstin:
        st.error("No GSTIN associated with this account.")
        st.stop()

    user_name = st.session_state.get("name", gstin)
    st.sidebar.markdown(f"**Company:** {user_name}")
    st.sidebar.markdown(f"**GSTIN:** `{gstin}`")
    st.sidebar.divider()

    menu = st.sidebar.selectbox(
        "Menu",
        ["Dashboard", "Reports", "Exports", "Logout"],
    )

    st.sidebar.divider()

    # Health indicator
    health = _get("/health")
    if health:
        hc1, hc2 = st.sidebar.columns(2)
        hc1.markdown(f"**MongoDB:** {'🟢' if health.get('mongodb') == 'UP' else '🔴'}")
        hc2.markdown(f"**Neo4j:** {'🟢' if health.get('neo4j') == 'UP' else '🔴'}")

    # ══════════════════════════════════════════════════
    # PAGE ROUTING
    # ══════════════════════════════════════════════════
    if menu == "Dashboard":
        _page_dashboard(gstin)
    elif menu == "Reports":
        _page_reports(gstin)
    elif menu == "Exports":
        _page_exports(gstin)
    elif menu == "Logout":
        _page_logout()


# ═══════════════════════════════════════════════════════
# PAGE: DASHBOARD
# ═══════════════════════════════════════════════════════

def _page_dashboard(gstin: str):
    data = _get(f"/dashboard/overview/{gstin}")
    if not data:
        st.info("No records found for this GSTIN.")
        return

    taxpayer = data.get("taxpayer", {})
    itc = data.get("itc_summary", {})
    purchase_reg = data.get("purchase_register", [])
    gstr2b = data.get("gstr2b", [])
    vendor_risk = data.get("vendor_risk", [])
    warnings = data.get("payment_warnings", [])

    # ── 1. Company Header ──
    risk_cat = taxpayer.get("Risk_Category", "N/A")
    risk_badge = {"HIGH": "🔴 HIGH", "MEDIUM": "🟡 MEDIUM", "LOW": "🟢 LOW"}.get(
        risk_cat, risk_cat
    )
    st.markdown(
        f"""
        <div class="cfo-banner">
            <h2>{taxpayer.get("Name", "N/A")}</h2>
            <p>GSTIN: {gstin} &nbsp;&nbsp;|&nbsp;&nbsp; Risk: {risk_badge}</p>
        </div>
        """,
        unsafe_allow_html=True,
    )

    # ── 2. ITC Summary ──
    st.subheader("ITC Summary (from GSTR2B)")
    c1, c2, c3 = st.columns(3)
    c1.metric("Total ITC", f"₹ {itc.get('total_itc', 0):,.2f}")
    c2.metric("Eligible ITC", f"₹ {itc.get('eligible_itc', 0):,.2f}")
    blocked = itc.get("blocked_itc", 0)
    c3.metric(
        "Blocked ITC",
        f"₹ {blocked:,.2f}",
        delta=f"-₹ {blocked:,.2f}" if blocked > 0 else None,
        delta_color="inverse",
    )
    st.divider()

    # ── 3. Purchase Register Table ──
    st.subheader("Purchase Register")
    if purchase_reg:
        df_pr = pd.DataFrame(purchase_reg)[
            ["Invoice_ID", "Seller_GSTIN", "Value_Claimed", "Tax_Claimed", "Claim_Date"]
        ]
        for col in ("Value_Claimed", "Tax_Claimed"):
            df_pr[col] = pd.to_numeric(df_pr[col], errors="coerce")
        st.dataframe(df_pr, width="stretch", hide_index=True)
    else:
        st.info("No records found for this GSTIN.")
    st.divider()

    # ── 4. GSTR2B Records Table ──
    st.subheader("GSTR2B Records")
    if gstr2b:
        df_g2b = pd.DataFrame(gstr2b)[
            ["Invoice_ID", "Seller_GSTIN", "Value", "Tax", "ITC_Eligible"]
        ]
        for col in ("Value", "Tax"):
            df_g2b[col] = pd.to_numeric(df_g2b[col], errors="coerce")

        def _itc_color(val):
            if val == "NO":
                return "background-color: #ffcdd2; color: #b71c1c;"
            if val == "YES":
                return "background-color: #c8e6c9; color: #1b5e20;"
            return ""

        styled = df_g2b.style.map(_itc_color, subset=["ITC_Eligible"])
        st.dataframe(styled, width="stretch", hide_index=True)
    else:
        st.info("No records found for this GSTIN.")
    st.divider()

    # ── 5. Vendor Risk Panel ──
    st.subheader("Vendor Risk Panel")
    if vendor_risk:
        rows = []
        for v in vendor_risk:
            rows.append({
                "Vendor GSTIN": v["gstin"],
                "Vendor Name": v["name"],
                "Risk Level": v["risk_level"],
                "Reason": " | ".join(v["reasons"]),
            })
        df_vr = pd.DataFrame(rows)

        def _risk_color(val):
            if val == "HIGH":
                return "background-color: #ffcdd2; color: #b71c1c;"
            if val == "MEDIUM":
                return "background-color: #ffe0b2; color: #e65100;"
            return ""

        styled_vr = df_vr.style.map(_risk_color, subset=["Risk Level"])
        st.dataframe(styled_vr, width="stretch", hide_index=True)
    else:
        st.info("No vendor risk data available.")
    st.divider()

    # ── 6. Payment Warnings ──
    st.subheader("Payment Warnings")
    if warnings:
        for w in warnings:
            if w.get("severity") == "CRITICAL":
                st.error(w["message"])
            else:
                st.warning(w["message"])
    else:
        st.success("No payment warnings — all invoices clear for release.")


# ═══════════════════════════════════════════════════════
# PAGE: REPORTS (Executive View)
# ═══════════════════════════════════════════════════════

def _page_reports(gstin: str):
    st.markdown(
        '<div class="cfo-banner"><h2>Executive Report</h2>'
        "<p>Compliance summary &amp; action items</p></div>",
        unsafe_allow_html=True,
    )

    data = _get(f"/dashboard/overview/{gstin}")
    if not data:
        st.info("No records found for this GSTIN.")
        return

    itc = data.get("itc_summary", {})
    vendor_risk = data.get("vendor_risk", [])
    warnings = data.get("payment_warnings", [])
    purchase_reg = data.get("purchase_register", [])
    gstr2b = data.get("gstr2b", [])

    # ── Executive Summary Metrics ──
    st.subheader("Executive Summary")
    e1, e2, e3, e4 = st.columns(4)
    e1.metric("Total ITC", f"₹ {itc.get('total_itc', 0):,.2f}")
    e2.metric("Blocked ITC", f"₹ {itc.get('blocked_itc', 0):,.2f}")
    high_risk_count = sum(1 for v in vendor_risk if v.get("risk_level") == "HIGH")
    e3.metric("High-Risk Vendors", high_risk_count)
    e4.metric("Payment Alerts", len(warnings))
    st.divider()

    # ── Action Items ──
    st.subheader("Action Items")
    action_count = 0

    for w in warnings:
        action_count += 1
        if w.get("severity") == "CRITICAL":
            st.error(f"**Action {action_count}:** {w['message']}")
        else:
            st.warning(f"**Action {action_count}:** {w['message']}")

    for v in vendor_risk:
        if v.get("risk_level") == "HIGH":
            action_count += 1
            st.warning(
                f"**Action {action_count}:** Review vendor "
                f"{v['name']} ({v['gstin']}) — {' | '.join(v['reasons'])}"
            )

    if action_count == 0:
        st.success("No action items — all clear.")

    st.divider()

    # ── Export buttons ──
    st.subheader("Download Reports")
    col_a, col_b = st.columns(2)
    if purchase_reg:
        with col_a:
            df = pd.DataFrame(purchase_reg)
            st.download_button(
                "Download Purchase Register (CSV)",
                df.to_csv(index=False),
                f"purchase_register_{gstin}.csv",
                "text/csv",
            )
    if gstr2b:
        with col_b:
            df = pd.DataFrame(gstr2b)
            st.download_button(
                "Download GSTR2B Records (CSV)",
                df.to_csv(index=False),
                f"gstr2b_{gstin}.csv",
                "text/csv",
            )


# ═══════════════════════════════════════════════════════
# PAGE: EXPORTS (Dedicated download page)
# ═══════════════════════════════════════════════════════

def _page_exports(gstin: str):
    st.markdown(
        '<div class="cfo-banner"><h2>Data Exports</h2>'
        "<p>Download compliance data as CSV</p></div>",
        unsafe_allow_html=True,
    )

    data = _get(f"/dashboard/overview/{gstin}")
    if not data:
        st.info("No records found for this GSTIN.")
        return

    purchase_reg = data.get("purchase_register", [])
    gstr2b = data.get("gstr2b", [])
    vendor_risk = data.get("vendor_risk", [])

    st.subheader("Available Exports")

    col_a, col_b, col_c = st.columns(3)

    if purchase_reg:
        with col_a:
            df = pd.DataFrame(purchase_reg)
            st.metric("Purchase Register", f"{len(df)} rows")
            st.download_button(
                "Download CSV",
                df.to_csv(index=False),
                f"purchase_register_{gstin}.csv",
                "text/csv",
                key="exp_pr",
            )
    else:
        with col_a:
            st.info("No purchase register data.")

    if gstr2b:
        with col_b:
            df = pd.DataFrame(gstr2b)
            st.metric("GSTR2B Records", f"{len(df)} rows")
            st.download_button(
                "Download CSV",
                df.to_csv(index=False),
                f"gstr2b_{gstin}.csv",
                "text/csv",
                key="exp_g2b",
            )
    else:
        with col_b:
            st.info("No GSTR2B data.")

    if vendor_risk:
        with col_c:
            rows = [{
                "Vendor GSTIN": v["gstin"],
                "Vendor Name": v["name"],
                "Risk Level": v["risk_level"],
                "Reason": " | ".join(v["reasons"]),
            } for v in vendor_risk]
            df = pd.DataFrame(rows)
            st.metric("Vendor Risk Report", f"{len(df)} vendors")
            st.download_button(
                "Download CSV",
                df.to_csv(index=False),
                f"vendor_risk_{gstin}.csv",
                "text/csv",
                key="exp_vr",
            )
    else:
        with col_c:
            st.info("No vendor risk data.")


# ═══════════════════════════════════════════════════════
# PAGE: LOGOUT
# ═══════════════════════════════════════════════════════

def _page_logout():
    from frontend.login import logout
    logout()
