"""
CA Reconciliation Dashboard — Production-grade Streamlit UI.

Multi-GSTIN reconciliation dashboard for Chartered Accountants.
Compares Purchase Register ↔ GSTR2B, detects missing ITC,
checks filing status, and generates explainable compliance output.

Consumes FastAPI backend at http://localhost:8000 (read-only).

Entry point:  show_ca_dashboard()
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
# API HELPERS
# ═══════════════════════════════════════════════════════

def _get(path: str, timeout: int = TIMEOUT):
    """GET request to backend. Returns JSON dict or None on error."""
    try:
        r = requests.get(f"{BACKEND}{path}", timeout=timeout)
        r.raise_for_status()
        return r.json()
    except requests.exceptions.HTTPError as e:
        if e.response is not None and e.response.status_code in (404, 403):
            return None
        st.error(f"Backend error: {e}")
        return None
    except requests.exceptions.RequestException as e:
        st.error(f"Cannot reach backend: {e}")
        return None


# ═══════════════════════════════════════════════════════
# SECTION 7 — EXPLAINABLE OUTPUT
# ═══════════════════════════════════════════════════════

def generate_ca_explanation(invoice_detail: dict) -> list[str]:
    """Generate human-readable reconciliation explanations from invoice detail."""
    if not invoice_detail:
        return ["Invoice not found."]
    return invoice_detail.get("explanations", ["No explanation available."])


# ═══════════════════════════════════════════════════════
# SHOW_CA_DASHBOARD — entry point
# ═══════════════════════════════════════════════════════

def show_ca_dashboard():
    """Render the entire CA Reconciliation Dashboard UI."""
    from frontend.login import require_role, logout
    require_role("CA")

    # ── Custom CSS ──
    st.markdown(
        """
        <style>
        .ca-banner {
            background: linear-gradient(135deg, #1b5e20, #2e7d32);
            color: #ffffff;
            padding: 1.2rem 2rem;
            border-radius: 10px;
            margin-bottom: 1rem;
        }
        .ca-banner h2 { color: #ffffff; margin: 0; }
        .ca-banner p  { color: #c8e6c9; margin: 0.2rem 0 0 0; font-size: 0.95rem; }
        div[data-testid="stMetric"] {
            background: #f0f2f6;
            border: 1px solid #e0e0e0;
            border-radius: 8px;
            padding: 10px 14px;
        }
        .status-matched  { color: #2e7d32; font-weight: bold; }
        .status-mismatch { color: #e65100; font-weight: bold; }
        .status-missing  { color: #c62828; font-weight: bold; }
        </style>
        """,
        unsafe_allow_html=True,
    )

    # ══════════════════════════════════════════════════
    # SIDEBAR (Section 9)
    # ══════════════════════════════════════════════════
    st.sidebar.title("CA Dashboard")
    st.sidebar.caption("GST Reconciliation System")
    st.sidebar.divider()

    menu = st.sidebar.selectbox(
        "Menu",
        ["Dashboard", "Reconciliation", "Search", "Reports", "Logout"],
    )

    st.sidebar.divider()

    # ── Load clients (filtered to authenticated user's assigned GSTINs) ──
    allowed_clients = st.session_state.get("clients", [])
    if not allowed_clients:
        st.error("No client GSTINs assigned to this account.")
        st.stop()

    clients_resp = _get("/ca/clients")
    if not clients_resp or not clients_resp.get("clients"):
        st.error("Could not load client list. Is the backend running on port 8000?")
        return

    # Filter to only the GSTINs this CA user is authorised to see
    all_clients = clients_resp["clients"]
    clients_list = [c for c in all_clients if c["GSTIN"] in allowed_clients]

    gstin_to_name = {c["GSTIN"]: c.get("Name", c["GSTIN"]) for c in clients_list}

    # ── Health indicator ──
    health = _get("/health")
    if health:
        hc1, hc2 = st.sidebar.columns(2)
        hc1.markdown(f"**MongoDB:** {'🟢' if health.get('mongodb') == 'UP' else '🔴'}")
        hc2.markdown(f"**Neo4j:** {'🟢' if health.get('neo4j') == 'UP' else '🔴'}")
    st.sidebar.divider()
    st.sidebar.info(f"**User:** {st.session_state.get('username', 'ca_demo')}\n\n**Clients:** {len(clients_list)}")

    # ══════════════════════════════════════════════════
    # PAGE ROUTING
    # ══════════════════════════════════════════════════
    if menu == "Dashboard":
        _page_dashboard(gstin_to_name)
    elif menu == "Reconciliation":
        _page_reconciliation(gstin_to_name)
    elif menu == "Search":
        _page_search(gstin_to_name)
    elif menu == "Reports":
        _page_reports(gstin_to_name)
    elif menu == "Logout":
        _page_logout()


# ═══════════════════════════════════════════════════════
# PAGE: DASHBOARD (Sections 1-5, 8)
# ═══════════════════════════════════════════════════════

def _page_dashboard(gstin_to_name: dict):
    # ── Title ──
    st.markdown(
        """
        <div class="ca-banner">
            <h2>CA Reconciliation Dashboard</h2>
            <p>Multi-client GST reconciliation &amp; ITC risk analysis</p>
        </div>
        """,
        unsafe_allow_html=True,
    )

    # ── Section 1: Client Selection ──
    client_gstin = st.selectbox(
        "Select Client GSTIN",
        st.session_state.clients,
        format_func=lambda g: f"{gstin_to_name.get(g, g)}  |  {g}",
    )

    if not client_gstin:
        st.info("No records for selected GSTIN")
        return

    # ── Fetch overview ──
    data = _get(f"/ca/overview/{client_gstin}")
    if not data:
        st.info("No records for selected GSTIN")
        return

    taxpayer = data.get("taxpayer", {})
    itc = data.get("itc_summary", {})
    recon = data.get("reconciliation", [])
    missing = data.get("missing_itc", [])
    filing = data.get("filing_status", [])
    vendor_risk = data.get("vendor_risk", [])

    st.divider()

    # ── Section 2: Client Summary ──
    st.subheader(f"Client Summary — {taxpayer.get('Name', client_gstin)}")
    m1, m2, m3, m4, m5 = st.columns(5)
    m1.metric("GSTIN", client_gstin)
    m2.metric("Total Invoices", itc.get("total_invoices", 0))
    m3.metric("Total ITC Claimed", f"₹ {itc.get('total_itc', 0):,.2f}")
    m4.metric("Eligible ITC", f"₹ {itc.get('eligible_itc', 0):,.2f}")
    blocked = itc.get("blocked_itc", 0)
    m5.metric(
        "Blocked ITC",
        f"₹ {blocked:,.2f}",
        delta=f"-₹ {blocked:,.2f}" if blocked > 0 else None,
        delta_color="inverse",
    )
    st.divider()

    # ── Section 3: Reconciliation Engine ──
    st.subheader("Reconciliation — Purchase Register vs GSTR2B")
    if recon:
        df_recon = pd.DataFrame(recon)
        # Compute summary counts
        matched = sum(1 for r in recon if r["Status"] == "MATCHED")
        mismatched = sum(1 for r in recon if r["Status"] == "MISMATCH")
        missing_count = sum(1 for r in recon if r["Status"] == "MISSING")

        sc1, sc2, sc3 = st.columns(3)
        sc1.metric("Matched", matched)
        sc2.metric("Mismatch", mismatched)
        sc3.metric("Missing in GSTR2B", missing_count)

        # Style the Status column
        def _highlight_status(val):
            colors = {
                "MATCHED": "background-color: #c8e6c9; color: #1b5e20;",
                "MISMATCH": "background-color: #ffe0b2; color: #e65100;",
                "MISSING": "background-color: #ffcdd2; color: #c62828;",
            }
            return colors.get(val, "")

        styled = df_recon.style.map(
            _highlight_status, subset=["Status"]
        )
        st.dataframe(styled, width="stretch", hide_index=True)
    else:
        st.info("No records for selected GSTIN")
    st.divider()

    # ── Section 4: Missing ITC Panel ──
    st.subheader("Missing ITC — Invoices not in GSTR2B")
    if missing:
        for m in missing:
            st.warning(
                f"**Invoice {m['Invoice_ID']}** | "
                f"Seller: {m['Seller_GSTIN']} | "
                f"Tax Claimed: ₹ {m.get('Tax_Claimed', 'N/A')} — "
                f"**Not found in GSTR2B. Do not release payment.**"
            )
    else:
        st.success("All purchase register invoices are present in GSTR2B")
    st.divider()

    # ── Section 5: Filing Status Check ──
    st.subheader("Seller Filing Status")
    if filing:
        df_filing = pd.DataFrame(filing)

        def _highlight_filing(val):
            if val in ("NOT FILED", "NO"):
                return "background-color: #ffcdd2; color: #c62828;"
            if val in ("FILED", "YES"):
                return "background-color: #c8e6c9; color: #1b5e20;"
            return ""

        styled_f = df_filing.style.map(
            _highlight_filing, subset=["GSTR1_Status", "GSTR3B_Payment"]
        )
        st.dataframe(styled_f, width="stretch", hide_index=True)
    else:
        st.info("No seller filing data available")
    st.divider()

    # ── Section 8: Client Risk Summary ──
    st.subheader("Vendor Risk Summary")
    if vendor_risk:
        df_risk = pd.DataFrame(vendor_risk)
        st.table(df_risk)
    else:
        st.success("No high-risk vendors detected for this client")


# ═══════════════════════════════════════════════════════
# PAGE: RECONCILIATION (dedicated view)
# ═══════════════════════════════════════════════════════

def _page_reconciliation(gstin_to_name: dict):
    st.title("Reconciliation Detail")

    client_gstin = st.selectbox(
        "Select Client GSTIN",
        st.session_state.clients,
        format_func=lambda g: f"{gstin_to_name.get(g, g)}  |  {g}",
        key="recon_gstin",
    )

    if not client_gstin:
        st.info("No records for selected GSTIN")
        return

    data = _get(f"/ca/overview/{client_gstin}")
    if not data:
        st.info("No records for selected GSTIN")
        return

    recon = data.get("reconciliation", [])
    if not recon:
        st.info("No records for selected GSTIN")
        return

    # Filters
    st.subheader("Filter Reconciliation")
    status_filter = st.multiselect(
        "Status", ["MATCHED", "MISMATCH", "MISSING"], default=["MATCHED", "MISMATCH", "MISSING"]
    )
    filtered = [r for r in recon if r["Status"] in status_filter]

    df = pd.DataFrame(filtered)

    def _highlight_status(val):
        colors = {
            "MATCHED": "background-color: #c8e6c9; color: #1b5e20;",
            "MISMATCH": "background-color: #ffe0b2; color: #e65100;",
            "MISSING": "background-color: #ffcdd2; color: #c62828;",
        }
        return colors.get(val, "")

    styled = df.style.map(_highlight_status, subset=["Status"])
    st.dataframe(styled, width="stretch", hide_index=True)

    st.divider()

    # Per-invoice explanations for non-matched
    issues = [r for r in filtered if r["Status"] != "MATCHED"]
    if issues:
        st.subheader("Explanations for Issues")
        for row in issues:
            detail = _get(f"/ca/invoice/{client_gstin}/{row['Invoice_ID']}")
            if detail:
                explanations = generate_ca_explanation(detail)
                for exp in explanations:
                    if "reconciled" in exp.lower():
                        st.success(exp)
                    elif "missing" in exp.lower():
                        st.error(exp)
                    else:
                        st.info(exp)
    else:
        st.success("All filtered invoices are fully reconciled")


# ═══════════════════════════════════════════════════════
# PAGE: SEARCH (Sections 6, 7)
# ═══════════════════════════════════════════════════════

def _page_search(gstin_to_name: dict):
    st.title("Invoice Search")

    # ── Section 1 (repeated): Client Selection ──
    client_gstin = st.selectbox(
        "Select Client GSTIN",
        st.session_state.clients,
        format_func=lambda g: f"{gstin_to_name.get(g, g)}  |  {g}",
        key="search_gstin",
    )
    st.caption(f"Searching invoices for GSTIN: **{client_gstin}**")

    # ── Section 6: Search Panel ──
    invoice_id = st.text_input("Search Invoice", placeholder="e.g. INV-1")

    if not invoice_id:
        st.info("Enter an Invoice ID above to begin.")
        return

    detail = _get(f"/ca/invoice/{client_gstin}/{invoice_id}")
    if not detail:
        st.info("No records found")
        return

    # ── Invoice Details ──
    st.subheader("Invoice Details")
    inv = detail.get("invoice") or {}
    pr = detail.get("purchase_register") or {}
    g2b = detail.get("gstr2b") or {}

    c1, c2 = st.columns(2)
    with c1:
        st.markdown(f"**Invoice ID:** {invoice_id}")
        val = inv.get("Value") or pr.get("Value_Claimed") or g2b.get("Value") or "N/A"
        try:
            st.markdown(f"**Value:** ₹ {float(val):,.2f}")
        except (ValueError, TypeError):
            st.markdown(f"**Value:** {val}")
        st.markdown(f"**Date:** {inv.get('Invoice_Date') or pr.get('Claim_Date') or 'N/A'}")
    with c2:
        st.markdown(f"**Seller GSTIN:** `{detail.get('seller_gstin', 'N/A')}`")
        st.markdown(f"**Buyer GSTIN:** `{client_gstin}`")

    st.divider()

    # ── Reconciliation Status ──
    st.subheader("Reconciliation Status")
    status = detail.get("reconciliation_status", "UNKNOWN")
    status_colors = {"MATCHED": "green", "MISMATCH": "orange", "MISSING": "red"}
    st.markdown(f"**Status:** :{status_colors.get(status, 'gray')}[{status}]")

    rc1, rc2, rc3 = st.columns(3)
    rc1.metric("Purchase Value", f"₹ {_fmt(pr.get('Value_Claimed'))}")
    rc2.metric("GSTR2B Value", f"₹ {_fmt(g2b.get('Value'))}")
    rc3.metric("Difference", f"₹ {_diff(pr.get('Value_Claimed'), g2b.get('Value'))}")

    st.divider()

    # ── ITC Eligibility ──
    st.subheader("ITC Eligibility")
    ic1, ic2, ic3 = st.columns(3)
    ic1.metric("ITC Eligible", detail.get("itc_eligible", "UNKNOWN"))
    ic2.metric("GSTR1 Status", detail.get("gstr1_status", "UNKNOWN"))
    ic3.metric("GSTR3B Payment", detail.get("gstr3b_payment", "UNKNOWN"))

    st.divider()

    # ── Section 7: Explainable Output ──
    st.subheader("Explainable Compliance Output")
    explanations = generate_ca_explanation(detail)
    for exp in explanations:
        if "reconciled" in exp.lower():
            st.success(exp)
        elif "missing" in exp.lower() or "blocked" in exp.lower():
            st.error(exp)
        else:
            st.info(exp)


# ═══════════════════════════════════════════════════════
# PAGE: REPORTS
# ═══════════════════════════════════════════════════════

def _page_reports(gstin_to_name: dict):
    st.title("Reports")

    client_gstin = st.selectbox(
        "Select Client GSTIN",
        st.session_state.clients,
        format_func=lambda g: f"{gstin_to_name.get(g, g)}  |  {g}",
        key="report_gstin",
    )

    data = _get(f"/ca/overview/{client_gstin}")
    if not data:
        st.info("No records for selected GSTIN")
        return

    taxpayer = data.get("taxpayer", {})
    itc = data.get("itc_summary", {})
    recon = data.get("reconciliation", [])
    missing = data.get("missing_itc", [])
    vendor_risk = data.get("vendor_risk", [])

    # ── Executive Summary ──
    st.subheader(f"Executive Summary — {taxpayer.get('Name', client_gstin)}")
    e1, e2, e3, e4, e5 = st.columns(5)
    e1.metric("Total Invoices", itc.get("total_invoices", 0))
    e2.metric("Total ITC", f"₹ {itc.get('total_itc', 0):,.2f}")
    e3.metric("Blocked ITC", f"₹ {itc.get('blocked_itc', 0):,.2f}")
    matched = sum(1 for r in recon if r["Status"] == "MATCHED")
    issues = sum(1 for r in recon if r["Status"] != "MATCHED")
    e4.metric("Reconciled", matched)
    e5.metric("Issues", issues)

    st.divider()

    # ── Action Items ──
    st.subheader("Action Items")
    action_count = 0
    for m in missing:
        action_count += 1
        st.error(
            f"**Action {action_count}:** Invoice {m['Invoice_ID']} "
            f"(Seller: {m['Seller_GSTIN']}) missing in GSTR2B. "
            f"Do not release payment."
        )
    for r in recon:
        if r["Status"] == "MISMATCH":
            action_count += 1
            st.warning(
                f"**Action {action_count}:** Invoice {r['Invoice_ID']} "
                f"value mismatch — PR: ₹{r.get('Purchase_Value')}, "
                f"GSTR2B: ₹{r.get('GSTR2B_Value')}. Verify with supplier."
            )
    for v in vendor_risk:
        action_count += 1
        st.warning(
            f"**Action {action_count}:** Vendor {v['Vendor_GSTIN']} — "
            f"{v['Reasons']}"
        )
    if action_count == 0:
        st.success("No action items — all clear")

    st.divider()

    # ── Export ──
    st.subheader("Export Data")
    ca, cb, cc = st.columns(3)
    if recon:
        with ca:
            df = pd.DataFrame(recon)
            st.download_button(
                "Download Reconciliation (CSV)",
                df.to_csv(index=False),
                f"reconciliation_{client_gstin}.csv",
                "text/csv",
            )
    if missing:
        with cb:
            df = pd.DataFrame(missing)
            st.download_button(
                "Download Missing ITC (CSV)",
                df.to_csv(index=False),
                f"missing_itc_{client_gstin}.csv",
                "text/csv",
            )
    if vendor_risk:
        with cc:
            df = pd.DataFrame(vendor_risk)
            st.download_button(
                "Download Vendor Risk (CSV)",
                df.to_csv(index=False),
                f"vendor_risk_{client_gstin}.csv",
                "text/csv",
            )


# ═══════════════════════════════════════════════════════
# PAGE: LOGOUT
# ═══════════════════════════════════════════════════════

def _page_logout():
    from frontend.login import logout
    logout()


# ═══════════════════════════════════════════════════════
# Formatting helpers
# ═══════════════════════════════════════════════════════

def _fmt(val) -> str:
    """Format a value as comma-separated float, or N/A."""
    if val is None:
        return "N/A"
    try:
        return f"{float(val):,.2f}"
    except (ValueError, TypeError):
        return str(val)


def _diff(a, b) -> str:
    """Return the absolute difference between two values."""
    try:
        return f"{abs(float(a) - float(b)):,.2f}"
    except (ValueError, TypeError):
        return "N/A"
