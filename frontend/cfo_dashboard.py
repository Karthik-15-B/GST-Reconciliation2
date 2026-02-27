"""
CFO Dashboard — Production-grade Streamlit UI for GST Reconciliation & ITC Risk Analysis.

Consumes:
    • FastAPI backend at http://localhost:8000
    • MongoDB (read-only, via backend)
    • Neo4j  (read-only, via backend)

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
# API HELPERS
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
# EXPLANATION ENGINE  (Section 7)
# ═══════════════════════════════════════════════════════

def generate_explanation(audit_data: dict) -> list[str]:
    """Produce human-readable compliance explanations from an audit response."""
    inv_id = audit_data.get("invoice_id", "Unknown")
    flags = audit_data.get("compliance", {}).get("flags", [])
    explanations: list[str] = []

    if "GSTR1_NOT_FILED" in flags:
        explanations.append(
            f"Invoice {inv_id} ITC blocked because supplier did not file GSTR1"
        )
    if "GSTR3B_NOT_FILED" in flags:
        explanations.append(
            f"Invoice {inv_id} risky because supplier has not filed GSTR3B"
        )
    if "GSTR3B_PAYMENT_NOT_CONFIRMED" in flags:
        explanations.append(
            f"Invoice {inv_id} risky because supplier tax payment missing"
        )
    if "ITC_MISMATCH_CLAIMED_BUT_NOT_ELIGIBLE" in flags:
        explanations.append(
            f"Invoice {inv_id} ITC mismatch — claimed but marked ineligible in GSTR2B"
        )
    if "EWAYBILL_MISSING" in flags:
        explanations.append(
            f"Invoice {inv_id} missing E-Way Bill — transport compliance gap"
        )
    # Catch any GSTR1 status anomalies
    for f in flags:
        if f.startswith("GSTR1_STATUS_") and f != "GSTR1_STATUS_FILED":
            explanations.append(
                f"Invoice {inv_id} — GSTR1 filing status is anomalous ({f})"
            )

    if not explanations:
        explanations.append(f"Invoice {inv_id} valid ITC claim")

    return explanations


# ═══════════════════════════════════════════════════════
# SHOW_CFO_DASHBOARD  —  entry point
# ═══════════════════════════════════════════════════════

def show_cfo_dashboard():
    """Render the entire CFO Dashboard UI."""

    # ── Custom CSS ──────────────────────────────────────
    st.markdown(
        """
        <style>
        .company-banner {
            background: linear-gradient(135deg, #1a237e, #0d47a1);
            color: #ffffff;
            padding: 1.4rem 2rem;
            border-radius: 10px;
            margin-bottom: 1.2rem;
        }
        .company-banner h2 { color: #ffffff; margin: 0; }
        .company-banner p  { color: #bbdefb; margin: 0.25rem 0 0 0; }
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
    st.sidebar.title("GST Reconciliation")
    st.sidebar.caption("Intelligent Knowledge Graph System")
    st.sidebar.divider()

    # ── Load taxpayers ──
    tp_resp = _get("/dashboard/taxpayers")
    if not tp_resp or not tp_resp.get("taxpayers"):
        st.error("Could not load taxpayer list. Is the backend running on port 8000?")
        return

    taxpayers = tp_resp["taxpayers"]
    label_to_gstin = {
        f"{t['Name']}  |  {t['GSTIN']}": t["GSTIN"] for t in taxpayers
    }

    selected_label = st.sidebar.selectbox(
        "Select Company", list(label_to_gstin.keys())
    )
    gstin = label_to_gstin[selected_label]
    st.session_state.gstin = gstin

    st.sidebar.divider()

    # ── Section 8: Sidebar Menu ──
    menu = st.sidebar.selectbox(
        "Menu",
        ["Dashboard", "Search", "Risk Analysis", "Reports", "Logout"],
    )

    st.sidebar.divider()

    # ── Health indicator ──
    health = _get("/health")
    if health:
        c1, c2 = st.sidebar.columns(2)
        c1.markdown(
            f"**MongoDB:** {'🟢' if health.get('mongodb') == 'UP' else '🔴'}"
        )
        c2.markdown(
            f"**Neo4j:** {'🟢' if health.get('neo4j') == 'UP' else '🔴'}"
        )

    # ══════════════════════════════════════════════════
    # PAGE ROUTING
    # ══════════════════════════════════════════════════
    if menu == "Dashboard":
        _page_dashboard(gstin)
    elif menu == "Search":
        _page_search(gstin)
    elif menu == "Risk Analysis":
        _page_risk_analysis(gstin)
    elif menu == "Reports":
        _page_reports(gstin)
    elif menu == "Logout":
        _page_logout()


# ═══════════════════════════════════════════════════════
# PAGE: DASHBOARD
# ═══════════════════════════════════════════════════════

def _page_dashboard(gstin: str):
    data = _get(f"/dashboard/overview/{gstin}")
    if not data:
        st.info("No records found")
        return

    taxpayer = data.get("taxpayer", {})
    itc = data.get("itc_summary", {})
    purchase_reg = data.get("purchase_register", [])
    gstr2b = data.get("gstr2b", [])
    vendor_risk = data.get("vendor_risk", [])
    warnings = data.get("payment_warnings", [])

    # ── TOP: Company Info ──
    st.markdown(
        f"""
        <div class="company-banner">
            <h2>{taxpayer.get("Name", "N/A")}</h2>
            <p>GSTIN: {gstin} &nbsp;&nbsp;|&nbsp;&nbsp;
               Risk Category: {taxpayer.get("Risk_Category", "N/A")}</p>
        </div>
        """,
        unsafe_allow_html=True,
    )

    # ── Section 1: ITC Summary ──
    st.subheader("ITC Summary (from GSTR2B)")
    col1, col2, col3 = st.columns(3)
    col1.metric("Total ITC", f"₹ {itc.get('total_itc', 0):,.2f}")
    col2.metric("Eligible ITC", f"₹ {itc.get('eligible_itc', 0):,.2f}")
    blocked = itc.get("blocked_itc", 0)
    col3.metric(
        "Blocked ITC",
        f"₹ {blocked:,.2f}",
        delta=f"-₹ {blocked:,.2f}" if blocked > 0 else None,
        delta_color="inverse",
    )
    st.divider()

    # ── Section 2: Purchase Register ──
    st.subheader("Purchase Register")
    if purchase_reg:
        df_pr = pd.DataFrame(purchase_reg)
        # Convert numeric columns for display
        for col in ("Value_Claimed", "Tax_Claimed"):
            if col in df_pr.columns:
                df_pr[col] = pd.to_numeric(df_pr[col], errors="coerce")
        st.dataframe(df_pr, use_container_width=True, hide_index=True)
    else:
        st.info("No records found")
    st.divider()

    # ── Section 3: GSTR2B Table ──
    st.subheader("GSTR2B Records")
    if gstr2b:
        df_g2b = pd.DataFrame(gstr2b)
        for col in ("Value", "Tax"):
            if col in df_g2b.columns:
                df_g2b[col] = pd.to_numeric(df_g2b[col], errors="coerce")
        st.dataframe(df_g2b, use_container_width=True, hide_index=True)
    else:
        st.info("No records found")
    st.divider()

    # ── Section 4: Vendor Risk Panel ──
    st.subheader("Vendor Risk Panel")
    if vendor_risk:
        risk_rows = []
        for v in vendor_risk:
            risk_rows.append(
                {
                    "Vendor GSTIN": v["gstin"],
                    "Vendor Name": v["name"],
                    "Risk Level": v["risk_level"],
                    "Reasons": " | ".join(v["reasons"]),
                }
            )
        st.table(pd.DataFrame(risk_rows))
    else:
        st.info("No vendor risk data available")
    st.divider()

    # ── Section 5: Payment Warnings ──
    st.subheader("Payment Warnings")
    if warnings:
        for w in warnings:
            if w.get("severity") == "CRITICAL":
                st.error(w["message"])
            else:
                st.warning(w["message"])
    else:
        st.success("No payment warnings — all clear")


# ═══════════════════════════════════════════════════════
# PAGE: SEARCH  (Sections 6, 7)
# ═══════════════════════════════════════════════════════

def _page_search(gstin: str):
    st.title("Invoice Search")
    st.caption(f"Searching invoices for GSTIN: **{gstin}**")

    # ── Section 6: Search Panel ──
    invoice_id = st.text_input("Search Invoice", placeholder="e.g. INV-1")

    if not invoice_id:
        st.info("Enter an Invoice ID above to begin.")
        return

    audit = _get(f"/graph/audit/{invoice_id}")
    if not audit:
        st.info("No records found")
        return

    # ── GSTIN guard — never show data for other GSTINs ──
    mongo = audit.get("mongo_data", {})
    inv_doc = mongo.get("invoice") or {}
    seller = inv_doc.get("Seller_GSTIN", "")
    buyer = inv_doc.get("Buyer_GSTIN", "")

    if gstin not in (seller, buyer):
        st.warning(
            f"Invoice {invoice_id} does not belong to the selected company ({gstin})."
        )
        return

    graph = audit.get("graph_data", {})
    compliance = audit.get("compliance", {})

    # ── Invoice Details ──
    st.subheader("Invoice Details")
    c1, c2 = st.columns(2)
    with c1:
        st.markdown(f"**Invoice ID:** {invoice_id}")
        val = inv_doc.get("Value", "N/A")
        try:
            st.markdown(f"**Value:** ₹ {float(val):,.2f}")
        except (ValueError, TypeError):
            st.markdown(f"**Value:** {val}")
        st.markdown(f"**Date:** {inv_doc.get('Invoice_Date', 'N/A')}")
    with c2:
        seller_info = graph.get("seller") or {}
        buyer_info = graph.get("buyer") or {}
        st.markdown(f"**Seller:** {seller_info.get('name', seller)} (`{seller}`)")
        st.markdown(f"**Buyer:** {buyer_info.get('name', buyer)} (`{buyer}`)")

    st.divider()

    # ── ITC Status ──
    st.subheader("ITC Status")
    mc1, mc2, mc3 = st.columns(3)
    mc1.metric("ITC Eligible", compliance.get("itc_eligible", "UNKNOWN"))
    mc2.metric("ITC Claimed", "Yes" if compliance.get("itc_claimed") else "No")
    mc3.metric(
        "E-Way Bill",
        "Present" if compliance.get("ewaybill_present") else "Missing",
    )

    st.divider()

    # ── Vendor Risk (for the seller of this invoice) ──
    st.subheader("Vendor Risk")
    if seller:
        risk_data = _get(f"/graph/risk-score/{seller}")
        if risk_data:
            rc1, rc2, rc3 = st.columns(3)
            rc1.metric("Risk Score", f"{risk_data.get('risk_score', 'N/A')} / 100")
            rc2.metric("Own Risk", risk_data.get("own_risk", "N/A"))
            rc3.metric("High-Risk Neighbors", len(risk_data.get("high_risk_neighbors", [])))
        else:
            st.info("Vendor risk data not available.")

    st.divider()

    # ── Section 7: Explainable Compliance Output ──
    st.subheader("Explainable Compliance Output")
    explanations = generate_explanation(audit)
    for exp in explanations:
        if "valid" in exp.lower():
            st.success(exp)
        else:
            st.info(exp)


# ═══════════════════════════════════════════════════════
# PAGE: RISK ANALYSIS  (Section 9 + Neo4j panels)
# ═══════════════════════════════════════════════════════

def _page_risk_analysis(gstin: str):
    st.title("Risk Analysis")
    st.caption(f"Network & risk analysis for GSTIN: **{gstin}**")

    # ── Risk Score ──
    st.subheader("Your Risk Score")
    risk = _get(f"/graph/risk-score/{gstin}")
    if risk:
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Risk Score", f"{risk.get('risk_score', 0)} / 100")
        c2.metric("Own Risk", risk.get("own_risk", "N/A"))
        c3.metric("Total Neighbors", risk.get("total_neighbors", 0))
        high_risk = risk.get("high_risk_neighbors", [])
        c4.metric("High-Risk Neighbors", len(high_risk))

        if high_risk:
            with st.expander("High-Risk Neighbors", expanded=False):
                for n in high_risk:
                    st.markdown(f"- `{n.get('gstin')}` — {n.get('name')}")

        med_risk = risk.get("medium_risk_neighbors", [])
        if med_risk:
            with st.expander("Medium-Risk Neighbors", expanded=False):
                for n in med_risk:
                    st.markdown(f"- `{n.get('gstin')}` — {n.get('name')}")
    else:
        st.info("No records found")

    st.divider()

    # ── Section 9: Vendor Network (Neo4j) ──
    st.subheader("Vendor Network")
    if st.button("Show Vendor Network"):
        network = _get(f"/dashboard/vendor-network/{gstin}")
        if network and network.get("connections"):
            connections = network["connections"]

            # Split by role
            as_seller = [c for c in connections if c.get("role") == "SELLER"]
            as_buyer = [c for c in connections if c.get("role") == "BUYER"]

            if as_seller:
                st.markdown("**Customers (invoices you issued):**")
                for c in as_seller:
                    risk_label = c.get("partner_risk", "N/A")
                    st.markdown(
                        f"- **{c.get('partner_name')}** (`{c.get('partner_gstin')}`) "
                        f"— Risk: {risk_label} "
                        f"— Invoices: {', '.join(c.get('invoices', []))}"
                    )

            if as_buyer:
                st.markdown("**Suppliers (invoices billed to you):**")
                for c in as_buyer:
                    risk_label = c.get("partner_risk", "N/A")
                    st.markdown(
                        f"- **{c.get('partner_name')}** (`{c.get('partner_gstin')}`) "
                        f"— Risk: {risk_label} "
                        f"— Invoices: {', '.join(c.get('invoices', []))}"
                    )
        else:
            st.info("No vendor connections found in the graph")

    st.divider()

    # ── Circular Trading ──
    st.subheader("Circular Trading Detection")
    if st.button("Detect Circular Trading"):
        circles = _get("/graph/detect-circles")
        if circles and circles.get("circles"):
            st.warning(
                f"{circles['circles_found']} circular trading loop(s) detected!"
            )
            for i, c in enumerate(circles["circles"], 1):
                with st.expander(
                    f"Loop {i}: {c.get('name_a')} → {c.get('name_b')} → {c.get('name_c')}"
                ):
                    loop_df = pd.DataFrame(
                        [
                            {
                                "Party": "A",
                                "GSTIN": c.get("gstin_a"),
                                "Name": c.get("name_a"),
                                "Risk": c.get("risk_a"),
                            },
                            {
                                "Party": "B",
                                "GSTIN": c.get("gstin_b"),
                                "Name": c.get("name_b"),
                                "Risk": c.get("risk_b"),
                            },
                            {
                                "Party": "C",
                                "GSTIN": c.get("gstin_c"),
                                "Name": c.get("name_c"),
                                "Risk": c.get("risk_c"),
                            },
                        ]
                    )
                    st.table(loop_df)
                    st.markdown(
                        f"**Invoices:** A→B: {c.get('inv_a_to_b')}, "
                        f"B→C: {c.get('inv_b_to_c')}, "
                        f"C→A: {c.get('inv_c_to_a')}"
                    )
        else:
            st.success("No circular trading patterns detected")

    st.divider()

    # ── Shadow Networks ──
    st.subheader("Shadow Network Detection")
    if st.button("Find Shadow Networks"):
        shadow = _get("/graph/find-shadow-networks")
        if shadow and shadow.get("networks"):
            st.warning(f"{shadow['networks_found']} shadow network(s) detected!")
            for net in shadow["networks"]:
                match_type = net.get("match_type", "UNKNOWN")
                shared = net.get("shared_value", "N/A")
                members = net.get("members", [])
                with st.expander(
                    f"{match_type}: {shared} ({len(members)} members)"
                ):
                    for m in members:
                        involved = gstin == m.get("gstin")
                        prefix = "**→ YOU:** " if involved else ""
                        st.markdown(
                            f"{prefix}`{m.get('gstin')}` — "
                            f"{m.get('name')} — Risk: {m.get('risk', 'N/A')}"
                        )
        else:
            st.success("No shadow networks detected")


# ═══════════════════════════════════════════════════════
# PAGE: REPORTS
# ═══════════════════════════════════════════════════════

def _page_reports(gstin: str):
    st.title("Reports")
    st.caption(f"Compliance reports for GSTIN: **{gstin}**")

    data = _get(f"/dashboard/overview/{gstin}")
    if not data:
        st.info("No records found")
        return

    itc = data.get("itc_summary", {})
    vendor_risk = data.get("vendor_risk", [])
    warnings = data.get("payment_warnings", [])

    # ── Executive Summary ──
    st.subheader("Executive Summary")
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total ITC", f"₹ {itc.get('total_itc', 0):,.2f}")
    c2.metric("ITC at Risk", f"₹ {itc.get('blocked_itc', 0):,.2f}")
    c3.metric(
        "High-Risk Vendors",
        sum(1 for v in vendor_risk if v.get("risk_level") == "HIGH"),
    )
    c4.metric("Payment Alerts", len(warnings))

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
        st.success("No action items — all clear")

    st.divider()

    # ── Export Data ──
    st.subheader("Export Data")
    col_a, col_b = st.columns(2)
    if data.get("purchase_register"):
        df = pd.DataFrame(data["purchase_register"])
        with col_a:
            st.download_button(
                "Download Purchase Register (CSV)",
                df.to_csv(index=False),
                f"purchase_register_{gstin}.csv",
                "text/csv",
            )
    if data.get("gstr2b"):
        df = pd.DataFrame(data["gstr2b"])
        with col_b:
            st.download_button(
                "Download GSTR2B Records (CSV)",
                df.to_csv(index=False),
                f"gstr2b_{gstin}.csv",
                "text/csv",
            )


# ═══════════════════════════════════════════════════════
# PAGE: LOGOUT
# ═══════════════════════════════════════════════════════

def _page_logout():
    st.title("Logged Out")
    st.info("Session cleared. Select a company from the sidebar to continue.")
    for key in list(st.session_state.keys()):
        del st.session_state[key]
