"""
Tax Inspector Dashboard — Law-enforcement grade GST fraud detection & compliance UI.

Full data access across ALL GSTINs. No filtering restrictions.
Consumes FastAPI backend + Neo4j graph for fraud pattern analysis.

Entry point:  show_inspector_dashboard()
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
    """GET to backend. Returns JSON dict or None on error."""
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
# SECTION 9 — EXPLAINABLE OUTPUT
# ═══════════════════════════════════════════════════════

def generate_taxpayer_explanation(gstin: str, profile: dict | None, high_risk_map: dict) -> list[str]:
    """Generate human-readable fraud/compliance explanations for a GSTIN."""
    if not profile:
        return [f"{gstin} — no data available."]

    tp = profile.get("taxpayer", {})
    name = tp.get("Name", gstin)
    compliance = profile.get("compliance", {})
    risk = profile.get("risk") or {}
    explanations: list[str] = []

    hr = high_risk_map.get(gstin)
    if hr:
        reasons = hr.get("reasons", [])
        if any("circular" in r.lower() for r in reasons):
            explanations.append(f"{name} ({gstin}) is high risk due to circular trading detected in the knowledge graph.")
        if any("shared ip" in r.lower() for r in reasons):
            explanations.append(f"{name} ({gstin}) is flagged for shared IP address usage — possible shell entity.")
        if any("gstr1 filed" in r.lower() and "payment" in r.lower() for r in reasons):
            explanations.append(f"{name} ({gstin}) is risky because invoices were filed but tax was not paid.")
        if any("itc blocked" in r.lower() for r in reasons):
            explanations.append(f"{name} ({gstin}) has multiple invoices with blocked ITC — possible fake billing.")

    if compliance.get("status") == "NON-COMPLIANT":
        if not compliance.get("gstr1_filed"):
            explanations.append(f"{name} ({gstin}) has not filed GSTR1 for any invoices.")
        if not compliance.get("payment_confirmed"):
            explanations.append(f"{name} ({gstin}) — GSTR3B tax payment not confirmed.")

    if risk.get("risk_score", 0) >= 31:
        explanations.append(
            f"{name} ({gstin}) risk score is {risk['risk_score']}/100 "
            f"({risk.get('risk_level', 'MEDIUM')}) — "
            f"{risk.get('high_risk_neighbors', 0)} high-risk entity(s) in network."
        )

    if not explanations:
        explanations.append(f"{name} ({gstin}) — no significant risk indicators detected.")

    return explanations


# ═══════════════════════════════════════════════════════
# SHOW_INSPECTOR_DASHBOARD — entry point
# ═══════════════════════════════════════════════════════

def show_inspector_dashboard():
    """Render the entire Tax Inspector Dashboard UI."""
    from frontend.login import require_role, logout
    require_role("INSPECTOR")

    # ── Custom CSS ──
    st.markdown(
        """
        <style>
        .insp-banner {
            background: linear-gradient(135deg, #b71c1c, #c62828, #d32f2f);
            color: #ffffff;
            padding: 1.4rem 2rem;
            border-radius: 10px;
            margin-bottom: 1rem;
        }
        .insp-banner h2 { color: #ffffff; margin: 0; }
        .insp-banner p  { color: #ffcdd2; margin: 0.25rem 0 0 0; font-size: 0.95rem; }
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
    # SIDEBAR  (Section 10)
    # ══════════════════════════════════════════════════
    st.sidebar.title("GST Intelligence")
    st.sidebar.caption("Tax Inspector Console")
    st.sidebar.divider()

    menu = st.sidebar.selectbox(
        "Menu",
        ["Dashboard", "GSTIN Search", "Fraud Detection", "Graph Analysis", "Reports", "Logout"],
    )

    st.sidebar.divider()

    # Health indicator
    health = _get("/health")
    if health:
        hc1, hc2 = st.sidebar.columns(2)
        hc1.markdown(f"**MongoDB:** {'🟢' if health.get('mongodb') == 'UP' else '🔴'}")
        hc2.markdown(f"**Neo4j:** {'🟢' if health.get('neo4j') == 'UP' else '🔴'}")

    st.sidebar.divider()
    st.sidebar.info(f"**User:** {st.session_state.get('username', 'inspector_demo')}\n\n**Role:** INSPECTOR\n\n**Access:** ALL GSTINs")

    # ══════════════════════════════════════════════════
    # PAGE ROUTING
    # ══════════════════════════════════════════════════
    if menu == "Dashboard":
        _page_dashboard()
    elif menu == "GSTIN Search":
        _page_gstin_search()
    elif menu == "Fraud Detection":
        _page_fraud_detection()
    elif menu == "Graph Analysis":
        _page_graph_analysis()
    elif menu == "Reports":
        _page_reports()
    elif menu == "Logout":
        _page_logout()


# ═══════════════════════════════════════════════════════
# PAGE: DASHBOARD  (Sections 1, 2, 4)
# ═══════════════════════════════════════════════════════

def _page_dashboard():
    # ── Title ──
    st.markdown(
        """
        <div class="insp-banner">
            <h2>GST Intelligence Dashboard</h2>
            <p>Fraud Detection and Vendor Compliance Monitoring</p>
        </div>
        """,
        unsafe_allow_html=True,
    )

    # ── Section 1: Global Summary ──
    summary = _get("/inspector/summary")
    if summary:
        m1, m2, m3, m4 = st.columns(4)
        m1.metric("Total Taxpayers", summary.get("total_taxpayers", 0))
        m2.metric("Total Invoices", summary.get("total_invoices", 0))
        m3.metric("Total ITC Claimed", f"₹ {summary.get('total_itc_claimed', 0):,.2f}")
        m4.metric("High Risk Vendors", summary.get("high_risk_vendors", 0))
    else:
        st.warning("Could not load global summary.")

    st.divider()

    # ── Section 2: High Risk Vendors ──
    st.subheader("High Risk Vendors")
    hr_data = _get("/inspector/high-risk")
    if hr_data and hr_data.get("vendors"):
        vendors = hr_data["vendors"]
        rows = []
        for v in vendors:
            rows.append({
                "GSTIN": v["gstin"],
                "Name": v.get("name", "Unknown"),
                "Risk Level": v["risk_level"],
                "Reason": " | ".join(v["reasons"]),
            })
        df = pd.DataFrame(rows)

        def _risk_color(val):
            if val == "HIGH":
                return "background-color: #ffcdd2; color: #b71c1c;"
            if val == "MEDIUM":
                return "background-color: #ffe0b2; color: #e65100;"
            return ""

        styled = df.style.map(_risk_color, subset=["Risk Level"])
        st.dataframe(styled, width="stretch", hide_index=True)
        st.caption(f"{len(vendors)} high-risk vendor(s) detected")
    else:
        st.success("No high-risk vendors detected")

    st.divider()

    # ── Section 4: Vendor Compliance Table ──
    st.subheader("Vendor Compliance — GSTR1 vs GSTR3B")
    comp = _get("/inspector/compliance")
    if comp and comp.get("compliance"):
        df_c = pd.DataFrame(comp["compliance"])

        def _comp_color(val):
            if val in ("NO", "NON-COMPLIANT"):
                return "background-color: #ffcdd2; color: #b71c1c;"
            if val in ("YES", "COMPLIANT"):
                return "background-color: #c8e6c9; color: #1b5e20;"
            return ""

        styled_c = df_c.style.map(
            _comp_color,
            subset=["GSTR1_Filed", "Tax_Paid", "Compliance_Status"],
        )
        st.dataframe(styled_c, width="stretch", hide_index=True)

        non_comp = sum(1 for r in comp["compliance"] if r["Compliance_Status"] == "NON-COMPLIANT")
        if non_comp:
            st.warning(f"{non_comp} non-compliant vendor(s) found")
        else:
            st.success("All vendors are compliant")
    else:
        st.info("No compliance data available")


# ═══════════════════════════════════════════════════════
# PAGE: GSTIN SEARCH  (Sections 3, 9)
# ═══════════════════════════════════════════════════════

def _page_gstin_search():
    st.markdown(
        '<div class="insp-banner"><h2>GSTIN Search</h2>'
        "<p>Full profile lookup — company info, invoices, risk, compliance</p></div>",
        unsafe_allow_html=True,
    )

    gstin = st.text_input("Search GSTIN", placeholder="e.g. 29HSmIZ8246mZ6")

    if not gstin:
        st.info("Enter a GSTIN above to begin investigation.")
        return

    profile = _get(f"/inspector/gstin/{gstin}")
    if not profile:
        st.error("GSTIN not found")
        return

    tp = profile.get("taxpayer", {})

    # ── Company Info ──
    st.subheader(f"{tp.get('Name', 'Unknown')}  —  {gstin}")
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Risk Category", tp.get("Risk_Category", "N/A"))
    c2.metric("Phone", tp.get("Phone", "N/A"))
    c3.metric("IP Address", tp.get("IP_Address", "N/A"))

    risk = profile.get("risk") or {}
    risk_label = risk.get("risk_level", "N/A") if risk else "N/A"
    c4.metric("Risk Score", f"{risk.get('risk_score', 'N/A')} / 100 ({risk_label})" if risk else "N/A")

    st.divider()

    # ── Invoices ──
    st.subheader("Invoices")
    sold = profile.get("invoices_as_seller", [])
    bought = profile.get("invoices_as_buyer", [])

    tc1, tc2, tc3 = st.columns(3)
    tc1.metric("As Seller", len(sold))
    tc2.metric("As Buyer", len(bought))
    tc3.metric("GSTR1 Filings", profile.get("gstr1_filings", 0))

    tab_sell, tab_buy = st.tabs(["Sold (Issued)", "Purchased (Billed To)"])
    with tab_sell:
        if sold:
            st.dataframe(pd.DataFrame(sold), width="stretch", hide_index=True)
        else:
            st.info("No invoices issued by this GSTIN")
    with tab_buy:
        if bought:
            st.dataframe(pd.DataFrame(bought), width="stretch", hide_index=True)
        else:
            st.info("No invoices billed to this GSTIN")

    st.divider()

    # ── Compliance Status ──
    st.subheader("Compliance Status")
    compliance = profile.get("compliance", {})
    cs1, cs2, cs3 = st.columns(3)
    cs1.metric("GSTR1 Filed", "YES" if compliance.get("gstr1_filed") else "NO")
    cs2.metric("Payment Confirmed", "YES" if compliance.get("payment_confirmed") else "NO")
    status = compliance.get("status", "UNKNOWN")
    cs3.metric("Status", status)

    st.divider()

    # ── Section 9: Explainable Output ──
    st.subheader("Intelligence Assessment")
    hr_data = _get("/inspector/high-risk")
    hr_map = {}
    if hr_data:
        for v in hr_data.get("vendors", []):
            hr_map[v["gstin"]] = v

    explanations = generate_taxpayer_explanation(gstin, profile, hr_map)
    for exp in explanations:
        if "no significant risk" in exp.lower():
            st.success(exp)
        else:
            st.info(exp)


# ═══════════════════════════════════════════════════════
# PAGE: FRAUD DETECTION  (Sections 5, 6, 7)
# ═══════════════════════════════════════════════════════

def _page_fraud_detection():
    st.markdown(
        '<div class="insp-banner"><h2>Fraud Detection</h2>'
        "<p>Fake ITC, E-Way Bill violations, circular trading networks</p></div>",
        unsafe_allow_html=True,
    )

    # ── Section 5: Fake ITC Detection ──
    st.subheader("Fake ITC Detection — Purchase Register vs GSTR1")
    st.caption("Invoices claimed in Purchase Register but supplier never filed in GSTR1")
    fake = _get("/inspector/fake-itc")
    if fake and fake.get("suspects"):
        suspects = fake["suspects"]
        st.error(f"{len(suspects)} suspected fake ITC invoice(s) detected!")
        df_fake = pd.DataFrame(suspects)
        st.dataframe(df_fake, width="stretch", hide_index=True)
    else:
        st.success("No fake ITC detected")

    st.divider()

    # ── Section 6: E-Way Bill Fraud ──
    st.subheader("E-Way Bill Violations — Value > ₹50,000 Without EWayBill")
    ewb = _get("/inspector/ewaybill-fraud")
    if ewb and ewb.get("suspects"):
        suspects = ewb["suspects"]
        st.error(f"{len(suspects)} high-value invoice(s) without E-Way Bill!")
        df_ewb = pd.DataFrame(suspects)
        df_ewb["Value"] = df_ewb["Value"].apply(lambda v: f"₹ {v:,.2f}")
        st.dataframe(df_ewb, width="stretch", hide_index=True)
    else:
        st.success("No E-Way Bill violations detected")

    st.divider()

    # ── Section 7: Circular Trading Detection (Neo4j) ──
    st.subheader("Circular Trading Detection — Knowledge Graph")
    st.caption("3-party A → B → C → A trading loops detected via Neo4j graph traversal")
    if st.button("Run Circular Trading Scan", key="circles_btn"):
        circles = _get("/graph/detect-circles")
        if circles and circles.get("circles"):
            st.error(f"{circles['circles_found']} circular trading loop(s) detected!")
            for i, c in enumerate(circles["circles"], 1):
                chain = (
                    f"{c.get('name_a', c.get('gstin_a'))} ({c.get('gstin_a')})"
                    f" → {c.get('name_b', c.get('gstin_b'))} ({c.get('gstin_b')})"
                    f" → {c.get('name_c', c.get('gstin_c'))} ({c.get('gstin_c')})"
                    f" → {c.get('name_a', c.get('gstin_a'))} ({c.get('gstin_a')})"
                )
                with st.expander(f"Loop {i}: {c.get('gstin_a')} → {c.get('gstin_b')} → {c.get('gstin_c')} → {c.get('gstin_a')}"):
                    st.write(chain)
                    loop_df = pd.DataFrame([
                        {"Party": "A", "GSTIN": c.get("gstin_a"), "Name": c.get("name_a"), "Risk": c.get("risk_a")},
                        {"Party": "B", "GSTIN": c.get("gstin_b"), "Name": c.get("name_b"), "Risk": c.get("risk_b")},
                        {"Party": "C", "GSTIN": c.get("gstin_c"), "Name": c.get("name_c"), "Risk": c.get("risk_c")},
                    ])
                    st.table(loop_df)
                    st.markdown(
                        f"**Invoices:** A→B: {c.get('inv_a_to_b')} | "
                        f"B→C: {c.get('inv_b_to_c')} | "
                        f"C→A: {c.get('inv_c_to_a')}"
                    )
        else:
            st.success("No fraud patterns detected")


# ═══════════════════════════════════════════════════════
# PAGE: GRAPH ANALYSIS  (Section 8)
# ═══════════════════════════════════════════════════════

def _page_graph_analysis():
    st.markdown(
        '<div class="insp-banner"><h2>Graph Analysis</h2>'
        "<p>Neo4j Knowledge Graph — vendor networks, shadow entities, risk propagation</p></div>",
        unsafe_allow_html=True,
    )

    # ── Section 8: Graph Search ──
    st.subheader("Graph Search — Vendor Network")
    gstin = st.text_input("Graph Search GSTIN", placeholder="e.g. 29HSmIZ8246mZ6", key="graph_gstin")

    if gstin:
        network = _get(f"/dashboard/vendor-network/{gstin}")
        if network and network.get("connections"):
            connections = network["connections"]
            st.success(f"{len(connections)} connection(s) found in the knowledge graph")

            rows = []
            for c in connections:
                rows.append({
                    "Role": c.get("role", "N/A"),
                    "Partner GSTIN": c.get("partner_gstin", "N/A"),
                    "Partner Name": c.get("partner_name", "N/A"),
                    "Risk": c.get("partner_risk", "N/A"),
                    "Invoices": ", ".join(c.get("invoices", [])),
                    "Total Value": f"₹ {c.get('total_value', 0):,.2f}" if c.get("total_value") else "N/A",
                })
            df = pd.DataFrame(rows)

            def _risk_hl(val):
                if val == "HIGH":
                    return "background-color: #ffcdd2; color: #b71c1c;"
                if val == "MEDIUM":
                    return "background-color: #ffe0b2; color: #e65100;"
                return ""

            styled = df.style.map(_risk_hl, subset=["Risk"])
            st.dataframe(styled, width="stretch", hide_index=True)
        elif network:
            st.info("No connections found for this GSTIN in the graph")
        # else: error already shown by _get

    st.divider()

    # ── Shadow Networks ──
    st.subheader("Shadow Network Detection")
    st.caption("Taxpayers sharing the same IP address or phone number")
    if st.button("Scan Shadow Networks", key="shadow_btn"):
        shadow = _get("/graph/find-shadow-networks")
        if shadow and shadow.get("networks"):
            st.warning(f"{shadow['networks_found']} shadow network cluster(s) detected!")
            for net in shadow["networks"]:
                match_type = net.get("match_type", "UNKNOWN")
                shared = net.get("shared_value", "N/A")
                members = net.get("members", [])
                with st.expander(f"{match_type}: {shared} — {len(members)} entities"):
                    mem_rows = []
                    for m in members:
                        mem_rows.append({
                            "GSTIN": m.get("gstin"),
                            "Name": m.get("name"),
                            "Risk": m.get("risk", "N/A"),
                        })
                    st.table(pd.DataFrame(mem_rows))
        else:
            st.success("No shadow networks detected")

    st.divider()

    # ── Risk Score Lookup ──
    st.subheader("Risk Score Calculator")
    risk_gstin = st.text_input("Enter GSTIN for risk scoring", key="risk_gstin")
    if risk_gstin:
        risk = _get(f"/graph/risk-score/{risk_gstin}")
        if risk:
            rc1, rc2, rc3, rc4 = st.columns(4)
            rc1.metric("Risk Score", f"{risk.get('risk_score', 0)} / 100")
            rc2.metric("Risk Level", risk.get("risk_level", "N/A"))
            rc3.metric("Total Neighbors", risk.get("total_neighbors", 0))
            rc4.metric("High-Risk Neighbors", len(risk.get("high_risk_neighbors", [])))

            hr = risk.get("high_risk_neighbors", [])
            if hr:
                st.markdown("**High-Risk Neighbors:**")
                for n in hr:
                    st.markdown(f"- `{n.get('gstin')}` — {n.get('name')}")
        else:
            st.error("GSTIN not found in graph")


# ═══════════════════════════════════════════════════════
# PAGE: REPORTS
# ═══════════════════════════════════════════════════════

def _page_reports():
    st.markdown(
        '<div class="insp-banner"><h2>Reports</h2>'
        "<p>Export fraud detection &amp; compliance reports</p></div>",
        unsafe_allow_html=True,
    )

    # ── Quick summary ──
    summary = _get("/inspector/summary")
    if summary:
        e1, e2, e3, e4 = st.columns(4)
        e1.metric("Taxpayers", summary.get("total_taxpayers", 0))
        e2.metric("Invoices", summary.get("total_invoices", 0))
        e3.metric("Total ITC", f"₹ {summary.get('total_itc_claimed', 0):,.2f}")
        e4.metric("High Risk", summary.get("high_risk_vendors", 0))

    st.divider()

    # ── Action items ──
    st.subheader("Flagged Items")
    action = 0

    fake = _get("/inspector/fake-itc")
    if fake and fake.get("suspects"):
        for s in fake["suspects"]:
            action += 1
            st.error(
                f"**[Fake ITC]** Invoice {s['Invoice_ID']} — "
                f"Buyer: {s['Buyer_GSTIN']}, Seller: {s['Seller_GSTIN']}"
            )

    ewb = _get("/inspector/ewaybill-fraud")
    if ewb and ewb.get("suspects"):
        for s in ewb["suspects"]:
            action += 1
            st.warning(
                f"**[E-Way Bill]** Invoice {s['Invoice_ID']} — "
                f"₹ {s['Value']:,.2f} — Seller: {s['Seller_GSTIN']}, Buyer: {s['Buyer_GSTIN']}"
            )

    if action == 0:
        st.success("No flagged items — system clear")

    st.divider()

    # ── Exports ──
    st.subheader("Export Data")
    col_a, col_b, col_c, col_d = st.columns(4)

    hr_data = _get("/inspector/high-risk")
    if hr_data and hr_data.get("vendors"):
        with col_a:
            df = pd.DataFrame(hr_data["vendors"])
            st.download_button(
                "High Risk Vendors (CSV)",
                df.to_csv(index=False),
                "high_risk_vendors.csv",
                "text/csv",
            )

    comp = _get("/inspector/compliance")
    if comp and comp.get("compliance"):
        with col_b:
            df = pd.DataFrame(comp["compliance"])
            st.download_button(
                "Compliance Table (CSV)",
                df.to_csv(index=False),
                "vendor_compliance.csv",
                "text/csv",
            )

    if fake and fake.get("suspects"):
        with col_c:
            df = pd.DataFrame(fake["suspects"])
            st.download_button(
                "Fake ITC Report (CSV)",
                df.to_csv(index=False),
                "fake_itc_suspects.csv",
                "text/csv",
            )

    if ewb and ewb.get("suspects"):
        with col_d:
            df = pd.DataFrame(ewb["suspects"])
            st.download_button(
                "E-Way Bill Fraud (CSV)",
                df.to_csv(index=False),
                "ewaybill_fraud.csv",
                "text/csv",
            )


# ═══════════════════════════════════════════════════════
# PAGE: LOGOUT
# ═══════════════════════════════════════════════════════

def _page_logout():
    from frontend.login import logout
    logout()
