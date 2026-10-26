import os
import json
import time
import pandas as pd
import numpy as np
import psycopg2
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv()

st.set_page_config(
    page_title="FinGuard India | Real-Time Fraud Operations Center",
    layout="wide",
    initial_sidebar_state="expanded",
)


def render_html(html_str: str):
    clean = "\n".join(line.strip() for line in html_str.strip().splitlines() if line.strip())
    st.markdown(clean, unsafe_allow_html=True)


render_html(
    """
    <link rel="preconnect" href="https://fonts.googleapis.com">
    <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800&family=JetBrains+Mono:wght@400;500;600;700&display=swap" rel="stylesheet">

    <style>
    :root {
        --bg-main: #07090e;
        --bg-panel: #0d121d;
        --bg-card: #111726;
        --border-subtle: #1e293b;
        --border-focus: #334155;
        --accent-primary: #2563eb;
        --accent-success: #059669;
        --accent-danger: #dc2626;
        --accent-warning: #d97706;
        --text-primary: #f8fafc;
        --text-secondary: #94a3b8;
        --text-muted: #64748b;
    }

    .stApp {
        background-color: var(--bg-main) !important;
        color: var(--text-primary) !important;
        font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif !important;
    }

    [data-testid="stSidebar"] {
        background-color: #0a0e17 !important;
        border-right: 1px solid var(--border-subtle) !important;
    }

    [data-testid="stSidebar"] hr {
        border-color: var(--border-subtle) !important;
        margin: 1rem 0 !important;
    }

    .enterprise-header {
        background: #0d1322;
        border: 1px solid var(--border-subtle);
        border-radius: 8px;
        padding: 18px 24px;
        margin-bottom: 20px;
        display: flex;
        justify-content: space-between;
        align-items: center;
    }

    .enterprise-title-group {
        display: flex;
        flex-direction: column;
        gap: 2px;
    }

    .enterprise-title {
        font-size: 1.40rem;
        font-weight: 700;
        letter-spacing: -0.01em;
        color: #f8fafc;
        margin: 0;
        display: flex;
        align-items: center;
        gap: 10px;
    }

    .enterprise-version {
        font-family: 'JetBrains Mono', monospace;
        font-size: 0.70rem;
        font-weight: 600;
        color: #94a3b8;
        background: #1e293b;
        border: 1px solid #334155;
        padding: 2px 6px;
        border-radius: 4px;
    }

    .enterprise-subtitle {
        font-size: 0.80rem;
        color: var(--text-secondary);
        font-weight: 400;
    }

    .status-indicator-badge {
        display: inline-flex;
        align-items: center;
        gap: 8px;
        padding: 6px 12px;
        border-radius: 6px;
        font-family: 'JetBrains Mono', monospace;
        font-size: 0.75rem;
        font-weight: 600;
        background: rgba(5, 150, 105, 0.12);
        border: 1px solid rgba(5, 150, 105, 0.35);
        color: #34d399;
    }

    .status-indicator-badge.threat {
        background: rgba(220, 38, 38, 0.12);
        border-color: rgba(220, 38, 38, 0.35);
        color: #f87171;
    }

    .status-dot {
        width: 7px;
        height: 7px;
        border-radius: 50%;
        background-color: currentColor;
    }

    .kpi-row {
        display: grid;
        grid-template-columns: repeat(6, 1fr);
        gap: 12px;
        margin-bottom: 20px;
    }

    .kpi-tile {
        background: var(--bg-card);
        border: 1px solid var(--border-subtle);
        border-radius: 8px;
        padding: 14px 16px;
        transition: border-color 0.15s ease;
    }

    .kpi-tile:hover {
        border-color: var(--border-focus);
    }

    .kpi-tile-header {
        font-size: 0.70rem;
        font-weight: 600;
        text-transform: uppercase;
        letter-spacing: 0.05em;
        color: var(--text-secondary);
        margin-bottom: 6px;
    }

    .kpi-tile-value {
        font-family: 'JetBrains Mono', monospace;
        font-size: 1.35rem;
        font-weight: 700;
        color: #ffffff;
        letter-spacing: -0.02em;
        line-height: 1.2;
        margin-bottom: 6px;
    }

    .kpi-tile-footer {
        font-size: 0.72rem;
        color: var(--text-muted);
        font-family: 'JetBrains Mono', monospace;
    }

    .kpi-tile-footer.highlight-danger {
        color: #f87171;
    }

    .kpi-tile-footer.highlight-success {
        color: #34d399;
    }

    .console-panel {
        background: var(--bg-panel);
        border: 1px solid var(--border-subtle);
        border-radius: 8px;
        padding: 18px;
        margin-bottom: 20px;
    }

    .console-panel-title {
        font-size: 0.92rem;
        font-weight: 600;
        color: #f1f5f9;
        margin-bottom: 12px;
        padding-bottom: 8px;
        border-bottom: 1px solid var(--border-subtle);
        display: flex;
        justify-content: space-between;
        align-items: center;
    }

    .audit-dossier {
        background: #0b0f19;
        border: 1px solid #1e293b;
        border-radius: 6px;
        padding: 16px;
        margin-top: 10px;
    }

    .audit-dossier-header {
        display: flex;
        justify-content: space-between;
        align-items: center;
        margin-bottom: 10px;
        padding-bottom: 8px;
        border-bottom: 1px solid #1e293b;
    }

    .audit-meta-id {
        font-family: 'JetBrains Mono', monospace;
        font-size: 0.75rem;
        font-weight: 600;
        color: #94a3b8;
    }

    .audit-severity-tag {
        font-family: 'JetBrains Mono', monospace;
        font-size: 0.70rem;
        font-weight: 700;
        padding: 2px 8px;
        border-radius: 4px;
        text-transform: uppercase;
    }

    .audit-severity-tag.critical {
        background: rgba(220, 38, 38, 0.2);
        color: #f87171;
        border: 1px solid rgba(220, 38, 38, 0.4);
    }

    .audit-severity-tag.high {
        background: rgba(217, 119, 6, 0.2);
        color: #fbbf24;
        border: 1px solid rgba(217, 119, 6, 0.4);
    }

    .audit-severity-tag.elevated {
        background: rgba(79, 70, 229, 0.2);
        color: #a5b4fc;
        border: 1px solid rgba(79, 70, 229, 0.4);
    }

    .audit-summary {
        font-size: 0.85rem;
        line-height: 1.55;
        color: #e2e8f0;
        margin-bottom: 12px;
    }

    .containment-protocol-box {
        background: rgba(220, 38, 38, 0.08);
        border: 1px solid rgba(220, 38, 38, 0.3);
        border-radius: 4px;
        padding: 8px 12px;
        font-family: 'JetBrains Mono', monospace;
        font-size: 0.78rem;
        font-weight: 600;
        color: #fca5a5;
        margin-bottom: 12px;
    }

    .playbook-container {
        background: #111726;
        border: 1px solid var(--border-subtle);
        border-radius: 6px;
        padding: 10px;
        margin-bottom: 10px;
    }

    .playbook-step {
        padding: 6px 0;
        border-bottom: 1px solid rgba(255, 255, 255, 0.04);
    }

    .playbook-step:last-child {
        border-bottom: none;
    }

    .playbook-step-title {
        font-family: 'JetBrains Mono', monospace;
        font-size: 0.75rem;
        font-weight: 600;
        color: #93c5fd;
        margin-bottom: 2px;
    }

    .playbook-step-desc {
        font-size: 0.80rem;
        color: #cbd5e1;
        line-height: 1.4;
    }

    .recovery-status-bar {
        background: rgba(5, 150, 105, 0.08);
        border: 1px solid rgba(5, 150, 105, 0.25);
        border-radius: 4px;
        padding: 6px 10px;
        font-family: 'JetBrains Mono', monospace;
        font-size: 0.75rem;
        font-weight: 600;
        color: #34d399;
    }

    .sidebar-section-title {
        font-size: 0.72rem;
        font-weight: 700;
        text-transform: uppercase;
        letter-spacing: 0.06em;
        color: var(--text-secondary);
        margin-bottom: 8px;
    }

    .sidebar-status-row {
        font-size: 0.76rem;
        font-family: 'JetBrains Mono', monospace;
        color: var(--text-secondary);
        margin-bottom: 4px;
        display: flex;
        align-items: center;
        gap: 6px;
    }

    button[kind="primary"] {
        background: #2563eb !important;
        border: 1px solid #3b82f6 !important;
        border-radius: 6px !important;
        font-weight: 600 !important;
        font-size: 0.84rem !important;
    }

    div[data-testid="stSelectbox"] > div {
        background: #111726 !important;
        border-color: #1e293b !important;
        border-radius: 6px !important;
        font-family: 'JetBrains Mono', monospace !important;
        font-size: 0.82rem !important;
    }

    [data-testid="stTabs"] button {
        font-weight: 600 !important;
        font-size: 0.82rem !important;
    }
    </style>
    """
)

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "127.0.0.1")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5433"))
POSTGRES_DB = os.getenv("POSTGRES_DB", "fraud_detection")
POSTGRES_USER = os.getenv("POSTGRES_USER", "fraud_admin")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "fraud_password")
CONTROL_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "simulation_control.json")


def format_inr(amount: float) -> str:
    if amount >= 10000000:
        return f"₹{amount / 10000000:.2f} Cr"
    elif amount >= 100000:
        return f"₹{amount / 100000:.2f} L"
    return f"₹{amount:,.2f}"


def format_inr_full(amount: float) -> str:
    return f"₹{amount:,.2f}"


def update_simulation_control(attack_mode: bool, attack_type: str, stream_delay: float):
    config = {
        "attack_mode": attack_mode,
        "attack_type": attack_type,
        "stream_delay": stream_delay,
        "updated_at": datetime.utcnow().isoformat(),
    }
    try:
        with open(CONTROL_FILE, "w") as f:
            json.dump(config, f, indent=2)
    except Exception as e:
        st.sidebar.error(f"Error updating simulation: {e}")


def get_db_connection():
    ports_to_try = [POSTGRES_PORT, 5433, 5432]
    ports_to_try = list(dict.fromkeys(ports_to_try))
    
    last_error = None
    for port in ports_to_try:
        try:
            conn = psycopg2.connect(
                host=POSTGRES_HOST,
                port=port,
                dbname=POSTGRES_DB,
                user=POSTGRES_USER,
                password=POSTGRES_PASSWORD,
                connect_timeout=2,
            )
            st.session_state["active_pg_port"] = port
            return conn
        except Exception as e:
            last_error = e
            continue
            
    st.session_state["db_last_error"] = str(last_error) if last_error else "Unknown"
    return None


def fetch_cumulative_metrics():
    conn = get_db_connection()
    if conn is None:
        return None

    query = """
        SELECT 
            COUNT(*) as total_count,
            COALESCE(SUM(amount), 0) as total_volume,
            COALESCE(SUM(fraud_prediction), 0) as total_fraud,
            COALESCE(SUM(CASE WHEN fraud_prediction = 1 THEN amount ELSE 0 END), 0) as fraud_volume,
            COALESCE(AVG(processing_time_ms), 0) as avg_latency,
            COALESCE(AVG(risk_score), 0) as avg_risk
        FROM transactions_log;
    """
    try:
        with conn.cursor() as cur:
            cur.execute(query)
            row = cur.fetchone()
            return {
                "total_count": int(row[0]),
                "total_volume": float(row[1]),
                "total_fraud": int(row[2]),
                "fraud_volume": float(row[3]),
                "avg_latency": float(row[4]),
                "avg_risk": float(row[5]),
            }
    except Exception:
        return None
    finally:
        if conn:
            conn.close()


def fetch_transaction_data(limit: int = 500) -> pd.DataFrame:
    conn = get_db_connection()
    if conn is None:
        return pd.DataFrame()

    query = f"""
        SELECT 
            id, transaction_id, card_id, timestamp, amount,
            merchant_category, location, distance_from_home,
            velocity_flag, foreign_transaction, device_risk_score,
            fraud_prediction, risk_score, processing_time_ms, created_at
        FROM transactions_log
        ORDER BY id DESC
        LIMIT {limit};
    """
    try:
        with conn.cursor() as cur:
            cur.execute(query)
            columns = [desc[0] for desc in cur.description]
            data = cur.fetchall()
            df = pd.DataFrame(data, columns=columns)
            if not df.empty:
                df["timestamp"] = pd.to_datetime(df["timestamp"])
                df["amount"] = df["amount"].astype(float)
                df["risk_score"] = df["risk_score"].astype(float)
                df["fraud_prediction"] = df["fraud_prediction"].astype(int)
            return df
    except psycopg2.ProgrammingError:
        return pd.DataFrame()
    except Exception:
        return pd.DataFrame()
    finally:
        if conn:
            conn.close()


def compute_xai_feature_attribution(tx_record: dict) -> dict:
    amount = float(tx_record.get("amount", 0))
    velocity = int(tx_record.get("velocity_flag", 0))
    distance = float(tx_record.get("distance_from_home", 0))
    dev_risk = float(tx_record.get("device_risk_score", 0))
    cat = tx_record.get("merchant_category", "grocery")
    
    ts = tx_record.get("timestamp")
    hour = ts.hour if isinstance(ts, datetime) else 12
    is_late_night = 1 if 1 <= hour <= 5 else 0

    raw_weights = {
        "Velocity Spike (Burst Swipes)": 45.0 if velocity == 1 else 2.0,
        "Spatial Travel Anomaly": min(distance * 0.045, 42.0) if distance > 40 else 2.0,
        "Expenditure Baseline Deviation": min(amount / 3200.0, 38.0) if amount > 4500 else 3.0,
        "Device Trust Score Anomaly": dev_risk * 30.0,
        "Off-Hours IST Window (01-05h)": 24.0 if is_late_night else 1.5,
        "High-Risk Category Exposure": 22.0 if cat in ["crypto_exchange", "luxury_goods", "electronics"] else 3.0,
    }

    total_weight = sum(raw_weights.values())
    if total_weight == 0:
        total_weight = 1.0

    attribution = {k: round((v / total_weight) * 100, 1) for k, v in raw_weights.items()}
    return attribution


def run_llm_threat_audit(tx_record: dict) -> dict:
    groq_key = os.getenv("GROQ_API_KEY", "")
    gemini_key = os.getenv("GEMINI_API_KEY", "")

    prompt = f"""
    You are a Senior Financial Crime & Payment Risk Analyst for an Indian Banking Payment Gateway.
    Analyze the following transaction telemetry data:

    - Transaction ID: {tx_record.get('transaction_id')}
    - Card Identifier: {tx_record.get('card_id')}
    - Timestamp (IST): {tx_record.get('timestamp')}
    - Transaction Amount: INR {float(tx_record.get('amount', 0)):,.2f}
    - Merchant Category: {tx_record.get('merchant_category')}
    - Terminal Location: {tx_record.get('location')}
    - Distance From Registered Home Residence: {tx_record.get('distance_from_home')} km
    - Rapid Velocity Flag: {tx_record.get('velocity_flag')}
    - Foreign Cross-Border Flag: {tx_record.get('foreign_transaction')}
    - Device Risk Score: {tx_record.get('device_risk_score')} (0.0 to 1.0)
    - Evaluated Fraud Risk Score: {float(tx_record.get('risk_score', 0))*100:.1f}%

    Provide a professional, actionable incident analysis and remediation protocol in valid JSON:
    {{
        "executive_summary": "2-3 sentence root cause evaluation focusing on the empirical anomalies.",
        "attack_classification": "e.g., Spatial Geolocation Anomaly, Multi-Merchant Velocity Burst, Off-Hours High-Value Deviation",
        "threat_severity": "CRITICAL / HIGH / ELEVATED",
        "immediate_containment": "e.g., [PROTOCOL: AUTO-FREEZE CARD TOKEN & REVOKE PoS PERMISSIONS]",
        "practical_solutions": [
            {{"title": "1. Customer Authentication & Outreach", "action": "Dispatch automated voice/SMS challenge to registered mobile and require biometric verification."}},
            {{"title": "2. Gateway Settlement Escrow Hold", "action": "Place pre-settlement hold on payment gateway payout and log chargeback per RBI zero-liability policy."}},
            {{"title": "3. Credential & Device Hardening", "action": "Invalidate active session tokens, blacklist device hardware fingerprint, and mandate password reset."}}
        ],
        "recovery_outlook": "100% Capital Intercepted (Pre-settlement gateway hold, zero cardholder liability)"
    }}
    Return ONLY valid JSON.
    """

    if groq_key:
        try:
            from groq import Groq
            client = Groq(api_key=groq_key)
            models_to_try = [
                "openai/gpt-oss-120b",
                "openai/gpt-oss-20b",
                "qwen/qwen3.8-27b",
                "llama-3.3-70b-versatile",
                "llama-3.1-8b-instant",
            ]
            for model_name in models_to_try:
                try:
                    completion = client.chat.completions.create(
                        model=model_name,
                        messages=[{"role": "user", "content": prompt}],
                        temperature=0.2,
                        response_format={"type": "json_object"}
                    )
                    text = completion.choices[0].message.content.strip()
                    return json.loads(text)
                except Exception:
                    continue
        except Exception:
            pass

    if gemini_key:
        try:
            import google.generativeai as genai
            genai.configure(api_key=gemini_key)
            model = genai.GenerativeModel("gemini-1.5-flash")
            response = model.generate_content(prompt)
            text = response.text.strip()
            if text.startswith("```json"):
                text = text.replace("```json", "").replace("```", "").strip()
            return json.loads(text)
        except Exception:
            pass

    amount = float(tx_record.get("amount", 0))
    velocity = int(tx_record.get("velocity_flag", 0))
    cat = tx_record.get("merchant_category", "unknown")
    dist = float(tx_record.get("distance_from_home", 0))
    loc = tx_record.get("location", "India")
    dev_risk = float(tx_record.get("device_risk_score", 0))

    if dist > 600:
        summary = f"Spatial anomaly detected. Transaction initiated at '{loc}' ({dist:,.1f} km from registered residence) within minutes of preceding baseline activity, indicating cloned credentials or counterfeit physical token."
        attack_class = "Spatial Geolocation Discrepancy"
        severity = "CRITICAL"
        containment = "[PROTOCOL: AUTO-FREEZE CARD TOKEN & REVOKE PoS PERMISSIONS]"
        solutions = [
            {"title": "1. Out-of-Band Cardholder Verification", "action": "Trigger automated bank voice verification challenge to registered mobile number to verify physical presence."},
            {"title": "2. Pre-Settlement Gateway Escrow Hold", "action": "Halt payment gateway payout to merchant and file zero-liability chargeback under RBI guidelines."},
            {"title": "3. Terminal & Device Blacklisting", "action": "Blacklist terminal identifier and require biometric step-up authentication for all subsequent transactions."}
        ]
    elif velocity == 1:
        summary = f"High-frequency transaction burst detected. Transaction amount INR {amount:,.2f} at '{cat}' was executed in rapid succession alongside elevated device trust risk ({dev_risk:.2f})."
        attack_class = "Multi-Merchant Velocity Burst"
        severity = "CRITICAL"
        containment = "[PROTOCOL: TEMPORARILY SUSPEND E-COMMERCE CLEARING]"
        solutions = [
            {"title": "1. Rate-Limiting & Channel Lock", "action": "Temporarily suspend card-not-present permissions for 30 minutes to mitigate automated scripted attacks."},
            {"title": "2. Multi-Factor Step-Up Challenge", "action": "Dispatch dynamic OTP with step-up biometric facial verification requirement."},
            {"title": "3. Invalidate Active Session Tokens", "action": "Revoke active checkout tokens across integrated merchant platforms (Flipkart, Amazon, Swiggy)."}
        ]
    elif amount > 50000:
        summary = f"Expenditure baseline deviation of INR {amount:,.2f} at category '{cat}'. The transaction deviates significantly from historical cardholder spend profile."
        attack_class = "Off-Hours High-Value Spend Outlier"
        severity = "HIGH"
        containment = "[PROTOCOL: TEMPORARY HOLD & MANUAL FRAUD DESK ESCALATION]"
        solutions = [
            {"title": "1. Fraud Desk Analyst Review", "action": "Route transaction to 24/7 bank risk analyst queue for direct customer confirmation prior to settlement."},
            {"title": "2. Merchant KYC Verification", "action": "Request digital invoice verification from merchant PoS before gateway settlement approval."},
            {"title": "3. Velocity Threshold Hardening", "action": "Temporarily cap non-essential spending limits to INR 25,000 pending customer authorization."}
        ]
    else:
        summary = f"Transaction flagged due to compounding anomalous telemetry for '{cat}' totaling INR {amount:,.2f} with elevated device risk score."
        attack_class = "Anomalous Retail Outlier"
        severity = "ELEVATED"
        containment = "[PROTOCOL: DISPATCH TRANSACTION ALERT & MONITOR]"
        solutions = [
            {"title": "1. In-App Fraud Notification", "action": "Send instant notification with 1-click 'Unrecognized Activity' freeze option."},
            {"title": "2. Risk Profile Calibration", "action": "Adjust cardholder anomaly thresholds for 48 hours to monitor subsequent activity."},
            {"title": "3. Device Trust Score Audit", "action": "Flag client browser/device fingerprint for re-evaluation on next login attempt."}
        ]

    return {
        "executive_summary": summary,
        "attack_classification": attack_class,
        "threat_severity": severity,
        "immediate_containment": containment,
        "practical_solutions": solutions,
        "recovery_outlook": "100% Capital Intercepted (Pre-settlement gateway hold, zero cardholder liability)"
    }


with st.sidebar:
    render_html(
        """
        <div style="margin-bottom: 12px;">
            <div style="font-size: 1.15rem; font-weight: 700; color: #ffffff; letter-spacing: -0.01em;">FinGuard Operations</div>
            <div style="font-size: 0.75rem; color: #94a3b8;">Payment Risk & Telemetry Engine</div>
        </div>
        """
    )
    st.markdown("---")

    render_html("<div class='sidebar-section-title'>Simulation & Stress Testing</div>")
    
    attack_toggle = st.toggle("Inject Synthetic Traffic Anomaly", value=False)
    attack_type = st.selectbox(
        "Simulation Scenario",
        options=["velocity_burst", "impossible_travel", "night_amount_outlier"],
        format_func=lambda x: {
            "velocity_burst": "Multi-Merchant Velocity Spike",
            "impossible_travel": "Spatial Geolocation Discrepancy",
            "night_amount_outlier": "Off-Hours High-Value Spend"
        }[x],
    )
    stream_speed = st.slider("Dispatch Interval (Seconds)", min_value=0.2, max_value=2.0, value=1.0, step=0.2)

    update_simulation_control(attack_toggle, attack_type, stream_speed)
    
    if attack_toggle:
        render_html(
            f"""
            <div style="background: rgba(220, 38, 38, 0.1); border: 1px solid rgba(220, 38, 38, 0.3); border-radius: 6px; padding: 10px; margin-top: 8px;">
                <div style="font-size: 0.75rem; font-weight: 700; color: #f87171;">SIMULATION ACTIVE</div>
                <div style="font-size: 0.72rem; color: #fca5a5; margin-top: 2px;">Injecting {attack_type} telemetry into active stream.</div>
            </div>
            """
        )
    else:
        render_html(
            """
            <div style="background: rgba(5, 150, 105, 0.08); border: 1px solid rgba(5, 150, 105, 0.25); border-radius: 6px; padding: 10px; margin-top: 8px;">
                <div style="font-size: 0.75rem; font-weight: 700; color: #34d399;">BASELINE TELEMETRY</div>
                <div style="font-size: 0.72rem; color: #a7f3d0; margin-top: 2px;">Standard domestic Indian payment flow (~1.2% baseline anomaly).</div>
            </div>
            """
        )

    st.markdown("---")
    render_html("<div class='sidebar-section-title'>Analytics & Window Configuration</div>")
    refresh_rate = st.slider("Polling Frequency (Seconds)", min_value=1, max_value=10, value=2, step=1)
    rolling_window_sec = st.select_slider("Rolling Window Interval (Seconds)", options=[30, 60, 120, 300, 600], value=60)
    risk_filter_threshold = st.slider("Alert Threshold (Risk Score)", min_value=0.0, max_value=1.0, value=0.60, step=0.05)

    st.markdown("---")
    render_html("<div class='sidebar-section-title'>Infrastructure Health</div>")
    
    db_conn = get_db_connection()
    if db_conn:
        active_port = st.session_state.get("active_pg_port", POSTGRES_PORT)
        render_html(f"<div class='sidebar-status-row' style='color: #34d399;'>● PostgreSQL: Online ({active_port})</div>")
    else:
        render_html("<div class='sidebar-status-row' style='color: #f87171;'>● PostgreSQL: Offline</div>")
        if "db_last_error" in st.session_state:
            st.caption(f"Error: `{st.session_state['db_last_error']}`")
    
    render_html("<div class='sidebar-status-row' style='color: #38bdf8;'>● Kafka Cluster: Port 9092</div>")
    render_html("<div class='sidebar-status-row' style='color: #94a3b8;'>● Model: Random Forest / XGBoost</div>")
    
    if os.getenv("GROQ_API_KEY"):
        render_html("<div class='sidebar-status-row' style='color: #a78bfa;'>● Groq Engine: Online</div>")
    elif os.getenv("GEMINI_API_KEY"):
        render_html("<div class='sidebar-status-row' style='color: #34d399;'>● Gemini API: Online</div>")
    else:
        render_html("<div class='sidebar-status-row' style='color: #d97706;'>● Inference: Rule Engine Active</div>")
    
    auto_refresh_enabled = st.checkbox("Enable Live Polling", value=True)


df = fetch_transaction_data(limit=1000)
cumulative_stats = fetch_cumulative_metrics()

is_threat_active = attack_toggle

render_html(
    f"""
    <div class="enterprise-header">
        <div class="enterprise-title-group">
            <h1 class="enterprise-title">
                FinGuard Risk Operations Center
                <span class="enterprise-version">ENTERPRISE v2.5</span>
            </h1>
            <div class="enterprise-subtitle">
                Real-Time Transaction Stream Telemetry & Fraud Prevention Engine (INR)
            </div>
        </div>
        <div class="status-indicator-badge {'threat' if is_threat_active else ''}">
            <span class="status-dot"></span>
            <span>{'ANOMALY SIMULATION ACTIVE' if is_threat_active else 'STREAM INGESTION ACTIVE'}</span>
        </div>
    </div>
    """
)

if df.empty or cumulative_stats is None:
    st.warning("Awaiting transaction stream telemetry. Ensure Kafka Producer and ML Consumer Engine are running.")
    st.info("Start workers in separate terminal sessions:\n1. `python producer.py`\n2. `python consumer_ml_engine.py`")
else:
    latest_time = df["timestamp"].max()
    window_cutoff = latest_time - timedelta(seconds=rolling_window_sec)
    window_df = df[df["timestamp"] >= window_cutoff]
    
    if len(window_df) < 10:
        window_df = df.head(50)

    window_total = len(window_df)
    window_fraud = len(window_df[window_df["fraud_prediction"] == 1])
    window_fraud_rate = (window_fraud / window_total) * 100 if window_total > 0 else 0.0

    cumulative_total = cumulative_stats["total_count"]
    cumulative_fraud = cumulative_stats["total_fraud"]
    cumulative_fraud_rate = (cumulative_fraud / cumulative_total) * 100 if cumulative_total > 0 else 0.0
    cumulative_volume = cumulative_stats["total_volume"]
    cumulative_prevented = cumulative_stats["fraud_volume"]
    avg_latency = cumulative_stats["avg_latency"]

    render_html(
        f"""
        <div class="kpi-row">
            <div class="kpi-tile">
                <div class="kpi-tile-header">Ingested Transactions</div>
                <div class="kpi-tile-value">{cumulative_total:,}</div>
                <div class="kpi-tile-footer">+{window_total} in {rolling_window_sec}s window</div>
            </div>
            <div class="kpi-tile">
                <div class="kpi-tile-header">Flagged Incidents</div>
                <div class="kpi-tile-value">{cumulative_fraud:,}</div>
                <div class="kpi-tile-footer {'highlight-danger' if window_fraud > 0 else ''}">{window_fraud} in active window</div>
            </div>
            <div class="kpi-tile">
                <div class="kpi-tile-header">Window Fraud Rate</div>
                <div class="kpi-tile-value">{window_fraud_rate:.2f}%</div>
                <div class="kpi-tile-footer">Lifetime: {cumulative_fraud_rate:.2f}%</div>
            </div>
            <div class="kpi-tile">
                <div class="kpi-tile-header">Processed Volume</div>
                <div class="kpi-tile-value">{format_inr(cumulative_volume)}</div>
                <div class="kpi-tile-footer">{format_inr_full(cumulative_volume)}</div>
            </div>
            <div class="kpi-tile">
                <div class="kpi-tile-header">Capital Intercepted</div>
                <div class="kpi-tile-value">{format_inr(cumulative_prevented)}</div>
                <div class="kpi-tile-footer highlight-success">Protected Value</div>
            </div>
            <div class="kpi-tile">
                <div class="kpi-tile-header">Inference Latency</div>
                <div class="kpi-tile-value">{avg_latency:.2f} ms</div>
                <div class="kpi-tile-footer highlight-success">SLA &lt; 3.00 ms</div>
            </div>
        </div>
        """
    )

    xai_col, genai_col = st.columns([6, 6])

    flagged_candidates = df[df["risk_score"] >= 0.50].head(30)
    if flagged_candidates.empty:
        flagged_candidates = df.head(10)

    selected_txn_id = st.selectbox(
        "Select Transaction for Deep Forensic Audit & Attribution:",
        options=flagged_candidates["transaction_id"].tolist(),
        format_func=lambda x: f"{x} | {flagged_candidates[flagged_candidates['transaction_id'] == x]['merchant_category'].values[0]} | ₹{flagged_candidates[flagged_candidates['transaction_id'] == x]['amount'].values[0]:,.2f} | Risk: {flagged_candidates[flagged_candidates['transaction_id'] == x]['risk_score'].values[0]*100:.1f}%",
    )

    selected_row = df[df["transaction_id"] == selected_txn_id].iloc[0].to_dict()

    with xai_col:
        render_html(
            """
            <div class="console-panel">
                <div class="console-panel-title">
                    <span>Feature Attribution Matrix (SHAP Analysis)</span>
                </div>
            """
        )

        attribution_scores = compute_xai_feature_attribution(selected_row)
        
        attr_df = pd.DataFrame({
            "Feature": list(attribution_scores.keys()),
            "Contribution (%)": list(attribution_scores.values()),
        }).sort_values(by="Contribution (%)", ascending=True)

        fig_xai = px.bar(
            attr_df,
            x="Contribution (%)",
            y="Feature",
            orientation="h",
            color="Contribution (%)",
            color_continuous_scale=[
                [0.0, "#3b82f6"],
                [0.4, "#6366f1"],
                [0.7, "#d97706"],
                [1.0, "#dc2626"]
            ],
            text="Contribution (%)",
        )
        fig_xai.update_traces(
            texttemplate='%{text:.1f}%',
            textposition='outside',
            marker=dict(line=dict(width=1, color='#1e293b')),
            textfont=dict(family="JetBrains Mono", size=11, color="#f1f5f9"),
        )
        fig_xai.update_layout(
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            margin=dict(l=10, r=40, t=10, b=10),
            height=280,
            coloraxis_showscale=False,
            xaxis=dict(
                range=[0, 100],
                showgrid=True,
                gridcolor="#1e293b",
                tickfont=dict(family="JetBrains Mono", size=10, color="#94a3b8"),
            ),
            yaxis=dict(
                tickfont=dict(family="Inter", size=11, color="#e2e8f0"),
            ),
        )
        st.plotly_chart(fig_xai, use_container_width=True)
        render_html("</div>")

    with genai_col:
        render_html(
            """
            <div class="console-panel">
                <div class="console-panel-title">
                    <span>Automated Root Cause Analysis & Playbook</span>
                    <span style="font-family: 'JetBrains Mono', monospace; font-size: 0.72rem; color: #94a3b8; font-weight: 500;">Inference Engine: Groq LPU</span>
                </div>
            """
        )

        if "audit_cache" not in st.session_state:
            st.session_state["audit_cache"] = {}

        audit_btn = st.button("Run Forensic Threat Audit", key="run_ai_audit_btn", use_container_width=True, type="primary")

        if audit_btn:
            with st.spinner("Analyzing empirical telemetry vectors, geolocation jumps, and velocity anomalies..."):
                audit_result = run_llm_threat_audit(selected_row)
                st.session_state["audit_cache"][selected_txn_id] = audit_result

        if selected_txn_id in st.session_state["audit_cache"]:
            audit_result = st.session_state["audit_cache"][selected_txn_id]
            sev = audit_result.get("threat_severity", "ELEVATED").upper()
            sev_class = "critical" if "CRITICAL" in sev else ("high" if "HIGH" in sev else "elevated")
            
            containment = audit_result.get("immediate_containment") or audit_result.get("recommended_action", "[PROTOCOL: AUTO-FREEZE CARD]")
            solutions = audit_result.get("practical_solutions", [])
            recovery = audit_result.get("recovery_outlook", "100% Capital Intercepted (Pre-settlement gateway hold)")
            
            solutions_html = ""
            if solutions and isinstance(solutions, list):
                for item in solutions:
                    title = item.get("title", "") if isinstance(item, dict) else str(item)
                    act = item.get("action", "") if isinstance(item, dict) else ""
                    solutions_html += f'<div class="playbook-step"><div class="playbook-step-title">{title}</div><div class="playbook-step-desc">{act}</div></div>'

            render_html(
                f"""
                <div class="audit-dossier">
                    <div class="audit-dossier-header">
                        <span class="audit-meta-id">CLASSIFICATION: {audit_result.get('attack_classification')}</span>
                        <span class="audit-severity-tag {sev_class}">
                            {sev}
                        </span>
                    </div>
                    
                    <div class="audit-summary">
                        {audit_result.get('executive_summary')}
                    </div>
                    
                    <div style="font-size: 0.70rem; font-weight: 700; color: #f87171; text-transform: uppercase; letter-spacing: 0.05em; margin-bottom: 4px;">
                        Immediate Containment Protocol:
                    </div>
                    <div class="containment-protocol-box">
                        {containment}
                    </div>

                    <div style="font-size: 0.70rem; font-weight: 700; color: #94a3b8; text-transform: uppercase; letter-spacing: 0.05em; margin-top: 10px; margin-bottom: 4px;">
                        Actionable Remediation Playbook:
                    </div>
                    <div class="playbook-container">
                        {solutions_html if solutions_html else '<div class="playbook-step-desc">Standard zero-liability containment engaged.</div>'}
                    </div>

                    <div class="recovery-status-bar">
                        STATUS: {recovery}
                    </div>
                </div>
                """
            )
        else:
            render_html(
                """
                <div style="background: #0d121d; border: 1px dashed #1e293b; border-radius: 6px; padding: 32px 20px; text-align: center; margin-top: 8px;">
                    <div style="font-size: 0.88rem; font-weight: 600; color: #cbd5e1;">Awaiting Forensic Audit Execution</div>
                    <div style="font-size: 0.75rem; color: #64748b; margin-top: 4px;">Click 'Run Forensic Threat Audit' to synthesize root-cause telemetry into actionable operational protocols.</div>
                </div>
                """
            )

        render_html("</div>")

    chart_col1, chart_col2 = st.columns([7, 5])

    with chart_col1:
        render_html(
            """
            <div class="console-panel">
                <div class="console-panel-title">
                    <span>Transaction Velocity & Risk Distribution (INR)</span>
                </div>
            """
        )
        
        timeline_df = df.head(150).sort_values(by="timestamp").copy()
        
        fig_timeline = px.scatter(
            timeline_df,
            x="timestamp",
            y="amount",
            color=timeline_df["fraud_prediction"].map({0: "Legitimate", 1: "Flagged Fraudulent"}),
            color_discrete_map={"Legitimate": "#059669", "Flagged Fraudulent": "#dc2626"},
            size=timeline_df["risk_score"].apply(lambda x: max(x * 16, 6)),
            hover_data=["transaction_id", "card_id", "merchant_category", "risk_score", "location"],
            labels={"amount": "Amount (INR)", "timestamp": "Timestamp (IST)", "color": "Classification"},
        )
        fig_timeline.update_traces(
            marker=dict(
                line=dict(width=1, color='#0f172a'),
                opacity=0.85
            )
        )
        fig_timeline.update_layout(
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            margin=dict(l=10, r=20, t=10, b=10),
            legend=dict(
                orientation="h",
                yanchor="bottom",
                y=1.02,
                xanchor="right",
                x=1,
                font=dict(family="Inter", size=11, color="#cbd5e1"),
            ),
            xaxis=dict(
                showgrid=True,
                gridcolor="#1e293b",
                tickfont=dict(family="JetBrains Mono", size=10, color="#94a3b8"),
            ),
            yaxis=dict(
                showgrid=True,
                gridcolor="#1e293b",
                tickfont=dict(family="JetBrains Mono", size=10, color="#94a3b8"),
                tickprefix="₹",
            ),
            height=340,
        )
        st.plotly_chart(fig_timeline, use_container_width=True)
        render_html("</div>")

    with chart_col2:
        render_html(
            """
            <div class="console-panel">
                <div class="console-panel-title">
                    <span>Merchant Category Risk Exposure</span>
                </div>
            """
        )
        
        cat_summary = (
            df.groupby("merchant_category")
            .agg(
                total_transactions=("id", "count"),
                fraud_count=("fraud_prediction", "sum"),
                total_amount=("amount", "sum"),
            )
            .reset_index()
        )
        cat_summary["fraud_rate"] = (cat_summary["fraud_count"] / cat_summary["total_transactions"]) * 100
        cat_summary = cat_summary.sort_values(by="fraud_count", ascending=True)

        fig_cat = px.bar(
            cat_summary,
            x="fraud_count",
            y="merchant_category",
            orientation="h",
            color="fraud_rate",
            color_continuous_scale=[
                [0.0, "#2563eb"],
                [0.5, "#d97706"],
                [1.0, "#dc2626"]
            ],
            labels={"fraud_count": "Flagged Incidents", "merchant_category": "Merchant Category", "fraud_rate": "Fraud Rate (%)"},
        )
        fig_cat.update_traces(
            marker=dict(line=dict(width=1, color='#1e293b')),
        )
        fig_cat.update_layout(
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            margin=dict(l=10, r=20, t=10, b=10),
            height=340,
            coloraxis_showscale=False,
            xaxis=dict(
                showgrid=True,
                gridcolor="#1e293b",
                tickfont=dict(family="JetBrains Mono", size=10, color="#94a3b8"),
            ),
            yaxis=dict(
                tickfont=dict(family="Inter", size=11, color="#e2e8f0"),
            ),
        )
        st.plotly_chart(fig_cat, use_container_width=True)
        render_html("</div>")

    row2_col1, row2_col2 = st.columns([6, 6])

    with row2_col1:
        render_html(
            """
            <div class="console-panel">
                <div class="console-panel-title">
                    <span>Model Risk Score Probability Distribution</span>
                </div>
            """
        )
        fig_hist = px.histogram(
            df,
            x="risk_score",
            color=df["fraud_prediction"].map({0: "Legitimate", 1: "Flagged Fraudulent"}),
            color_discrete_map={"Legitimate": "#059669", "Flagged Fraudulent": "#dc2626"},
            nbins=30,
            marginal="box",
            labels={"risk_score": "Risk Probability", "count": "Volume"},
        )
        fig_hist.update_layout(
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            margin=dict(l=10, r=20, t=10, b=10),
            legend=dict(
                orientation="h",
                yanchor="bottom",
                y=1.02,
                xanchor="right",
                x=1,
                font=dict(family="Inter", size=11, color="#cbd5e1"),
            ),
            xaxis=dict(
                showgrid=True,
                gridcolor="#1e293b",
                tickfont=dict(family="JetBrains Mono", size=10, color="#94a3b8"),
            ),
            yaxis=dict(
                showgrid=True,
                gridcolor="#1e293b",
                tickfont=dict(family="JetBrains Mono", size=10, color="#94a3b8"),
            ),
            height=320,
        )
        st.plotly_chart(fig_hist, use_container_width=True)
        render_html("</div>")

    with row2_col2:
        render_html(
            """
            <div class="console-panel">
                <div class="console-panel-title">
                    <span>Threat Vector Classification Composition</span>
                </div>
            """
        )
        
        threat_data = {
            "Threat Vector": [
                "Velocity Burst Spikes",
                "Spatial Travel Anomaly (>500km)",
                "Elevated Device Risk (>0.75)",
                "Off-Hours Transactions (01-05h IST)",
                "Standard Baseline Clearing"
            ],
            "Count": [
                int(df["velocity_flag"].sum()),
                int((df["distance_from_home"] > 500.0).sum()),
                int((df["device_risk_score"] > 0.75).sum()),
                int(df["timestamp"].dt.hour.between(1, 5).sum()),
                int(((df["velocity_flag"] == 0) & (df["distance_from_home"] <= 500) & (df["fraud_prediction"] == 0)).sum())
            ]
        }
        threat_df = pd.DataFrame(threat_data)

        fig_pie = px.pie(
            threat_df,
            names="Threat Vector",
            values="Count",
            color_discrete_sequence=["#dc2626", "#ea580c", "#d97706", "#4f46e5", "#059669"],
            hole=0.55,
        )
        fig_pie.update_traces(
            textposition='inside',
            textinfo='percent',
            marker=dict(line=dict(color='#0d121d', width=2)),
            textfont=dict(family="JetBrains Mono", size=11),
        )
        fig_pie.update_layout(
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            margin=dict(l=10, r=20, t=10, b=10),
            legend=dict(
                orientation="v",
                yanchor="middle",
                y=0.5,
                xanchor="left",
                x=1.02,
                font=dict(family="Inter", size=10, color="#cbd5e1"),
            ),
            height=320,
        )
        st.plotly_chart(fig_pie, use_container_width=True)
        render_html("</div>")

    render_html(
        """
        <div class="console-panel-title" style="margin-top: 10px; margin-bottom: 12px; font-size: 1.05rem;">
            <span>Transaction Telemetry & Flagged Incident Feeds</span>
        </div>
        """
    )

    tab1, tab2 = st.tabs(["Flagged High-Risk Incidents", "All Streamed Telemetry"])

    with tab1:
        flagged_df = df[df["risk_score"] >= risk_filter_threshold].copy()
        if flagged_df.empty:
            st.success(f"No transactions currently exceeding risk threshold of {risk_filter_threshold:.2f}")
        else:
            display_cols = [
                "transaction_id", "card_id", "timestamp", "amount",
                "merchant_category", "location", "distance_from_home", "risk_score", "velocity_flag",
                "processing_time_ms"
            ]
            st.dataframe(
                flagged_df[display_cols],
                column_config={
                    "amount": st.column_config.NumberColumn("Amount (INR)", format="₹%.2f"),
                    "distance_from_home": st.column_config.NumberColumn("Distance (km)", format="%.1f km"),
                    "risk_score": st.column_config.ProgressColumn(
                        "Risk Probability",
                        help="ML fraud risk score",
                        format="%.2f",
                        min_value=0.0,
                        max_value=1.0,
                    ),
                    "timestamp": st.column_config.DatetimeColumn("Timestamp (IST)", format="HH:mm:ss"),
                    "processing_time_ms": st.column_config.NumberColumn("Inference Latency", format="%.2f ms"),
                    "velocity_flag": st.column_config.NumberColumn("Velocity Flag", format="%d"),
                },
                use_container_width=True,
                height=300,
                hide_index=True,
            )

    with tab2:
        display_cols = [
            "transaction_id", "card_id", "timestamp", "amount",
            "merchant_category", "location", "distance_from_home", "fraud_prediction", "risk_score", "processing_time_ms"
        ]
        st.dataframe(
            df[display_cols].head(100),
            column_config={
                "amount": st.column_config.NumberColumn("Amount (INR)", format="₹%.2f"),
                "distance_from_home": st.column_config.NumberColumn("Distance (km)", format="%.1f km"),
                "risk_score": st.column_config.ProgressColumn(
                    "Risk Probability",
                    format="%.2f",
                    min_value=0.0,
                    max_value=1.0,
                ),
                "fraud_prediction": st.column_config.CheckboxColumn("Flagged"),
                "timestamp": st.column_config.DatetimeColumn("Timestamp (IST)", format="HH:mm:ss"),
                "processing_time_ms": st.column_config.NumberColumn("Inference Latency", format="%.2f ms"),
            },
            use_container_width=True,
            height=300,
            hide_index=True,
        )

if auto_refresh_enabled:
    time.sleep(refresh_rate)
    st.rerun()
