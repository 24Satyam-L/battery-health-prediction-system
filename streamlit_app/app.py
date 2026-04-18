import streamlit as st
import requests
import pandas as pd
import matplotlib.pyplot as plt
import os

BASE_URL = os.getenv("API_URL", "http://127.0.0.1:8000")

st.title("🔋 Battery Intelligence Dashboard")

battery_list = [
    "B0005","B0006","B0007","B0018","B0025","B0026","B0027","B0028",
    "B0029","B0030","B0031","B0032","B0033","B0034","B0036","B0038",
    "B0039","B0040","B0041","B0042","B0043","B0044","B0045","B0046",
    "B0047","B0048","B0049","B0050","B0051","B0052","B0053","B0054",
    "B0055","B0056"
]

st.markdown("### 🔌 Select Battery ID")
battery_id = st.selectbox("", battery_list)

st.markdown("---")

st.subheader("🧾 Enter Cycle Data")


default_data = pd.DataFrame([
    {
        "soh": 98,
        "charge_ah": 2.1,
        "discharge_ah": 2.0,
        "charge_duration": 58,
        "discharge_duration": 55,
        "avg_discharge_voltage": 3.75,
        "charge_wh": 7.8,
        "discharge_wh": 7.5,
        "ambient_temp": 25,
        "max_discharge_temp": 30
    },
    {
        "soh": 92,
        "charge_ah": 2.05,
        "discharge_ah": 2.0,
        "charge_duration": 60,
        "discharge_duration": 56,
        "avg_discharge_voltage": 3.72,
        "charge_wh": 7.6,
        "discharge_wh": 7.3,
        "ambient_temp": 26,
        "max_discharge_temp": 31
    }
])

edited_df = st.data_editor(default_data, num_rows="dynamic")

cycles = edited_df.to_dict(orient="records")

payload = {
    "battery_id": battery_id,
    "cycles": cycles
}

st.markdown("---")

if st.button("🚀 Run Analysis", use_container_width=True):

    # ---------------- Prediction ----------------
    response = requests.post(f"{BASE_URL}/predict", json=payload)
    data = response.json()

    st.subheader("📈 SOH Prediction")

    st.metric(label="Current SOH", value=data["data"]["current_soh"])

    predictions = data["data"]["predicted_soh"]
    cycles_plot = [p["cycle"] for p in predictions]
    soh_plot = [p["soh"] for p in predictions]

    fig, ax = plt.subplots()
    ax.plot(cycles_plot, soh_plot)
    ax.set_xlabel("Cycle")
    ax.set_ylabel("SOH")
    ax.set_title("SOH Forecast")

    st.pyplot(fig)

    st.markdown("---")

    # ---------------- Analysis ----------------
    analysis_response = requests.post(f"{BASE_URL}/Analysis", json=payload)
    analysis = analysis_response.json()

    st.subheader("📊 Battery Analysis")

    st.markdown("### 🌡️ Thermal")
    st.info(analysis["Thermal Analysis"])

    st.markdown("### 🔋 Health")
    st.warning(
        analysis["Health Analysis"]["level"] + " - " +
        analysis["Health Analysis"]["reason"]
    )

    st.markdown("### ⚙️ Usage")
    st.info(analysis["Usage Analysis"])

    st.markdown("---")

    # ---------------- Decision ----------------
    rec_response = requests.post(f"{BASE_URL}/recommend", json=payload)
    rec = rec_response.json()

    st.subheader("🚨 Final Decision")

    level = rec["final_level"]

    if level == "critical":
        st.error("🔥 CRITICAL")
    elif level == "warning":
        st.warning("⚠️ WARNING")
    else:
        st.success("✅ HEALTHY")

    st.markdown(f"**🔎 Issue:** {rec['primary_issue']}")
    st.markdown(f"**📌 Reason:** {rec['reason']}")
    st.markdown(f"**🛠️ Recommended Action:** {rec['action']}")