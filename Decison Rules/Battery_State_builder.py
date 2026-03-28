def build_battery_state(summary, cycle):

    # --- Core Series ---
    soh_series = cycle["SOH"].tolist()

    discharge_ah_series = cycle["Discharge_Ah"].tolist()
    charge_ah_series = cycle["Charge_Ah"].tolist()

    discharge_duration_series = cycle["Discharge Duration_Sec"].tolist()
    charge_duration_series = cycle["Charge Duration_Sec"].tolist()

    voltage_series = cycle["Average_Discharge_Voltage"].tolist()

    charge_wh_series = cycle["Charge_Wh"].tolist()
    discharge_wh_series = cycle["Discharge_Wh"].tolist()

    ambient_temp_series = cycle["Ambient Temperature"].tolist()
    max_temp_series = cycle["Max Discharge Temperature"].tolist()

    # --- Derived ---
    latest_soh = soh_series[-1] if soh_series else None

    state = {

        # --- Identifiers ---
        "battery_id": summary["Battery"],

        # --- Degradation / Model Info ---
        "segment": summary["Segment"],
        "model_type": summary["Model"],
        "degradation_slope": summary["Slope"],

        # --- Lifecycle ---
        "remaining_cycles": summary["Remaining_Cycles"],
        "latest_soh": latest_soh,

        # --- Time Series ---
        "soh_series": soh_series,

        "discharge_ah_series": discharge_ah_series,
        "charge_ah_series": charge_ah_series,

        "discharge_duration_series": discharge_duration_series,
        "charge_duration_series": charge_duration_series,

        "voltage_series": voltage_series,

        "charge_wh_series": charge_wh_series,
        "discharge_wh_series": discharge_wh_series,

        "ambient_temp_series": ambient_temp_series,
        "max_temp_series": max_temp_series,

        # --- Meta ---
        "cycle_count": len(soh_series)
    }

    return state