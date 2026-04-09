import sys
import os
import pandas as pd
from schemas import BatteryRequest

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from Decision_Rules.Battery_State_builder import build_battery_state

from Decision_Rules.Battery_Thermal_Rules import summarize_thermal,evaluate_thermal

from Decision_Rules.Battery_Health_Rules import summarize_battery_health,evaluate_battery_health
from Decision_Rules.Battery_Usage_Rules import summarize_usage,evaluate_usage
from Decision_Rules.Battery_Decision_Engine import final_decision_engine

def load_summary(battery_id):
    df = pd.read_csv(rf"C:\Projects\Battery Engineering\Summary Files\Final_Results_Phase_3.csv")

    filtered = df[df["Battery"] == battery_id]

    if filtered.empty:
        raise ValueError(f"Battery {battery_id} not found in summary data")
    
    row = filtered.iloc[0]

    return {
        "Battery": row["Battery"],
        "Segment": row["Segment"],
        "Model": row["Model"],
        "Slope": row["Slope"],
        "Remaining_Cycles": row["Remaining_Cycles"]
    }

def prepare_inputs(request: BatteryRequest):

    # Convert cycles → DataFrame
    cycle_df = pd.DataFrame([c.dict() for c in request.cycles])

    # Rename columns to match your existing function
    cycle_df = cycle_df.rename(columns={
        "soh": "SOH",
        "charge_ah": "Charge_Ah",
        "discharge_ah": "Discharge_Ah",
        "charge_duration": "Charge Duration_Sec",
        "discharge_duration": "Discharge Duration_Sec",
        "avg_discharge_voltage": "Average_Discharge_Voltage",
        "charge_wh": "Charge_Wh",
        "discharge_wh": "Discharge_Wh",
        "ambient_temp": "Ambient Temperature",
        "max_discharge_temp": "Max Discharge Temperature"
    })

    # Build summary dict
    summary = load_summary(request.battery_id)

    return summary, cycle_df

def run_recommendation(request):

    summary, cycle_df = prepare_inputs(request)

    state = build_battery_state(summary, cycle_df)

    thermal = summarize_thermal(evaluate_thermal(state))
    health = summarize_battery_health(evaluate_battery_health(state))
    usage = summarize_usage(evaluate_usage(state))

    decision = final_decision_engine(thermal,health,usage)

    return {
        "battery_id": state["battery_id"],
        "final_level": decision["final_level"],
        "primary_issue": decision["primary_issue"],
        "reason": decision["reason"],
        "action": decision["action"]
    }

def run_analyis(request):

    summary, cycle_df = prepare_inputs(request)

    state = build_battery_state(summary, cycle_df)

    flag_th=evaluate_thermal(state)
    Thermal = summarize_thermal(flag_th)

    flag_health = evaluate_battery_health(state)
    Health = summarize_battery_health(flag_health)

    flag_usage = evaluate_usage(state)
    Usage = summarize_usage(flag_usage)

    return {
        "Thermal Analysis": Thermal,
        "Health Analysis": Health,
        "Usage Analysis": Usage
    }

def generate_soh_curve(current_soh, slope, start_cycle, num_cycles=20):

    predictions = []

    for i in range(1, num_cycles + 1):
        future_cycle = start_cycle + i
        future_soh = current_soh + (slope * i)

        predictions.append({
            "cycle": future_cycle,
            "soh": round(future_soh, 4)
        })

    return predictions

def run_prediction(request):

    summary, cycle_df = prepare_inputs(request)
    state = build_battery_state(summary, cycle_df)

    current_soh = state["latest_soh"]
    slope = summary["Slope"]
    start_cycle = state["cycle_count"]
    predictions = generate_soh_curve(current_soh, slope, start_cycle,num_cycles=20)

    return {
        "status": "success",
        "data": {
            "battery_id": summary["Battery"],
            "current_soh": current_soh,
            "predicted_soh": predictions
        }
    }






