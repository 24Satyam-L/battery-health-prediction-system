from Battery_State_builder import build_battery_state
from Battery_Thermal_Rules import evaluate_thermal, summarize_thermal
from Battery_Health_Rules import evaluate_battery_health, summarize_battery_health
from Battery_Usage_Rules import evaluate_usage,summarize_usage
from Battery_Decision_Engine import final_decision_engine  


import pandas as pd

def Battery_Diagnostic_Pipeline(summary, cycle):

    state = build_battery_state(summary, cycle)

    thermal_flags = evaluate_thermal(state)
    thermal_summary = summarize_thermal(thermal_flags)

    health_flags = evaluate_battery_health(state)
    health_summary = summarize_battery_health(health_flags)

    usage_flags = evaluate_usage(state)
    usage_summary = summarize_usage(usage_flags)

    decision = final_decision_engine(thermal_summary, health_summary, usage_summary)

    return {
        "Battery ID": state["battery_id"],
        "Thermal Summary": thermal_summary,
        "Health Level": health_summary["level"],
        "Health Concern": health_summary["reason"],
        "Usage Summary": usage_summary,
        "Final Level": decision["final_level"],
        "Primary Issue": decision["primary_issue"],
        "Reason": decision["reason"],
        "Action": decision["action"]
    }


def run_diagnosis_pipeline(summary, cycle,save_path):
    results = []

    for __, summary_row in summary.iterrows():
        cycle_df = cycle[cycle["Battery ID"] == summary_row["Battery"]]
        battery_id = summary_row["Battery"]

        if cycle_df.empty:
            print(f"No cycle data found for battery {battery_id}. Skipping.")
            continue
        else:
            result = Battery_Diagnostic_Pipeline(summary_row, cycle_df)
            results.append(result)

    results_df = pd.DataFrame(results)
    results_df.to_csv(save_path+ "/Battery_Diagnostic_Results.csv", index=False)
    return results_df