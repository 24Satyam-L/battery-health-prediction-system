def final_decision_engine(thermal, health, usage):

    # --- CRITICAL ---
    if thermal == "frequent_overheating":
        return {
            "final_level": "critical",
            "primary_issue": "thermal",
            "reason": "frequent_overheating",
            "action": "immediate_cooling_or_shutdown"
        }

    if health["level"] == "critical":
        return {
            "final_level": "critical",
            "primary_issue": "degradation",
            "reason": health["reason"],
            "action": "battery_replacement_required"
        }

    # --- WARNING ---
    if thermal == "occasional_overheating":
        return {
            "final_level": "warning",
            "primary_issue": "thermal",
            "reason": "occasional_overheating",
            "action": "reduce_load_and_monitor_temperature"
        }

    if health["level"] == "warning":
        return {
            "final_level": "warning",
            "primary_issue": "degradation",
            "reason": health["reason"],
            "action": "plan_maintenance"
        }

    # --- USAGE ---
    if usage in ["unstable_usage", "irregular_discharge", "electrical_instability"]:
        return {
            "final_level": "warning",
            "primary_issue": "usage",
            "reason": usage,
            "action": "optimize_usage_patterns"
        }

    # --- HEALTHY ---
    return {
        "final_level": "healthy",
        "primary_issue": "none",
        "reason": "all_systems_normal",
        "action": "normal_operation"
    }