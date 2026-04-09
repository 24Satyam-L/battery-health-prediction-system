LOW_SOH_THRESHOLD = 80




def low_soh_detection(soh):
    return soh < LOW_SOH_THRESHOLD

MODERATE_DEGRADATION = -1
RAPID_DEGRADATION = -2


def classify_degradation(slope):

    if slope > 0:
        return "invalid"   

    elif slope < RAPID_DEGRADATION:
        return "rapid"

    elif slope < MODERATE_DEGRADATION:
        return "moderate"

    else:
        return "normal"

def detect_degradation_level(state):

    if state["model_type"] == "linear":
        return classify_degradation(state["degradation_slope"])

    elif state["model_type"] == "polynomial":

        soh_series = state["soh_series"][-20:]
        max_drop = 0

        for i in range(1, len(soh_series)):
            drop = soh_series[i-1] - soh_series[i]
            max_drop = max(max_drop, drop)


        if max_drop > 0.05:
            return "rapid"
        elif max_drop > 0.02:
            return "moderate"

    return "normal"

def spike_detection(segment):
    return segment != "No Spike"

LOW_RUL_THRESHOLD = 50
MODERATE_RUL_THRESHOLD = 150

def classify_remaining_life(remaining_cycles):

    if not isinstance(remaining_cycles, (int, float)):
        return "unknown"

    if remaining_cycles < LOW_RUL_THRESHOLD:
        return "low"
    elif remaining_cycles <MODERATE_RUL_THRESHOLD:
        return "moderate"
    else:
        return "healthy"
    
def evaluate_battery_health(state):
    soh_flags = low_soh_detection(state["latest_soh"])
    degradation_level = detect_degradation_level(state)
    spike_flag = spike_detection(state["segment"])
    rul_category = classify_remaining_life(state["remaining_cycles"])

    return {
        "low_soh": soh_flags,
        "degradation_level": degradation_level,
        "spike": spike_flag,
        "rul_category": rul_category
    }

def summarize_battery_health(health_flags):

    # --- Critical ---
    if health_flags["low_soh"] and health_flags['rul_category'] == "low":
        return {"level": "critical", "reason": "low_soh_and_low_rul"}

    if health_flags["spike"] and health_flags["degradation_level"] == "rapid":
        return {"level": "critical", "reason": "spike_with_rapid_degradation"}

    # --- Warning ---
    if health_flags["degradation_level"] in ["rapid", "moderate"] and health_flags["low_soh"]:
        return {"level": "warning", "reason": "degrading_with_low_soh"}

    if health_flags["low_soh"]:
        return {"level": "warning", "reason": "low_soh"}

    if health_flags["degradation_level"] in ["rapid", "moderate"]:
        return {"level": "warning", "reason": f"degradation_{health_flags['degradation_level']}"}

    if health_flags["rul_category"] in ["low", "moderate"]:
        return {"level": "warning", "reason": f"{health_flags['rul_category']}_rul"}

    # --- Normal ---
    return {"level": "healthy", "reason": "normal"}