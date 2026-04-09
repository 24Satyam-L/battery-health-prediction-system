import numpy as np

def classify_discharge_behavior(discharge_duration_series):

    if not discharge_duration_series or len(discharge_duration_series) < 5:
        return "unknown"

    cv = np.std(discharge_duration_series) / (np.mean(discharge_duration_series) + 1e-6)

    if cv > 0.5:
        return "inconsistent"
    elif cv > 0.3:
        return "slightly_inconsistent"
    else:
        return "consistent"


def classify_voltage_behavior(voltage_series):

    import numpy as np

    if not voltage_series or len(voltage_series) < 5:
        return "unknown"

    diffs = np.diff(voltage_series)
    volatility = np.std(diffs)

    if volatility > 0.08:
        return "unstable"
    elif volatility > 0.04:
        return "slightly_unstable"
    else:
        return "stable"


def evaluate_usage(state):

    return {
        "discharge_behavior": classify_discharge_behavior(
            state["discharge_duration_series"]
        ),
        "voltage_behavior": classify_voltage_behavior(
            state["voltage_series"]
        )
    }

def summarize_usage(usage_flags):

    if usage_flags["discharge_behavior"] == "inconsistent" and \
       usage_flags["voltage_behavior"] == "unstable":
        return "unstable_usage"

    if usage_flags["discharge_behavior"] == "inconsistent":
        return "irregular_discharge"

    if usage_flags["voltage_behavior"] == "unstable":
        return "electrical_instability"

    if usage_flags["discharge_behavior"] == "slightly_inconsistent":
        return "mild_variation"

    return "stable_usage"