RATE_LOWER_BOUND = 0.85
RATE_UPPER_BOUND = 1.15
VOLTAGE_SPIKE_MULTIPLIER = 1.5
WINDOW = 10

def detect_discharge_rate_abuse(discharge_ah_series, discharge_duration_series, idx):

    if idx < WINDOW:
        return False  # not enough history

    # compute rate series
    rate_series = [
        discharge_ah_series[i] / discharge_duration_series[i]
        if discharge_duration_series[i] != 0 else 0
        for i in range(idx - WINDOW, idx)
    ]

    # baseline = average of previous WINDOW rates
    baseline = sum(rate_series) / len(rate_series)

    # current rate
    if discharge_duration_series[idx] == 0:
        return False

    current_rate = discharge_ah_series[idx] / discharge_duration_series[idx]

    ratio = current_rate / baseline

    return ratio < RATE_LOWER_BOUND or ratio > RATE_UPPER_BOUND

def detect_low_efficiency(charge_wh, discharge_wh):

    if charge_wh == 0:
        return False

    efficiency = discharge_wh / charge_wh

    return efficiency < 0.8

def detect_voltage_instability(voltage_series, idx):

    if idx < WINDOW:
        return False  # not enough history

    # compute past voltage differences
    diffs = [
        abs(voltage_series[i] - voltage_series[i-1])
        for i in range(idx - WINDOW + 1, idx)
    ]

    if len(diffs) == 0:
        return False

    baseline = sum(diffs) / len(diffs)

    # current difference
    current_diff = abs(voltage_series[idx] - voltage_series[idx-1])

    # detect spike
    return current_diff > (VOLTAGE_SPIKE_MULTIPLIER * baseline)

def detect_time_imbalance(charge_duration, discharge_duration):

    if charge_duration == 0:
        return False

    ratio = discharge_duration / charge_duration

    return ratio < 0.5 or ratio > 2

def compute_usage_stress_score(state):

    score = 0

    if detect_discharge_rate_abuse(state["discharge_ah_series"], state["discharge_duration_series"], state["current_idx"]):
        score += 1

    if detect_low_efficiency(state["charge_wh"], state["discharge_wh"]):
        score += 1

    if detect_time_imbalance(state["charge_duration"], state["discharge_duration"]):
        score += 1

    return score

def classify_usage_abuse(state):

    score = compute_usage_stress_score(state)

    if score >= 2:
        return "high_abuse"
    elif score == 1:
        return "moderate_abuse"
    else:
        return "normal"