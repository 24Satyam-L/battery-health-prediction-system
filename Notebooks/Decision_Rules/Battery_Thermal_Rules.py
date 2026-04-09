OVERHEAT_TEMP = 50
TEMP_RISE_LIMIT = 15
THERMAL_STRESS_LIMIT = 10
THERMAL_STRESS_RATIO = 0.3

def detect_Overheat(max_temp):
    return max_temp > OVERHEAT_TEMP
    
def detect_Temperature_Rise(max_temp, ambient_temp):
    temp_rise = max_temp - ambient_temp
    return temp_rise > TEMP_RISE_LIMIT

def detect_Thermal_Stress(max_temp_series, ambient_temp_series):
    
    rise = [(m - a) for m, a in zip(max_temp_series, ambient_temp_series)]
    high_rise_count = sum(1 for r in rise if r > TEMP_RISE_LIMIT)
    ratio = high_rise_count / len(max_temp_series)

    return ratio 

def evaluate_thermal(state):

    overheat_flags = []
    temp_rise_flags = []
    

    max_temps = state["max_temp_series"]
    ambient_temps = state["ambient_temp_series"]

    for i in range(state["cycle_count"]):

        max_temp = max_temps[i]
        ambient_temp = ambient_temps[i]

        overheat_flags.append(detect_Overheat(max_temp))
        temp_rise_flags.append(detect_Temperature_Rise(max_temp, ambient_temp))
    
    stress_flag = detect_Thermal_Stress(max_temps, ambient_temps)

    return {
        "overheat": overheat_flags,
        "temp_rise": temp_rise_flags,
        "thermal_stress": stress_flag
    }

def summarize_thermal(thermal_flags):

    total = len(thermal_flags["overheat"])

    overheat_ratio = sum(thermal_flags["overheat"]) / total

    stress_ratio = thermal_flags["thermal_stress"]
    

    if overheat_ratio > 0.2 or stress_ratio > THERMAL_STRESS_LIMIT:
        return "frequent_overheating"

    if overheat_ratio > 0.1 or stress_ratio > THERMAL_STRESS_LIMIT/2:
        return "occasional_overheating"

    return "normal"
