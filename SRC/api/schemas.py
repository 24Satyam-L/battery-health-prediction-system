from pydantic import BaseModel
from typing import List, Optional
import pandas as pd

class CycleData(BaseModel):
    soh: float
    charge_ah: float
    discharge_ah: float

    charge_duration: float
    discharge_duration: float

    avg_discharge_voltage: float

    charge_wh: float
    discharge_wh: float

    ambient_temp: float
    max_discharge_temp: float

class BatteryRequest(BaseModel):
    battery_id: str
    cycles: List[CycleData]