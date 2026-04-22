from fastapi import FastAPI

from .service import run_analyis,run_prediction,run_recommendation

from typing import List, Optional
import pandas as pd
from .schemas import BatteryRequest

import sys
import os

# --------- App ---------

app = FastAPI()


# --------- Endpoints ---------

@app.post("/recommend")
def recommend(request: BatteryRequest):
    return run_recommendation(request)

@app.post("/Analysis")
def analysis(request: BatteryRequest):
    return run_analyis(request)

@app.post("/predict")
def predict(request: BatteryRequest):
    return run_prediction(request)

