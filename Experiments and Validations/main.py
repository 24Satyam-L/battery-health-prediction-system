import nt

from fastapi import FastAPI

app = FastAPI()
from pydantic import BaseModel

app = FastAPI()

class PredictionOutput(BaseModel):
    result: float
    status: str

class BatteryInput(BaseModel):
    voltage: float
    current: float
    temperature: float

@app.get("/status")
def status():
    return {"system": "Battery Intelligence running"}

@app.post("/predict")
def predict(data: BatteryInput, response_model=PredictionOutput):
        result = data.voltage * data.current
        
        return {
        "result": result,
        "status": "ok"
    }
    

