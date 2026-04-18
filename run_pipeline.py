import pandas as pd
from src.ingestion.load_data import load_metadata, load_cutoff_data,load_input_path, save_summary

from src.features.battery_features import (
    process_charge_data,
    process_discharge_data,
    overall_summary
)
from src.decision_engine.main import run_diagnosis_pipeline

from src.forecasting.model import run_forecasting,save_forecasting_results

def main():
    print("Starting battery data processing and forecasting Pipeline...")

    #Loading Root Directory and File Paths

    print("Loading data and saving summary...")
    
    metadata_path, cutoff_path, root_path, save_path = load_input_path()

    #Loading Metadata and Cutoff Data and Saving Summary

    print("Foreasting In Progress...")
    
    cycle_summary_path,all_summary = save_summary(metadata_path, cutoff_path, root_path, save_path)
    
    forecasting_results, rul_estimate = run_forecasting(cycle_summary_path)

    forecast_summary = save_forecasting_results(forecasting_results, rul_estimate, save_path)

    print("Applying Issue Detection Logic")

    diagnosis_results = run_diagnosis_pipeline(rul_estimate,all_summary, save_path)

    





    
