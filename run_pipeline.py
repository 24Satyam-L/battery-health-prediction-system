import pandas as pd
import time

from src.ingestion.load_data import load_input_path, save_summary
from src.forecasting.model import run_forecasting, save_forecasting_results
from src.decision_engine.main import run_diagnosis_pipeline


def log_step(message):
    print(f"\n🔹 {message}...")


def log_success(message):
    print(f"✅ {message}")


def main():
    start_time = time.time()

    print("\n" + "=" * 60)
    print("🔋 BATTERY INTELLIGENCE PIPELINE STARTED")
    print("=" * 60)

    try:
        # -------------------- Step 1 --------------------
        log_step("Loading input paths")
        metadata_path, cutoff_path, root_path, save_path = load_input_path()
        log_success("Paths loaded")

        # -------------------- Step 2 --------------------
        log_step("Processing raw data & generating summaries")
        cycle_summary_path, all_summary = save_summary(
            metadata_path, cutoff_path, root_path, save_path
        )
        log_success("Data processing completed")

        # -------------------- Step 3 --------------------
        log_step("Running forecasting models (SOH & RUL)")
        forecasting_results, rul_estimate = run_forecasting(cycle_summary_path)
        forecast_summary = save_forecasting_results(
            forecasting_results, rul_estimate, save_path
        )
        log_success("Forecasting completed")

        # -------------------- Step 4 --------------------
        log_step("Applying decision engine (diagnostics & recommendations)")
        diagnosis_results = run_diagnosis_pipeline(
            forecast_summary, all_summary, save_path
        )
        log_success("Decision analysis completed")

        # -------------------- Done --------------------
        total_time = round(time.time() - start_time, 2)

        print("\n" + "=" * 60)
        print("🎉 PIPELINE COMPLETED SUCCESSFULLY")
        print(f"⏱️ Total Execution Time: {total_time} seconds")
        print("=" * 60 + "\n")

    except Exception as e:
        print("\n" + "=" * 60)
        print("❌ PIPELINE FAILED")
        print(f"Error: {str(e)}")
        print("=" * 60 + "\n")


if __name__ == "__main__":
    main()