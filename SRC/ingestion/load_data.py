import pandas as pd
from src.features.battery_features import (
    process_charge_data,
    process_discharge_data,
    overall_summary
)

def load_input_path():
    print("Enter the path to the metadata file (CSV format):")
    metadata_path = input()
    print("Enter the path to the battery cutoff voltage data file (CSV format):")
    cutoff_path = input()
    print("Enter the path to the root data directory (default: './data/'): ", end="")
    root_path = input() or "./data/"
    print("Enter path to save output (default: './outputs/'): ", end="")
    save_path = input() or "./outputs/"
    return metadata_path, cutoff_path, root_path, save_path

def load_metadata(path):
    return pd.read_csv(path)

def load_cutoff_data(path):
    return pd.read_csv(path)

def save_summary(metadata_path, cutoff_path, root_path, save_path):    

    metadata = load_metadata(metadata_path)
    cutoff_data = load_cutoff_data(cutoff_path)
    battery_list=metadata['Battery ID'].unique()

    All_Battery_Summaries=[]

    for Bat in battery_list:
        print(f"Processing Battery ID: {Bat}")
        cutoff_voltage=cutoff_data[cutoff_data['Battery ID']==Bat]['Cutoff Voltage'].values[0]
        Chg = process_charge_data(Bat, metadata,root_path)
        Dis = process_discharge_data(Bat, metadata,cutoff_voltage,root_path)
        cycle_df=overall_summary(Bat, Chg, Dis)
        

        Chg.to_csv(rf"{save_path}/charge_summary/Battery_{Bat}_Charge_Summary.csv", index=False)
        Dis.to_csv(rf"{save_path}/discharge_summary/Battery_{Bat}_Discharge_Summary.csv", index=False)
        cycle_df.to_csv(rf"{save_path}/overall_summary/Battery_{Bat}_combined_Summary.csv", index=False)
        
        

        All_Battery_Summaries.append(cycle_df)

    All_Battery_Summaries_df=pd.concat(All_Battery_Summaries, ignore_index=True)
    cycle_path = rf"{save_path}/overall_summary"
    All_Battery_Summaries_df.to_csv(rf"{save_path}/All_Batteries_Overall_Summary.csv", index=False)
    return cycle_path,All_Battery_Summaries_df