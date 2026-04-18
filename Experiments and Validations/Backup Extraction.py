import pandas as pd
import os
import numpy as np

# 1. Load Metadata 
def load_metadata():
    metadata = pd.read_excel(rf"C:\Projects\Battery Engineering\extra_infos\Inventory.xlsx", sheet_name="Sheet1")
    return metadata

# 2. Processing Charge Data
def process_charge_data(Bat, metadata):
    charge=metadata[metadata['Test Type']=='charge']
    chg_files=charge[charge['Battery ID']==Bat].reset_index()

    #min_charge_index=chg_files['Cycle ID'].min()

    Charge_Summary=[]
    for id,row in chg_files.iterrows():
        summary_dict={}
        summary_dict['Cycle ID']=row['Cycle ID']
        #summary_dict['Cycle Pair']=chg
        summary_dict['Battery ID']=Bat
        path=row['File Path'] + "\\" + row['File Name']
        temp_cycle=pd.read_csv(path)
        print("Processing charge cycle:",path)
        temp_cycle['Delta Time']=temp_cycle['Time'].diff().shift(-1)
        temp_cycle['Delta_Voltage']=temp_cycle['Voltage_measured'].diff()
        temp_cycle['Charge_Ah']=temp_cycle['Current_measured']*temp_cycle['Delta Time']/3600
        temp_cycle['Energy_Wh']=temp_cycle['Voltage_measured']*temp_cycle['Current_measured']*temp_cycle['Delta Time']/3600
        temp_cycle['dV/dt']=temp_cycle['Delta_Voltage']/temp_cycle['Delta Time']
        temp_cycle=temp_cycle[temp_cycle['Current_measured']>=0.02]

        if len(temp_cycle) > 5:
            
            #Cycle Statistics
            summary_dict['total_ah']=temp_cycle['Charge_Ah'].sum()
            summary_dict['total_wh']=temp_cycle['Energy_Wh'].sum()
            summary_dict['cycle_duration']=max(temp_cycle['Time'])-min(temp_cycle['Time'])
            
            #Voltage Statistics
            summary_dict['max_voltage']=temp_cycle['Voltage_measured'].max()
            summary_dict['min_voltage']=temp_cycle['Voltage_measured'].min()
            summary_dict['average_voltage']=temp_cycle['Voltage_measured'].mean()

            summary_dict['max_dVdt']=temp_cycle['dV/dt'].max()
            summary_dict['min_dVdt']=temp_cycle['dV/dt'].min()
            summary_dict['average_dVdt']=temp_cycle['dV/dt'].mean()
            #temperature Statistics
            summary_dict['Ambient_Temperature']=row['ambient_temperature']
            summary_dict['max_temp']=temp_cycle['Temperature_measured'].max()
            summary_dict['min_temp']=temp_cycle['Temperature_measured'].min()
            summary_dict['average_temp']=temp_cycle['Temperature_measured'].mean()
            summary_dict['Rise_temp_per_Sec']=(temp_cycle['Temperature_measured'].iloc[-1]-temp_cycle['Temperature_measured'].iloc[0])/summary_dict['cycle_duration']

            #Current Statistics
            summary_dict['max_current']=temp_cycle['Current_measured'].max()
            summary_dict['min_current']=temp_cycle['Current_measured'].min()
            summary_dict['average_current']=temp_cycle['Current_measured'].mean()

            #chg += 1

            
            #Appending to Original DF
            
            Charge_Summary.append(summary_dict)

        else:
            print(f"⚠️ Warning: No valid charge data for Battery {Bat}, Cycle ID {row['Cycle ID']}")
            continue
    Charge_Summary=pd.DataFrame(Charge_Summary)
        
    return Charge_Summary

#3. Processing Discharge Data
def process_discharge_data(Bat, metadata,cutoff_voltage):
    discharge=metadata[metadata['Test Type']=='discharge']
    dis_files=discharge[discharge['Battery ID']==Bat].reset_index()

    Discharge_Summary=[]
    for id,row in dis_files.iterrows():
        
        summary_dict={}
        summary_dict['Cycle ID']=row['Cycle ID']
        #summary_dict['Cycle Pair']=dis
        summary_dict['Battery ID']=Bat
        path=row['File Path'] + "\\" + row['File Name']
        temp_cycle=pd.read_csv(path)
        temp_cycle['Delta Time']=temp_cycle['Time'].diff().shift(-1)
        temp_cycle['Delta_Voltage']=temp_cycle['Voltage_measured'].diff()
        temp_cycle=temp_cycle[(temp_cycle['Current_measured']<-0.05) & (temp_cycle['Voltage_measured']>cutoff_voltage)]
        print("Processing Discharge Cycle:",path)
        if len(temp_cycle) >5:

        
            temp_cycle['Discharge_Ah']=temp_cycle['Current_measured']*temp_cycle['Delta Time']/3600
            temp_cycle['Energy_Wh']=temp_cycle['Voltage_measured']*temp_cycle['Current_measured']*temp_cycle['Delta Time']/3600
            temp_cycle['dV/dt']=temp_cycle['Delta_Voltage']/temp_cycle['Delta Time']
            
            #Cycle Statistics
            summary_dict['total_ah']=temp_cycle['Discharge_Ah'].sum()*-1
            summary_dict['total_wh']=temp_cycle['Energy_Wh'].sum()*-1
            summary_dict['cycle_duration']=max(temp_cycle['Time'])-min(temp_cycle['Time'])
            
            #Voltage Statistics
            summary_dict['max_voltage']=temp_cycle['Voltage_measured'].max()
            summary_dict['min_voltage']=temp_cycle['Voltage_measured'].min()
            summary_dict['average_voltage']=temp_cycle['Voltage_measured'].mean()

            summary_dict['max_dVdt']=temp_cycle['dV/dt'].max()
            summary_dict['min_dVdt']=temp_cycle['dV/dt'].min()
            summary_dict['average_dVdt']=temp_cycle['dV/dt'].mean()
            #temperature Statistics
            summary_dict['Ambient_Temperature']=row['ambient_temperature']
            summary_dict['max_temp']=temp_cycle['Temperature_measured'].max()
            summary_dict['min_temp']=temp_cycle['Temperature_measured'].min()
            summary_dict['average_temp']=temp_cycle['Temperature_measured'].mean()
            summary_dict['Rise_temp_per_Sec']=(temp_cycle['Temperature_measured'].iloc[-1]-temp_cycle['Temperature_measured'].iloc[0])/summary_dict['cycle_duration']

            #Current Statistics
            summary_dict['max_current']=temp_cycle['Current_measured'].max()
            summary_dict['min_current']=temp_cycle['Current_measured'].min()
            summary_dict['average_current']=temp_cycle['Current_measured'].mean()

            #dis += 1


            #Appending to Original DF
            
            Discharge_Summary.append(summary_dict)
        else:
            print(f"⚠️ Warning: No valid discharge data for Battery {Bat}, Cycle ID {row['Cycle ID']}")
            continue
    Discharge_Summary=pd.DataFrame(Discharge_Summary)

    return Discharge_Summary

def overall_summary(Bat, CHG, DCHG):
    Charge_Summary=CHG
    Discharge_Summary=DCHG

    min_chg_index=Charge_Summary['Cycle ID'].min()
    min_dchg_index=Discharge_Summary['Cycle ID'].min()

    
    #removing Extra Cycles if any
    if min_chg_index > min_dchg_index:
        Discharge_Summary=Discharge_Summary.iloc[1:].reset_index(drop=True)
    
        

    mean_dis_ah=Discharge_Summary['total_ah'].mean()
    #Selecting Base AH
    top_n = 10
    sorted_ah = Discharge_Summary['total_ah'].sort_values(ascending=False)

    n = min(top_n, len(sorted_ah))
    baseline_ah = sorted_ah.iloc[:n].mean()

    min_len=min(len(Charge_Summary), len(Discharge_Summary))
    
    if abs(len(Charge_Summary) - len(Discharge_Summary)) > 1:
        print(f"⚠️ Warning: large mismatch in cycles for Battery {Bat}")

    Overall_Stats=[]
    for i in range(min_len):
        overall_dict={}
        overall_dict['Cycle Pair']=i+1
        overall_dict['Battery ID']=Bat
        overall_dict['Charge_Ah']=Charge_Summary.iloc[i]['total_ah']
        overall_dict['Discharge_Ah']=Discharge_Summary.iloc[i]['total_ah']
        overall_dict['Initial Discharge_Ah']=Discharge_Summary.iloc[0]['total_ah']
        overall_dict['Charge_Wh']=Charge_Summary.iloc[i]['total_wh']
        overall_dict['Discharge_Wh']=Discharge_Summary.iloc[i]['total_wh']
        overall_dict['Charge Duration_Sec']=Charge_Summary.iloc[i]['cycle_duration']
        overall_dict['Discharge Duration_Sec']=Discharge_Summary.iloc[i]['cycle_duration']
        overall_dict['Average_Charge_Voltage']=Charge_Summary.iloc[i]['average_voltage']
        overall_dict['Average_Discharge_Voltage']=Discharge_Summary.iloc[i]['average_voltage']
        overall_dict['Voltage Hysteresis']=overall_dict['Average_Charge_Voltage'] - overall_dict['Average_Discharge_Voltage']
        overall_dict['Ambient Temperature']=max(Charge_Summary.iloc[i]['Ambient_Temperature'], Discharge_Summary.iloc[i]['Ambient_Temperature'])
        overall_dict['Max Charge Temperature']=Charge_Summary.iloc[i]['max_temp']
        overall_dict['Max Discharge Temperature']=Discharge_Summary.iloc[i]['max_temp']
        overall_dict['Charge_Tempaerature_Rise_Rate']=Charge_Summary.iloc[i]['Rise_temp_per_Sec']
        overall_dict['Discharge_Tempaerature_Rise_Rate']=Discharge_Summary.iloc[i]['Rise_temp_per_Sec']
        overall_dict['Coulombic_Efficiency_Ah']=(overall_dict['Discharge_Ah']/overall_dict['Charge_Ah'])*100
        overall_dict['Energy_Efficiency_Wh']=(overall_dict['Discharge_Wh']/overall_dict['Charge_Wh'])*100
        

        overall_dict['SOH']=(overall_dict['Discharge_Ah']/baseline_ah)*100
        if i >=1:
            overall_dict['Capacity_Fade']=(Overall_Stats[-1]['SOH'] - overall_dict['SOH'])
        else:
            overall_dict['Capacity_Fade']=None

        if overall_dict['Charge_Ah']<overall_dict['Discharge_Ah']:
            overall_dict['Cycle Status']='Invalid : Charge Ah less than Discharge Ah'
        elif overall_dict['Discharge_Ah'] < 0.6 * mean_dis_ah:
            overall_dict['Cycle Status']='Invalid : Discharge Ah significantly lower than average'
        else:
            overall_dict['Cycle Status']='Valid'
            
        Overall_Stats.append(overall_dict)
        

    #Analysing Overall Stats


    Overall_Stats_DF=pd.DataFrame(Overall_Stats)

    

    return Overall_Stats_DF



def main():
    metadata=load_metadata()

    bat_Cutoff_df=pd.read_csv(rf"C:\Projects\Battery Engineering\extra_infos\Battery Cutoff Voltage.csv")


    battery_list=metadata['Battery ID'].unique()

    All_Battery_Summaries=[]

    for Bat in battery_list:
        print(f"Processing Battery: {Bat}")
        cutoff_voltage=bat_Cutoff_df[bat_Cutoff_df['Battery ID']==Bat]['Cutoff Voltage'].values[0]
        CHG=process_charge_data(Bat, metadata)
        DCHG=process_discharge_data(Bat, metadata, cutoff_voltage)

        if CHG.empty or DCHG.empty:
            print(f"⚠️ Skipping {Bat}: missing charge or discharge cycles")
            continue
        OVERALL=overall_summary(Bat, CHG, DCHG)
        #Saving charge and discharge summaries
        CHG.to_csv(rf"C:\Projects\Battery Engineering\Summary Files\Charge Summary\{Bat}_Charge_Summary.csv", index=False)
        DCHG.to_csv(rf"C:\Projects\Battery Engineering\Summary Files\Discharge Summary\{Bat}_Discharge_Summary.csv", index=False)

        #saving individual battery overall summary
        OVERALL.to_csv(rf"C:\Projects\Battery Engineering\Summary Files\Overall Summary\{Bat}_Overall_Summary.csv", index=False)
        
        All_Battery_Summaries.append(OVERALL)

    All_Battery_DF=pd.concat(All_Battery_Summaries, ignore_index=True)

    #Saving to CSV file
    All_Battery_DF.to_csv(rf"C:\Projects\Battery Engineering\Summary Files\Overall Summary\All_Batteries_Overall_Summary.csv", index=False)

if __name__ == "__main__":
    main()





