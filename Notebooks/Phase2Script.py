import pandas as pd
import os
import numpy as np

# 1. Load Metadata 
def load_metadata():
    metadata = pd.read_excel(rf"C:\Projects\Battery Engineering\Inventory.xlsx", sheet_name="Sheet1")
    return metadata

# 2. Processing Charge Data
def process_charge_data(Bat, metadata):
    charge=metadata[metadata['Test Type']=='charge']
    chg_files=charge[charge['Battery ID']==Bat].reset_index()

    chg =1
    for id,row in chg_files.iterrows():
        summary_dict={}
        summary_dict['Cycle ID']=row['Cycle ID']
        summary_dict['Cycle Pair']=chg
        summary_dict['Battery ID']=Bat
        path=row['File Path'] + "\\" + row['File Name']
        temp_cycle=pd.read_csv(path)
        temp_cycle['Delta Time']=temp_cycle['Time'].diff().shift(-1)
        temp_cycle['Delta_Voltage']=temp_cycle['Voltage_measured'].diff()
        temp_cycle['Charge_Ah']=temp_cycle['Current_measured']*temp_cycle['Delta Time']/3600
        temp_cycle['Energy_Wh']=temp_cycle['Voltage_measured']*temp_cycle['Current_measured']*temp_cycle['Delta Time']/3600
        temp_cycle['dV/dt']=temp_cycle['Delta_Voltage']/temp_cycle['Delta Time']
        temp_cycle=temp_cycle[temp_cycle['Current_measured']>=0.02]
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

        chg += 1

        
        #Appending to CSV File
        Charge_Summary=pd.DataFrame([summary_dict])
        if not os.path.isfile(rf"C:\Projects\Battery Engineering\Summary Files\Charge Summary\Battery_{Bat}_Charge_Summary.csv"):
            Charge_Summary.to_csv(rf"C:\Projects\Battery Engineering\Summary Files\Charge Summary\Battery_{Bat}_Charge_Summary.csv", index=False)
        else:
            Charge_Summary.to_csv(rf"C:\Projects\Battery Engineering\Summary Files\Charge Summary\Battery_{Bat}_Charge_Summary.csv", mode='a', header=False, index=False)
        #print(f"Processed Charge Cycle {id + 1} for Battery ID {Bat}")

#3. Processing Discharge Data
def process_discharge_data(Bat, metadata):
    discharge=metadata[metadata['Test Type']=='discharge']
    dis_files=discharge[discharge['Battery ID']==Bat].reset_index()

    for id,row in dis_files.iterrows():
        cutoff_voltage=2.5
        summary_dict={}
        summary_dict['Cycle ID']=row['Cycle ID']
        summary_dict['Cycle Pair']=dis
        summary_dict['Battery ID']=Bat
        path=row['File Path'] + "\\" + row['File Name']
        temp_cycle=pd.read_csv(path)
        temp_cycle['Delta Time']=temp_cycle['Time'].diff().shift(-1)
        temp_cycle['Delta_Voltage']=temp_cycle['Voltage_measured'].diff()
        temp_cycle=temp_cycle[(temp_cycle['Current_measured']<-0.05) & (temp_cycle['Voltage_measured']>cutoff_voltage)]

        
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

        dis += 1


        #Appending to CSV File Except File 0 Cycle
        if (dis > 1):
        
            Discharge_Summary=pd.DataFrame([summary_dict])
            if not os.path.isfile(rf"C:\Projects\Battery Engineering\Summary Files\Discharge Summary\Battery_{Bat}_Discharge_Summary.csv"):
                Discharge_Summary.to_csv(rf"C:\Projects\Battery Engineering\Summary Files\Discharge Summary\Battery_{Bat}_Discharge_Summary.csv", index=False)
            else:
                Discharge_Summary.to_csv(rf"C:\Projects\Battery Engineering\Summary Files\Discharge Summary\Battery_{Bat}_Discharge_Summary.csv", mode='a', header=False, index=False)
            
            print(f"Processed Discharge Cycle {id + 1} for Battery ID {Bat}")

def overall_summary(Bat, metadata):
    Charge_Summary=pd.read_csv(rf"C:\Projects\Battery Engineering\Summary Files\Charge Summary\Battery_{Bat}_Charge_Summary.csv")
    Discharge_Summary=pd.read_csv(rf"C:\Projects\Battery Engineering\Summary Files\Discharge Summary\Battery_{Bat}_Discharge_Summary.csv")

    mean_dis_ah=Discharge_Summary['total_ah'].mean()

    min_len=min(len(Charge_Summary), len(Discharge_Summary))
    


    Overall_Stats=[]
    i = 0
    while i < min_len:
        overall_dict={}
        overall_dict['Cycle Pair']=i+1
        overall_dict['Battery ID']=Bat[0]
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
        overall_dict['Ambient Temperature']=min(Charge_Summary.iloc[i]['Ambient_Temperature'], Discharge_Summary.iloc[i]['Ambient_Temperature'])
        overall_dict['Max Charge Temperature']=Charge_Summary.iloc[i]['max_temp']
        overall_dict['Max Discharge Temperature']=Discharge_Summary.iloc[i]['max_temp']
        overall_dict['Charge_Tempaerature_Rise_Rate']=Charge_Summary.iloc[i]['Rise_temp_per_Sec']
        overall_dict['Discharge_Tempaerature_Rise_Rate']=Discharge_Summary.iloc[i]['Rise_temp_per_Sec']
        overall_dict['Coulombic_Efficiency_Ah']=(overall_dict['Discharge_Ah']/overall_dict['Charge_Ah'])*100
        overall_dict['Energy_Efficiency_Wh']=(overall_dict['Discharge_Wh']/overall_dict['Charge_Wh'])*100
        overall_dict['SOH']=(overall_dict['Discharge_Ah']/Discharge_Summary.iloc[0]['total_ah'])*100
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
        i += 1

    #Analysing Overall Stats


    Overall_Stats_DF=pd.DataFrame(Overall_Stats)


    Overall_Stats_DF.to_csv(rf"C:\Projects\Battery Engineering\Summary Files\Overall Summary\Battery_{Bat}_Overall_Summary.csv", index=False)
    Overall_Stats_DF.to_csv(rf"C:\Projects\Battery Engineering\Summary Files\Overall Summary\Battery_Phase_2.csv", index=False, mode='a', header=False)
