#Model For Battery Prediction 
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error,mean_absolute_error
import os

#Load Dataset 
def Battery_Dataset(file_path):
    Battery_Summary = pd.read_csv(rf"{file_path}")
    return Battery_Summary


#Removing Outliers from the dataset
def Outlier_Removal(Battery_Summary):
    Q1 = Battery_Summary['SOH'].quantile(0.25)
    Q3 = Battery_Summary['SOH'].quantile(0.75)
    IQR = Q3 - Q1
    Lower_Limit = Q1 - 1.5 * IQR
    Upper_Limit = Q3 + 1.5 * IQR

    Cleaned_SOH = Battery_Summary[(Battery_Summary['SOH'] >= Lower_Limit) & (Battery_Summary['SOH'] <= Upper_Limit)].copy()

    return Cleaned_SOH



#Findind Spike in the dataset
def Spike_Detection(Battery_Summary):
    spike_idx =0
    Threshold = 0.05 # Identifying max spike greater than 5% change in Discharge_Ah between consecutive cycles. 
    for i in range(1,len(Battery_Summary)):
        Fluctuation = (Battery_Summary['Discharge_Ah'].iloc[i] - Battery_Summary['Discharge_Ah'].iloc[i-1]) / Battery_Summary['Discharge_Ah'].iloc[i-1]
        if Fluctuation >= Threshold: # Adjust the threshold as needed
            spike_idx = i
            Threshold = Fluctuation # Update the threshold to the new maximum spike

    return spike_idx

# Applying Linear Regression Model
def apply_linear_regression(Cleaned_SOH):
    from sklearn.linear_model import LinearRegression
    from sklearn.metrics import mean_absolute_error, mean_squared_error

    X = Cleaned_SOH[['Cycle Pair']].values
    y = Cleaned_SOH['SOH'].values

    split_idx = int(0.8 * len(Cleaned_SOH))
    X_train, X_test = X[:split_idx], X[split_idx:]
    y_train, y_test = y[:split_idx], y[split_idx:]

    model = LinearRegression()
    model.fit(X_train, y_train)

    y_train_pred = model.predict(X_train)
    y_test_pred = model.predict(X_test)

    #Evaluating Error

    #1. Mean Absolute Error
    train_mae = mean_absolute_error(y_train, y_train_pred)
    test_mae = mean_absolute_error(y_test, y_test_pred)

    #2. Mean Squared Error
    train_mse = mean_squared_error(y_train, y_train_pred)
    test_mse = mean_squared_error(y_test, y_test_pred)

    slope = model.coef_[0]
    intercept = model.intercept_

    return {'slope': slope, 'intercept': intercept, 'train_mae': train_mae, 'test_mae': test_mae, 'train_mse': train_mse, 'test_mse': test_mse}

# Polynimal Regression Model
def apply_polynomial_regression(Cleaned_SOH):
    from sklearn.linear_model import LinearRegression
    from sklearn.metrics import mean_absolute_error, mean_squared_error
    from sklearn.preprocessing import PolynomialFeatures
    X = Cleaned_SOH[['Cycle Pair']].values
    y = Cleaned_SOH['SOH'].values

    #Splitting Data
    split_idx = int(0.8 * len(Cleaned_SOH))
    X_train, X_test = X[:split_idx], X[split_idx:]
    y_train, y_test = y[:split_idx], y[split_idx:]

    # Polynomial transformation
    poly = PolynomialFeatures(degree=2)
    X_train_poly = poly.fit_transform(X_train)
    X_test_poly = poly.transform(X_test)

    # Fit the model
    model = LinearRegression()
    model.fit(X_train_poly, y_train)

    # Predictions
    y_train_pred = model.predict(X_train_poly)
    y_test_pred = model.predict(X_test_poly)

    # Evaluating Error
    #1. Mean Absolute Error
    train_mae = mean_absolute_error(y_train, y_train_pred)
    test_mae = mean_absolute_error(y_test, y_test_pred)

    #2. Mean Squared Error
    train_mse = mean_squared_error(y_train, y_train_pred)
    test_mse = mean_squared_error(y_test, y_test_pred)

    #Calculating Slope
    a = model.coef_[2] # Coefficient of the squared term
    b = model.coef_[1] # Coefficient of the linear term
    slope = a*(X.min()+X.max()) + b # Slope at the midpoint of the data range


    return {'slope': slope, 'intercept': model.intercept_, 'train_mae': train_mae, 'test_mae': test_mae, 'train_mse': train_mse, 'test_mse': test_mse}

def split_data(Cleaned_SOH,spike_idx):
    if spike_idx !=0:
        return {"Pre Spike":Cleaned_SOH.iloc[:spike_idx], "Post Spike":Cleaned_SOH.iloc[spike_idx:]}
    else:
        return {"No Spike":Cleaned_SOH}
    
def Estimate_RUL(Battery, Results_DF, Latest_Cycle):
    # Assuming SOH reaches 80 % at the end of battery life
    Battery_Results = Results_DF[(Results_DF['Battery'] == Battery) & (Results_DF['Segment'].isin(['Post Spike','No Spike']))].tail(1)
    Slope = Battery_Results['Slope'].values[0]
    Intercept = Battery_Results['Intercept'].values[0]
    if Slope < 0:
        predicted_cycle = int((0.8 - Intercept) / Slope)

        Remaining_Cycles = max(0, int(predicted_cycle - Latest_Cycle))

        return predicted_cycle, Remaining_Cycles
    else:
        return float('inf'), float('inf')  # Infinite RUL if slope is zero (no degradation)


    
def main(folder_path):
    Regression_Results = []
    for files in os.listdir(folder_path):
        if files.endswith(".csv"):
            file_path = os.path.join(folder_path, files)
            SOH_df = Battery_Dataset(file_path)
            Latest_Cycle = SOH_df['Cycle Pair'].max()

            Battery_Name = os.path.basename(file_path).split('.')[0].split('_')[0] # Extracting battery name from file name
  
            #Removing Outliers
            SOH_Cleaned = Outlier_Removal(SOH_df)

            #Spike Detection 
            Spike_idx = Spike_Detection(SOH_Cleaned)

            #Splitting if Spike index is not zero 
            Segments = split_data(SOH_Cleaned,Spike_idx)

            #Looping through segments and applying models
            for segment_name, segment_data in Segments.items():
                if (segment_name == "Pre Spike" and Spike_idx != 0) or segment_name == "No Spike":

                    Linear_Results = apply_linear_regression(segment_data)
                    Polynomial_Results = apply_polynomial_regression(segment_data)

                    if Linear_Results['test_mae'] > Polynomial_Results['test_mae']:
                        Chosen_Model = "Polynomial Regression"
                        Chosen_results = Polynomial_Results
                    else:
                        Chosen_Model = "Linear Regression"
                        Chosen_results = Linear_Results
                else:

                    Results = apply_linear_regression(segment_data)

                    Chosen_Model = "Linear Regression"
                    Chosen_results = Results

                Regression_Results.append({'Battery': Battery_Name, 'Segment': segment_name, 'Model': Chosen_Model, 'Slope': Chosen_results.get('slope', None), 'Intercept': Chosen_results.get('intercept', None), 'Train_MAE': Chosen_results['train_mae'], 'Test_MAE': Chosen_results['test_mae'], 'Train_MSE': Chosen_results['train_mse'], 'Test_MSE': Chosen_results['test_mse']})

    Result_DF = pd.DataFrame(Regression_Results)

    Cycle_Estimation = []

    for battery in Result_DF['Battery'].unique():
        predicted_cycle, remaining_cycles = Estimate_RUL(battery, Result_DF, Latest_Cycle)
        Cycle_Estimation.append({'Battery': battery, 'Predicted_End_of_Life_Cycle': predicted_cycle, 'Remaining_Cycles': remaining_cycles})

    Cycle_Estimation_DF = pd.DataFrame(Cycle_Estimation)

    return Result_DF, Cycle_Estimation_DF


if __name__ == "__main__":
    folder_path = rf"C:\Projects\Battery Engineering\Summary Files\Overall Summary"
    results_df, cycle_estimation_df = main(folder_path)
    #print(results_df)
    results_df.to_csv(rf"C:\Projects\Battery Engineering\Summary Files\Regression_Results (Phase_3).csv", index=False)
    cycle_estimation_df.to_csv(rf"C:\Projects\Battery Engineering\Summary Files\Battery_Cycle_Estimation .csv", index=False)

    Final_Results = pd.merge(results_df, cycle_estimation_df, on='Battery', how='left')
    Final_Results.drop(index=Final_Results[Final_Results['Segment'] == 'Pre Spike'].index, inplace=True)
    Final_Results.drop(columns=['Train_MAE', 'Test_MAE'], inplace=True)
    Final_Results.to_csv(rf"C:\Projects\Battery Engineering\Summary Files\Final_Results_Phase_3.csv", index=False)



