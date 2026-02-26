#Model For Battery Prediction 
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from sklearn.model_selection import LinearRegression
from sklearn.model_selection import mean_squared_error,mean_absolute_error
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

    print(f"Train MAE: {train_mae}, Test MAE: {test_mae}")
    print(f"Train MSE: {train_mse}, Test MSE: {test_mse}")

    # Plotting the regression line
    plt.figure(figsize=(10,6))
    plt.scatter(Cleaned_SOH['Cycle Pair'], Cleaned_SOH['SOH'], color='blue', label='Data Points')
    #plot traun and test predictions
    plt.plot(X_train, y_train_pred, color='red', label='Train Prediction')
    plt.plot(X_test, y_test_pred, color='green', label='Test Prediction')
    plt.xlabel('Cycle Pair')
    plt.ylabel('SOH')
    plt.title('Linear Regression Model for SOH Prediction')
    plt.legend()
    plt.show()

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

    print(f"Train MAE: {train_mae}, Test MAE: {test_mae}")
    print(f"Train MSE: {train_mse}, Test MSE: {test_mse}")

    # Plotting the regression line
    plt.figure(figsize=(10,6))
    plt.scatter(Cleaned_SOH['Cycle Pair'], Cleaned_SOH['SOH'], color='blue', label='Data Points')
    plt.plot(X_train, y_train_pred, color='red', label='Train Prediction')
    plt.plot(X_test, y_test_pred, color='green', label='Test Prediction')
    plt.xlabel('Cycle Pair')
    plt.ylabel('SOH')
    plt.title('Polynomial Regression Model for SOH Prediction')
    plt.legend()
    plt.show()

    return {'coefficients': model.coef_, 'intercept': model.intercept_, 'train_mae': train_mae, 'test_mae': test_mae, 'train_mse': train_mse, 'test_mse': test_mse}

def split_data(Cleaned_SOH,spike_idx):
    if spike_idx !=0:
        return {"Segment 1":Cleaned_SOH.iloc[:spike_idx], "Segment 2":Cleaned_SOH.iloc[spike_idx:]}
    else:
        return {"Segment 1":Cleaned_SOH}


def model_selector( Error_Linear, Error_Polynomial):
    if Error_Linear > Error_Polynomial:
        print("Polynomial Regression model performs better.")
        return "Polynomial Regression"
    else:
        print("Linear Regression model performs better.")
        return "Linear Regression"
    
def main(folder_path):
    for files in os.listdir(folder_path):
        if files.endswith(".csv"):
            file_path = os.path.join(folder_path, files)
            SOH_df = Battery_Dataset(file_path)

            Battery_Name = os.path.basename(file_path).split('.')[0]

            Regression_Results = []
            #Removing Outliers
            SOH_Cleaned = Outlier_Removal(SOH_df)

            #Spike Detection 
            Spike_idx = Spike_Detection(SOH_Cleaned)

            #Splitting if Spike index is not zero 
            Segments = split_data(SOH_Cleaned,Spike_idx)

            #Looping through segments and applying models
            for segment_name, segment_data in Segments.items():
                print(f"Processing {segment_name}...")
                if segment_name == "Segment 1":
                    print("Comparing models for Segment 1...")

                    Linear_Results = apply_linear_regression(segment_data)
                    Polynomial_Results = apply_polynomial_regression(segment_data)

                    if Linear_Results['test_mae'] > Polynomial_Results['test_mae']:
                        print("Polynomial Regression model performs better for Segment 1.")
                        Chosen_Model = "Polynomial Regression"
                        Chosen_results = Polynomial_Results
                    else:
                        print("Linear Regression model performs better for Segment 1.")
                        Chosen_Model = "Linear Regression"
                        Chosen_results = Linear_Results
                else:

                    Results = apply_linear_regression(segment_data)

                    Chosen_Model = "Linear Regression"
                    Chosen_results = Results

                Regression_Results.append({'Battery': Battery_Name, 'Segment': segment_name, 'Model': Chosen_Model, 'Train_MAE': Chosen_results['train_mae'], 'Test_MAE': Chosen_results['test_mae'], 'Train_MSE': Chosen_results['train_mse'], 'Test_MSE': Chosen_results['test_mse']})

    return pd.DataFrame(Regression_Results)


if __name__ == "__main__":
    folder_path = rf"{input('Enter the folder path containing the CSV files: ')}"
    results_df = main()
    print(results_df)

            




