##########################################################################################################################################
# Import Required Modules
##########################################################################################################################################

import pandas as pd
import numpy as np

##########################################################################################################################################
# Function to create windowed data
##########################################################################################################################################

def process_windowed_data(input_csv_path, output_csv_path, time_resolution_minutes):
    """
    Process the input CSV to create windowed slices based on a given time resolution.

    Parameters:
    - input_csv_path (str): Path to the input CSV file (train or test).
    - output_csv_path (str): Path to save the processed CSV file.
    - time_resolution_minutes (int): Time resolution in minutes for windowing.
    """
    # Load the input CSV
    df = pd.read_csv(input_csv_path)

    # Ensure the timestamp column is in datetime format
    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')

    # Set the timestamp as the index
    df.set_index('timestamp', inplace=True)

    # Resample the data into non-overlapping windows
    resampled_df = df.resample(f'{time_resolution_minutes}T').mean()

    # Add the most frequent coarse_label for each window
    resampled_df['coarse_label'] = df['coarse_label'].resample(f'{time_resolution_minutes}T').apply(
        lambda x: x.mode()[0] if not x.mode().empty else 0.0
    )

    # Drop the timestamp column as per requirements
    resampled_df = resampled_df.drop(columns=['timestamp'], errors='ignore')

    # Reset the index
    resampled_df.reset_index(inplace=True)

    # Save the processed data to a new CSV
    resampled_df.to_csv(output_csv_path, index=False)

    print(f"Processed data saved to: {output_csv_path}")
    print(resampled_df.head())

##########################################################################################################################################
# Main Script
##########################################################################################################################################

train_csv_path = "C:\\Users\\ninad\\OneDrive - Washington State University (email.wsu.edu)\\24_CPTS415_TeamJam\\Data\\TrainingTesting\\User3-140617-TrainData-Div.csv"
test_csv_path = "C:\\Users\\ninad\\OneDrive - Washington State University (email.wsu.edu)\\24_CPTS415_TeamJam\\Data\\TrainingTesting\\User3-140617-TestData-Div.csv"
processed_train_csv_path = "C:\\Users\\ninad\\OneDrive - Washington State University (email.wsu.edu)\\24_CPTS415_TeamJam\\Data\\TrainingTesting\\User3-140617-TrainData-Complete.csv"
processed_test_csv_path = "C:\\Users\\ninad\\OneDrive - Washington State University (email.wsu.edu)\\24_CPTS415_TeamJam\\Data\\TrainingTesting\\User3-140617-TestData-Complete.csv"

time_resolution_minutes = 5

process_windowed_data(train_csv_path, processed_train_csv_path, time_resolution_minutes)
process_windowed_data(test_csv_path, processed_test_csv_path, time_resolution_minutes)
