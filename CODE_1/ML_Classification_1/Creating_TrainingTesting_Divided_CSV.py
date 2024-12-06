##########################################################################################################################################
# Import Required Modules
##########################################################################################################################################

import pandas as pd

##########################################################################################################################################
# Function for Splitting Data
##########################################################################################################################################

def split_train_test(input_csv_path, train_csv_path, test_csv_path, test_size=0.2):
    """
    Split the combined CSV into train and test sets by keeping the last test_size rows as test 
    and the remaining as train.

    Parameters:
    - input_csv_path (str): Path to the input combined CSV file.
    - train_csv_path (str): Path to save the train CSV file.
    - test_csv_path (str): Path to save the test CSV file.
    - test_size (float): Proportion of the dataset to include in the test split. Default is 0.2 (20%).
    """
    # Load the combined CSV
    df = pd.read_csv(input_csv_path)

    # Calculate the number of rows for the test set
    test_row_count = int(len(df) * test_size)

    # Split the data
    train_df = df.iloc[:-test_row_count]
    test_df = df.iloc[-test_row_count:]

    # Save the train and test sets to separate CSV files
    train_df.to_csv(train_csv_path, index=False)
    test_df.to_csv(test_csv_path, index=False)

    print(f"Train set saved to: {train_csv_path}")
    print(train_df.head())

    print(f"Test set saved to: {test_csv_path}")
    print(test_df.head())

##########################################################################################################################################
# Main Script
##########################################################################################################################################

input_csv_path = "C:\\Users\\ninad\\OneDrive - Washington State University (email.wsu.edu)\\24_CPTS415_TeamJam\\Data\\TrainingTesting\\User3-140617-Combined-TrainTestData.csv"
train_csv_path = "C:\\Users\\ninad\\OneDrive - Washington State University (email.wsu.edu)\\24_CPTS415_TeamJam\\Data\\TrainingTesting\\User3-140617-TrainData-Div.csv"
test_csv_path = "C:\\Users\\ninad\\OneDrive - Washington State University (email.wsu.edu)\\24_CPTS415_TeamJam\\Data\\TrainingTesting\\User3-140617-TestData-Div.csv"

split_train_test(input_csv_path, train_csv_path, test_csv_path, test_size=0.3)

