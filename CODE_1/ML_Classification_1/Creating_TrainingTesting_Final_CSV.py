##########################################################################################################################################
# Import Required Modules
##########################################################################################################################################

import pandas as pd

##########################################################################################################################################
# Main Script
##########################################################################################################################################

# Lists of train and test CSV file paths
train_csvs = [
    "C:\\Users\\ninad\\OneDrive - Washington State University (email.wsu.edu)\\24_CPTS415_TeamJam\\Data\\TrainingTesting\\User1-220617-TrainData-Complete.csv", 
    "C:\\Users\\ninad\\OneDrive - Washington State University (email.wsu.edu)\\24_CPTS415_TeamJam\\Data\\TrainingTesting\\User1-260617-TrainData-Complete.csv", 
    "C:\\Users\\ninad\\OneDrive - Washington State University (email.wsu.edu)\\24_CPTS415_TeamJam\\Data\\TrainingTesting\\User1-270617-TrainData-Complete.csv", 
    "C:\\Users\\ninad\\OneDrive - Washington State University (email.wsu.edu)\\24_CPTS415_TeamJam\\Data\\TrainingTesting\\User2-140617-TrainData-Complete.csv", 
    "C:\\Users\\ninad\\OneDrive - Washington State University (email.wsu.edu)\\24_CPTS415_TeamJam\\Data\\TrainingTesting\\User2-140717-TrainData-Complete.csv", 
    "C:\\Users\\ninad\\OneDrive - Washington State University (email.wsu.edu)\\24_CPTS415_TeamJam\\Data\\TrainingTesting\\User2-180717-TrainData-Complete.csv", 
    "C:\\Users\\ninad\\OneDrive - Washington State University (email.wsu.edu)\\24_CPTS415_TeamJam\\Data\\TrainingTesting\\User3-140617-TrainData-Complete.csv", 
    "C:\\Users\\ninad\\OneDrive - Washington State University (email.wsu.edu)\\24_CPTS415_TeamJam\\Data\\TrainingTesting\\User3-030717-TrainData-Complete.csv", 
    "C:\\Users\\ninad\\OneDrive - Washington State University (email.wsu.edu)\\24_CPTS415_TeamJam\\Data\\TrainingTesting\\User3-070717-TrainData-Complete.csv"
]

test_csvs = [
    "C:\\Users\\ninad\\OneDrive - Washington State University (email.wsu.edu)\\24_CPTS415_TeamJam\\Data\\TrainingTesting\\User1-220617-TestData-Complete.csv", 
    "C:\\Users\\ninad\\OneDrive - Washington State University (email.wsu.edu)\\24_CPTS415_TeamJam\\Data\\TrainingTesting\\User1-260617-TestData-Complete.csv", 
    "C:\\Users\\ninad\\OneDrive - Washington State University (email.wsu.edu)\\24_CPTS415_TeamJam\\Data\\TrainingTesting\\User1-270617-TestData-Complete.csv", 
    "C:\\Users\\ninad\\OneDrive - Washington State University (email.wsu.edu)\\24_CPTS415_TeamJam\\Data\\TrainingTesting\\User2-140617-TestData-Complete.csv", 
    "C:\\Users\\ninad\\OneDrive - Washington State University (email.wsu.edu)\\24_CPTS415_TeamJam\\Data\\TrainingTesting\\User2-140717-TestData-Complete.csv", 
    "C:\\Users\\ninad\\OneDrive - Washington State University (email.wsu.edu)\\24_CPTS415_TeamJam\\Data\\TrainingTesting\\User2-180717-TestData-Complete.csv", 
    "C:\\Users\\ninad\\OneDrive - Washington State University (email.wsu.edu)\\24_CPTS415_TeamJam\\Data\\TrainingTesting\\User3-140617-TestData-Complete.csv", 
    "C:\\Users\\ninad\\OneDrive - Washington State University (email.wsu.edu)\\24_CPTS415_TeamJam\\Data\\TrainingTesting\\User3-030717-TestData-Complete.csv", 
    "C:\\Users\\ninad\\OneDrive - Washington State University (email.wsu.edu)\\24_CPTS415_TeamJam\\Data\\TrainingTesting\\User3-070717-TestData-Complete.csv"
]

# Final output file paths
final_train_csv = "C:\\Users\\ninad\\OneDrive - Washington State University (email.wsu.edu)\\24_CPTS415_TeamJam\\Data\\TrainingTesting\\Final_TrainData.csv"
final_test_csv = "C:\\Users\\ninad\\OneDrive - Washington State University (email.wsu.edu)\\24_CPTS415_TeamJam\\Data\\TrainingTesting\\Final_TestData.csv"

# Concatenate train CSVs
train_dataframes = [pd.read_csv(csv) for csv in train_csvs]
final_train_df = pd.concat(train_dataframes, ignore_index=True)
final_train_df.to_csv(final_train_csv, index=False)
print(f"Final train data saved to {final_train_csv}")

# Concatenate test CSVs
test_dataframes = [pd.read_csv(csv) for csv in test_csvs]
final_test_df = pd.concat(test_dataframes, ignore_index=True)
final_test_df.to_csv(final_test_csv, index=False)
print(f"Final test data saved to {final_test_csv}")
