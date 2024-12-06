##########################################################################################################################################
# Import Required Modules
##########################################################################################################################################

import pandas as pd

##########################################################################################################################################
# Main Script
##########################################################################################################################################

# File paths for the CSVs
file_paths = [
    "C:\\Users\\ninad\\OneDrive - Washington State University (email.wsu.edu)\\24_CPTS415_TeamJam\\Data\\TrainingTesting\\User3-140617-Hand-TrainTestData.csv",
    "C:\\Users\\ninad\\OneDrive - Washington State University (email.wsu.edu)\\24_CPTS415_TeamJam\\Data\\TrainingTesting\\User3-140617-Hips-TrainTestData.csv",
    "C:\\Users\\ninad\\OneDrive - Washington State University (email.wsu.edu)\\24_CPTS415_TeamJam\\Data\\TrainingTesting\\User3-140617-Bag-TrainTestData.csv",
    "C:\\Users\\ninad\\OneDrive - Washington State University (email.wsu.edu)\\24_CPTS415_TeamJam\\Data\\TrainingTesting\\User3-140617-Torso-TrainTestData.csv"
]

# Sensor locations to add as prefixes
sensor_locations = ["Hand", "Hips", "Bag", "Torso"]

# Initialize an empty DataFrame for the combined data
combined_df = None

for file_path, location in zip(file_paths, sensor_locations):
    # Read the CSV file
    df = pd.read_csv(file_path)

    # Add prefixes to the columns except for 'timestamp' and 'coarse_label'
    df = df.rename(columns={col: f"{location}_{col}" for col in df.columns if col not in ["timestamp", "coarse_label"]})

    # Merge with the combined DataFrame on 'timestamp' and 'coarse_label'
    if combined_df is None:
        combined_df = df
    else:
        combined_df = pd.merge(combined_df, df, on=["timestamp", "coarse_label"], how="outer")

# Save the combined DataFrame to a new CSV
output_path = "C:\\Users\\ninad\\OneDrive - Washington State University (email.wsu.edu)\\24_CPTS415_TeamJam\\Data\\TrainingTesting\\User3-140617-Combined-TrainTestData.csv"
combined_df.to_csv(output_path, index=False)

print(f"Combined CSV created successfully at {output_path}")
print(combined_df.head())