##########################################################################################################################################
# Import Required Modules
##########################################################################################################################################

import pandas as pd

##########################################################################################################################################
# Parsing Functions
##########################################################################################################################################

def parse_motion_file(motion_file_path, max_lines=None):
    """
    Parse the Motion.txt file to extract motion sensor data with enhanced debugging.
    
    Args:
        motion_file_path (str): Path to the motion sensor data file.
        max_lines (int, optional): Maximum lines to parse for debugging. Default is None (no limit).
    
    Returns:
        dict: Dictionary with parsed motion sensor data.
    """
    def to_float_or_zero(value):
        try:
            return float(value) if value != 'NaN' else 0.0
        except ValueError:
            return 0.0

    motion_data = {
        "acceleration": [],
        "gyroscope": [],
        "linear_acceleration": []
    }

    with open(motion_file_path, 'r') as file:
        for line_num, line in enumerate(file):
            if max_lines and line_num >= max_lines:
                break

            values = line.strip().split()
            
            if len(values) == 23:
                try:
                    timestamp = int(float(values[0]))

                    motion_data["acceleration"].append({
                        "timestamp": timestamp,
                        "acc_x": to_float_or_zero(values[1]),
                        "acc_y": to_float_or_zero(values[2]),
                        "acc_z": to_float_or_zero(values[3])
                    })
                    
                    motion_data["gyroscope"].append({
                        "timestamp": timestamp,
                        "gyro_x": to_float_or_zero(values[4]),
                        "gyro_y": to_float_or_zero(values[5]),
                        "gyro_z": to_float_or_zero(values[6])
                    })
                    
                    motion_data["linear_acceleration"].append({
                        "timestamp": timestamp,
                        "li_acc_x": to_float_or_zero(values[17]),
                        "li_acc_y": to_float_or_zero(values[18]),
                        "li_acc_z": to_float_or_zero(values[19])
                    })

                except Exception as e:
                    print(f"Error parsing line {line_num}: {e}")
                    continue
    return motion_data

def parse_labels_file(label_file_path):
    """
    Parse the Labels.txt file inside the recording folder to extract label data.
    
    Args:
        label_file_path (str): Path to the Labels.txt file.
        
    Returns:
        list: List of dictionaries containing label data.
    """
    label_data = []
    with open(label_file_path, 'r') as f:
        for line in f:
            parts = line.strip().split()
            if len(parts) >= 2:  # At least timestamp and coarse_label
                label_data.append({
                    "timestamp": int(parts[0]),
                    "coarse_label": float(parts[1])
                })
    return label_data

##########################################################################################################################################
# Function for Storing Data in CSV
##########################################################################################################################################

def store_motion_data_with_labels(motion_file_path, label_file_path, output_path):
    """
    Parse motion and label files, combine the data, and save it as a CSV file.
    
    Args:
        motion_file_path (str): Path to the motion sensor data file.
        label_file_path (str): Path to the Labels.txt file.
        output_path (str): Path to save the combined CSV file.
    """
    # Parse motion and label files
    motion_data = parse_motion_file(motion_file_path)
    label_data = parse_labels_file(label_file_path)

    # Create pandas DataFrames
    acceleration_df = pd.DataFrame(motion_data["acceleration"])
    gyroscope_df = pd.DataFrame(motion_data["gyroscope"])
    linear_acceleration_df = pd.DataFrame(motion_data["linear_acceleration"])
    labels_df = pd.DataFrame(label_data)

    # Merge all DataFrames on timestamp
    combined_df = pd.merge(acceleration_df, gyroscope_df, on="timestamp", how="outer")
    combined_df = pd.merge(combined_df, linear_acceleration_df, on="timestamp", how="outer")
    combined_df = pd.merge(combined_df, labels_df, on="timestamp", how="outer")

    # Save to CSV
    combined_df.to_csv(output_path, index=False)

    print(f"Motion data with labels stored successfully in {output_path}")
    return combined_df

##########################################################################################################################################
# Main Script
##########################################################################################################################################

motion_file_path = "C:\\Users\\ninad\\OneDrive - Washington State University (email.wsu.edu)\\24_CPTS415_TeamJam\\Data\\RawData\\Uncompressed\\User3\\140617\\Hand_Motion.txt"
label_file_path = "C:\\Users\\ninad\\OneDrive - Washington State University (email.wsu.edu)\\24_CPTS415_TeamJam\\Data\\RawData\\Uncompressed\\User3\\140617\\Label.txt"
output_path = "C:\\Users\\ninad\\OneDrive - Washington State University (email.wsu.edu)\\24_CPTS415_TeamJam\\Data\\TrainingTesting\\User3-140617-Hand-TrainTestData.csv"

combined_df = store_motion_data_with_labels(motion_file_path, label_file_path, output_path)
print(combined_df.head())
