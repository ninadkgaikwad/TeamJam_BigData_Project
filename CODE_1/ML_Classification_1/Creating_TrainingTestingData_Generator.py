##########################################################################################################################################
# Import Required Modules
##########################################################################################################################################

import os, math
from datetime import datetime
import time

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
        # Convert to float or return 0.0 if the value is NaN or cannot be converted
        try:
            return float(value) if value != 'NaN' else 0.0
        except ValueError:
            return 0.0

    motion_data = {
        "acceleration": [],
        "gyroscope": [],
        "magnetometer": [],
        "orientation": [],
        "gravity": [],
        "linear_acceleration": [],
        "pressure": [],
        "altitude": [],
        "temperature": []
    }

    start_time = time.time()
    with open(motion_file_path, 'r') as file:
        for line_num, line in enumerate(file):
            if max_lines and line_num >= max_lines:
                print(f"Reached maximum line limit for debugging: {max_lines}")
                break

            values = line.strip().split()
            
            # Ensure the line has the expected number of values
            if len(values) == 23:
                try:
                    # Convert timestamp to an int (multiplying by 100 to preserve decimals if needed)
                    timestamp = int(float(values[0]))

                    # Populate motion data arrays with parsed values
                    motion_data["acceleration"].append({
                        "timestamp": timestamp,
                        "x": to_float_or_zero(values[1]),
                        "y": to_float_or_zero(values[2]),
                        "z": to_float_or_zero(values[3])
                    })
                    
                    motion_data["gyroscope"].append({
                        "x": to_float_or_zero(values[4]),
                        "y": to_float_or_zero(values[5]),
                        "z": to_float_or_zero(values[6])
                    })
                    
                    motion_data["linear_acceleration"].append({
                        "x": to_float_or_zero(values[17]),
                        "y": to_float_or_zero(values[18]),
                        "z": to_float_or_zero(values[19])
                    })

                except Exception as e:
                    print(f"Error parsing line {line_num} in {motion_file_path}: {e}")
                    continue  # Skip this line and move to the next

            else:
                print(f"Skipping line {line_num}: Unexpected format with {len(values)} values")

    end_time = time.time()
    print(f"Finished parsing {motion_file_path} in {end_time - start_time:.2f} seconds")
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
    
    if os.path.exists(label_file_path):
        with open(label_file_path, 'r') as f:
            lines = f.readlines()
            
            for line in lines:
                parts = line.strip().split()
                
                if len(parts) >= 8:
                    label_entry = {
                        "timestamp": int(parts[0]),
                        "coarse_label": float(parts[1])
                    }
                    label_data.append(label_entry)
    
    return label_data