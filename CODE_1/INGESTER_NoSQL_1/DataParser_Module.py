##########################################################################################################################################
##########################################################################################################################################
# Data Parser Module
##########################################################################################################################################
##########################################################################################################################################

##########################################################################################################################################
# Import Required Modules
##########################################################################################################################################

import os, math
from datetime import datetime

##########################################################################################################################################
# Function: To read 00inf.txt file contents
##########################################################################################################################################
def parse_recording_info_file(recording_path):
    """
    Parse the 00inf.txt file inside the recording folder to extract metadata.
    
    Args:
        recording_path (str): Path to the recording folder containing 00inf.txt.
        
    Returns:
        dict: Dictionary with parsed metadata from 00inf.txt.
    """
    info_file_path = os.path.join(recording_path, "00inf.txt")
    metadata = {}

    if os.path.exists(info_file_path):
        with open(info_file_path, 'r') as f:
            lines = f.readlines()
            for line in lines:

                if (line.split(':')[0] == "recdate"):

                    key = line.split(':')[0]

                    # Parse the datetime string into a naive datetime object
                    utc_datetime = datetime.strptime(line.split()[1] + " " + line.split()[2], "%H:%M:%S %d.%m.%Y")

                    # Convert the naive UTC datetime to Unix time
                    value = int(utc_datetime.timestamp())*1000

                else:

                    key, value = line.split(':')
                    key = key.strip()
                    value = value.strip()

                if key == 'sessionid':
                    metadata['user_id'] = value
                elif key == 'timemsmin':
                    metadata['start_time_ms'] = int(value)
                elif key == 'timemsmax':
                    metadata['end_time_ms'] = int(value)
                elif key == 'recdate':
                    metadata['recording_start_date'] = value  # Could be formatted if needed
                elif key == 'reclength':
                    metadata['recording_length_ms'] = int(value)
                elif key == 'recid':
                    metadata['recording_id'] = value

    return metadata
    
##########################################################################################################################################
# Function: To read """_Ambient.txt file contents
##########################################################################################################################################
def parse_ambient_sensor_file(sensor_file_path):
    """
    Parse the Ambient sensor file to extract lumix and temperature data.
    
    Args:
        sensor_file_path (str): Path to the ambient sensor file (e.g., Bag_Ambient.txt).
        
    Returns:
        list: A list of dictionaries containing timestamp, lumix, and temperature data.
    """
    ambient_data = []
    
    if os.path.exists(sensor_file_path):
        with open(sensor_file_path, 'r') as f:
            for line in f:
                columns = line.strip().split()  # Assuming space-delimited values
                if len(columns) >= 6:
                    timestamp = int(columns[0])  # Time ms
                    lumix = float(columns[4])  # Lumix value
                    temperature = float(columns[5])  # Temperature value
                    
                    # Append the parsed data to the list
                    ambient_data.append({
                        "timestamp": timestamp,
                        "lumix": lumix,
                        "temperature": temperature
                    })
    
    return ambient_data
    
    
##########################################################################################################################################
# Function: To read """_Battery.txt file contents
##########################################################################################################################################
def parse_battery_sensor_file(battery_file_path):
    """
    Parse the Battery.txt file inside the recording folder to extract battery sensor data.
    
    Args:
        battery_file_path (str): Path to the Battery.txt file.
        
    Returns:
        list: List of dictionaries with parsed battery sensor data.
    """
    battery_data = []

    if os.path.exists(battery_file_path):
        with open(battery_file_path, 'r') as f:
            lines = f.readlines()
            for line in lines:
                parts = line.strip().split()
                if len(parts) >= 5:
                    battery_entry = {
                        "timestamp": int(parts[0]),  # Time in ms
                        "battery_level": float(parts[3]),  # Battery level
                        "temperature": float(parts[4])  # Temperature
                    }
                    battery_data.append(battery_entry)

    return battery_data

##########################################################################################################################################
# Function: To read """_API.txt file contents
##########################################################################################################################################
def parse_api_file(file_path):
    """
    Parse the API sensor file to extract confidence readings.
    
    Args:
        file_path (str): Path to the API sensor file.
        
    Returns:
        list: List of dictionaries representing the API confidence readings.
    """
    api_data = []
    
    with open(file_path, 'r') as f:
        for line in f:
            parts = line.strip().split()
            if len(parts) < 11:
                continue  # Skip if the line doesn't have enough columns
            api_data.append({
                "timestamp": int(parts[0]),  # Time in ms
                "still_confidence": float(parts[3]),  # Still confidence (0-100%)
                "on_foot_confidence": float(parts[4]),  # On foot confidence (0-100%)
                "walking_confidence": float(parts[5]),  # Walking confidence (0-100%)
                "running_confidence": float(parts[6]),  # Running confidence (0-100%)
                "on_bicycle_confidence": float(parts[7]),  # On bicycle confidence (0-100%)
                "in_vehicle_confidence": float(parts[8]),  # In vehicle confidence (0-100%)
                "tilting_confidence": float(parts[9]),  # Tilting confidence (0-100%)
                "unknown_confidence": float(parts[10])  # Unknown confidence (0-100%)
            })
    return api_data

##########################################################################################################################################
# Function: To read """_Location.txt file contents
##########################################################################################################################################
def parse_location_file(location_file_path):
    """
    Parse the Location.txt file to extract location data.
    
    Args:
        location_file_path (str): Path to the location sensor data file.
    
    Returns:
        list: List of dictionaries with keys 'timestamp', 'accuracy', 'latitude', 'longitude', and 'altitude'.
    """
    location_data = []
    with open(location_file_path, 'r') as file:
        for line in file:
            # Split the line into individual components
            values = line.strip().split()
            # Parse the individual values
            timestamp = int(values[0])
            accuracy = float(values[3])
            latitude = float(values[4])
            longitude = float(values[5])
            altitude = float(values[6])
            # Create a dictionary for each line of data
            location_data.append({
                "timestamp": timestamp,
                "accuracy": accuracy,
                "latitude": latitude,
                "longitude": longitude,
                "altitude": altitude
            })
    return location_data

##########################################################################################################################################
# Function: To read """_Motion.txt file contents
##########################################################################################################################################
import time

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
                        "timestamp": timestamp,
                        "x": to_float_or_zero(values[4]),
                        "y": to_float_or_zero(values[5]),
                        "z": to_float_or_zero(values[6])
                    })
                    
                    motion_data["magnetometer"].append({
                        "timestamp": timestamp,
                        "x": to_float_or_zero(values[7]),
                        "y": to_float_or_zero(values[8]),
                        "z": to_float_or_zero(values[9])
                    })
                    
                    motion_data["orientation"].append({
                        "timestamp": timestamp,
                        "w": to_float_or_zero(values[10]),
                        "x": to_float_or_zero(values[11]),
                        "y": to_float_or_zero(values[12]),
                        "z": to_float_or_zero(values[13])
                    })
                    
                    motion_data["gravity"].append({
                        "timestamp": timestamp,
                        "x": to_float_or_zero(values[14]),
                        "y": to_float_or_zero(values[15]),
                        "z": to_float_or_zero(values[16])
                    })
                    
                    motion_data["linear_acceleration"].append({
                        "timestamp": timestamp,
                        "x": to_float_or_zero(values[17]),
                        "y": to_float_or_zero(values[18]),
                        "z": to_float_or_zero(values[19])
                    })
                    
                    motion_data["pressure"].append({
                        "timestamp": timestamp,
                        "value": to_float_or_zero(values[20])
                    })
                    
                    motion_data["altitude"].append({
                        "timestamp": timestamp,
                        "value": to_float_or_zero(values[21])
                    })
                    
                    motion_data["temperature"].append({
                        "timestamp": timestamp,
                        "value": to_float_or_zero(values[22])
                    })

                except Exception as e:
                    print(f"Error parsing line {line_num} in {motion_file_path}: {e}")
                    continue  # Skip this line and move to the next

            else:
                print(f"Skipping line {line_num}: Unexpected format with {len(values)} values")

    end_time = time.time()
    print(f"Finished parsing {motion_file_path} in {end_time - start_time:.2f} seconds")
    return motion_data


##########################################################################################################################################
# Function: To read """_DeprCells.txt file contents
##########################################################################################################################################
def parse_deprcells_file(deprcells_file_path):
    """
    Parses a DeprCells sensor file and extracts the data according to the schema.
    
    Args:
        deprcells_file_path (str): Path to the DeprCells sensor file.
    
    Returns:
        list: List of dictionaries, each representing a sensor data reading.
    """
    
    def to_int_or_default(value, default=0):
        """Convert value to int or return default if conversion fails."""
        try:
            return float(value)
        except ValueError:
            return default

    def to_float_or_default(value, default=0.0):
        """Convert value to float or return default if conversion fails."""
        try:
            return float(value)
        except ValueError:
            return default

    depr_cells_data = []
    
    # Read the DeprCells file line by line
    with open(deprcells_file_path, 'r') as file:
        lines = file.readlines()
        
        for line in lines:
            parts = line.strip().split()  # Split by whitespace
            if len(parts) >= 9:
                depr_cells_data.append({
                    "timestamp": int(parts[0]),  # Time in ms
                    "network_bsonType": parts[3],                  # Network type
                    "cid": parts[4],                           # Cell ID
                    "lac": parts[5],                           # Location Area Code
                    "dbm": to_float_or_default(parts[6]),      # Signal strength in dBm
                    "mcc": to_int_or_default(parts[7]),        # Mobile Country Code
                    "mns": to_int_or_default(parts[8])         # Mobile Network Code
                })
    
    return depr_cells_data

##########################################################################################################################################
# Function: To read """_WiFi.txt file contents
##########################################################################################################################################
def parse_wifi_file(wifi_file_path):
    """
    Parses a Wifi sensor file and extracts the data according to the flattened schema.
    
    Args:
        wifi_file_path (str): Path to the Wifi sensor file.
    
    Returns:
        list: List of dictionaries, each representing a flattened wifi network reading.
    """
    wifi_data = []
    
    # Read the WiFi file line by line
    with open(wifi_file_path, 'r', encoding='utf-8') as file:
        lines = file.readlines()
        for line in lines:
            parts = line.strip().split(';')  # Split by semicolon for Wifi fields

            # Creating WiFi_Data_Dict
            WiFi_Data_Dict = {}
            
            # Extract timestamp
            try:
                timestamp = int(parts[0])  # First element is timestamp

                WiFi_Data_Dict["timestamp"] = timestamp

            except ValueError:
                print(f"Skipping line due to invalid timestamp: {line}")
                continue  # Skip this line if timestamp is not an integer            
            
            # Creating WiFi_Data_Dict
            WiFi_Data_List = []

            # Parse each WiFi network's details starting from the 4th column
            for i in range(4, len(parts), 5):  # BSSID, SSID, RSSI, Frequency, Capabilities
                if i + 4 < len(parts):  # Ensure all 5 fields are available
                    try:
                        WiFi_Data_List.append({
                            "bssid": parts[i].strip(),
                            "ssid": parts[i+1].strip(),
                            "rssi": float(parts[i+2].strip()),
                            "frequency": float(parts[i+3].strip()),
                            "capabilities": parts[i+4].strip()
                        })
                    except ValueError as e:
                        print(f"Skipping network entry at index {i} due to parsing error: {e}")
                        continue  # Skip this network entry if any value is invalid
            
            WiFi_Data_Dict["wifi_networks"] = WiFi_Data_List

            wifi_data.append(WiFi_Data_Dict)
    
    return wifi_data

##########################################################################################################################################
# Function: To read """_GPS.txt file contents
##########################################################################################################################################
def parse_gps_file(gps_file_path, max_interval=10000000):
    """
    Parses a GPS sensor file and extracts data in chunks based on a time interval.
    
    Args:
        gps_file_path (str): Path to the GPS sensor file.
        max_interval (int): Maximum interval (in milliseconds) for each chunk.
    
    Yields:
        dict: A dictionary representing a chunk of GPS data.
    """
    gps_data = []
    start_time = None

    with open(gps_file_path, 'r') as file:
        lines = file.readlines()
        
        for line in lines:
            parts = line.strip().split(' ')
            try:
                timestamp = int(parts[0])  # First element is timestamp
                
                # Initialize start_time if it is None
                if start_time is None:
                    start_time = timestamp

                # Check if the interval exceeds the max_interval; if so, yield the current chunk
                if timestamp - start_time >= max_interval:
                    yield gps_data
                    gps_data = []
                    start_time = timestamp

                # Parse satellite information
                satellite_info = []
                if len(parts) > 3:
                    for i in range(3, len(parts) - 1, 4):
                        satellite_info.append({
                            "id": parts[i],
                            "snr": float(parts[i + 1]),
                            "azimuth": float(parts[i + 2]),
                            "elevation": float(parts[i + 3])
                        })

                # Append each GPS record with satellite information
                gps_data.append({
                    "timestamp": timestamp,
                    "satellite_info": satellite_info
                })

            except ValueError:
                print(f"Skipping malformed line: {line}")

    # Yield remaining data if there is any
    if gps_data:
        yield gps_data



##########################################################################################################################################
# Function: To read """_Cells.txt file contents
##########################################################################################################################################
def parse_cells_file(cells_file_path):
    """
    Parses a Cells sensor file and extracts the data according to the schema.
    
    Args:
        cells_file_path (str): Path to the Cells sensor file.
    
    Returns:
        list: List of dictionaries, each representing a Cells data reading.
    """
    cells_data = []
    
    # Read the Cells file line by line
    with open(cells_file_path, 'r') as file:
        lines = file.readlines()
        for line in lines:
            parts = line.strip().split(' ')
            
            # Check if the line contains at least 4 elements for timestamp and number_of_entries
            if len(parts) < 4:
                print(f"Skipping line due to insufficient data: {line.strip()}")
                continue
            
            timestamp = int(parts[0])  # First element is timestamp
            try:
                number_of_entries = int(parts[3])  # Number of entries is the 4th element
            except ValueError:
                print(f"Skipping line with invalid number_of_entries: {line.strip()}")
                continue

            entries = []
            index = 4  # Start reading the cell entries from the 5th element
            
            # Ensure we don't attempt to access parts that don't exist
            for _ in range(number_of_entries):
                if index >= len(parts):
                    print(f"Skipping incomplete entry for timestamp {timestamp}")
                    break  # Break if there are not enough parts to read the expected entry

                cell_type = parts[index]
                if cell_type == "LTE" and index + 8 < len(parts):  # Check if there are enough elements
                    entries.append({
                        "cell_bsonType": cell_type,
                        "signal_level": float(parts[index + 1]),
                        "signal_strength": float(parts[index + 2]),
                        "signal_level_1": float(parts[index + 3]),
                        "cell_id": parts[index + 4],
                        "mcc": float(parts[index + 5]),
                        "mnc": float(parts[index + 6]),
                        "physical_cell_id": float(parts[index + 7]),
                        "tracking_area_code": parts[index + 8] if index + 8 < len(parts) else None                        
                    })
                    index += 9 + 1  # Move to the next LTE entry
                elif cell_type == "GSM" and index + 7 < len(parts):  # Check if there are enough elements
                    entries.append({
                        "cell_bsonType": cell_type,
                        "signal_level": float(parts[index + 1]),
                        "signal_strength": float(parts[index + 2]),
                        "signal_level_1": float(parts[index + 3]),
                        "cell_id": parts[index + 4],
                        "lac": parts[index + 5],
                        "mcc": float(parts[index + 6]),
                        "mnc": float(parts[index + 7]) if index + 7 < len(parts) else None
                    })
                    index += 8 + 1 # Move to the next GSM entry
                elif cell_type == "WCDMA" and index + 9 < len(parts):  # Check if there are enough elements
                    entries.append({
                        "cell_bsonType": cell_type,
                        "is_registered": float(parts[index + 1]),
                        "cell_id": parts[index + 2],
                        "lac": parts[index + 3],
                        "mcc": float(parts[index + 4]),
                        "mnc": float(parts[index + 5]),
                        "psc": float(parts[index + 6]),
                        "asu_level": float(parts[index + 7]), 
                        "dbm": float(parts[index + 8]),
                        "level": float(parts[index + 9]) if index + 9 < len(parts) else None
                    })
                    index += 10 + 1  # Move to the next WCDMA entry
                else:
                    #print(f"Unknown or incomplete cell type entry at timestamp {timestamp}, skipping.")
                    break  # Skip incomplete or unknown cell types
            
            # Append each record with cell information if valid entries exist
            if entries:
                cells_data.append({
                    "timestamp": timestamp,
                    "double_of_entries": float(number_of_entries),
                    "entries": entries
                })
    
    return cells_data

    
##########################################################################################################################################
# Function: To read Label.txt file contents
##########################################################################################################################################
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
                        "coarse_label": float(parts[1]),
                        "fine_label": float(parts[2]),
                        "road_label": float(parts[3]),
                        "traffic_label": float(parts[4]),
                        "tunnels_label": float(parts[5]),
                        "social_label": float(parts[6]),
                        "food_label": float(parts[7])
                    }
                    label_data.append(label_entry)
    
    return label_data



    


















