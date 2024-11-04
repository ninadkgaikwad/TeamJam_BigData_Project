##########################################################################################################################################
##########################################################################################################################################
# Data Parser Module
##########################################################################################################################################
##########################################################################################################################################

##########################################################################################################################################
# Import Required Modules
##########################################################################################################################################

import os
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
                        "battery_level": int(parts[3]),  # Battery level
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
                "still_confidence": int(parts[3]),  # Still confidence (0-100%)
                "on_foot_confidence": int(parts[4]),  # On foot confidence (0-100%)
                "walking_confidence": int(parts[5]),  # Walking confidence (0-100%)
                "running_confidence": int(parts[6]),  # Running confidence (0-100%)
                "on_bicycle_confidence": int(parts[7]),  # On bicycle confidence (0-100%)
                "in_vehicle_confidence": int(parts[8]),  # In vehicle confidence (0-100%)
                "tilting_confidence": int(parts[9]),  # Tilting confidence (0-100%)
                "unknown_confidence": int(parts[10])  # Unknown confidence (0-100%)
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
def parse_motion_file(motion_file_path):
    """
    Parse the Motion.txt file to extract motion sensor data as chunks.
    
    Args:
        motion_file_path (str): Path to the motion sensor data file.
    
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
        "magnetometer": [],
        "orientation": [],
        "gravity": [],
        "linear_acceleration": [],
        "pressure": [],
        "altitude": [],
        "temperature": []
    }

    with open(motion_file_path, 'r') as file:
        for line in file:
            values = line.strip().split()

            # Ensure the line has the expected number of values
            if len(values) == 23:
                timestamp = int(float(values[0]) * 100)

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
    depr_cells_data = []

    # Read the DeprCells file line by line
    with open(deprcells_file_path, 'r') as file:
        lines = file.readlines()
        for line in lines:
            parts = line.strip().split()  # Split by whitespace
            if len(parts) >= 9:
                depr_cells_data.append({
                    "timestamp": int(parts[0]),  # Time in ms
                    "network_type": parts[3],    # Network type
                    "cid": parts[4],             # Cell ID
                    "lac": parts[5],             # Location Area Code
                    "dbm": float(parts[6]),      # Signal strength in dBm
                    "mcc": int(parts[7]),        # Mobile Country Code
                    "mns": int(parts[8])         # Mobile Network Code
                })
    
    return depr_cells_data

##########################################################################################################################################
# Function: To read """_WiFi.txt file contents
##########################################################################################################################################
def parse_wifi_file(wifi_file_path):
    """
    Parses a Wifi sensor file and extracts the data according to the schema.
    
    Args:
        wifi_file_path (str): Path to the Wifi sensor file.
    
    Returns:
        list: List of dictionaries, each representing a sensor data reading.
    """
    wifi_data = []
    
    # Read the WiFi file line by line
    with open(wifi_file_path, 'r') as file:
        lines = file.readlines()
        for line_number, line in enumerate(lines):
            parts = line.strip().split(';')  # Split by semicolon for Wifi fields
            
            # Log the parts to verify the structure
            print(f"Line {line_number}: {parts}")
            
            try:
                timestamp = int(parts[0])  # First element is timestamp
            except ValueError as e:
                print(f"Error parsing timestamp on line {line_number}: {e}")
                continue  # Skip this line if timestamp is invalid

            wifi_networks = []
            
            # Parse each WiFi network's details starting from the 4th column
            for i in range(3, len(parts), 5):  # BSSID, SSID, RSSI, Frequency, Capabilities
                if i + 4 < len(parts):
                    # Check if parts[i+2] and parts[i+3] are numeric before converting
                    try:
                        rssi = float(parts[i+2].strip())
                        frequency = float(parts[i+3].strip())
                    except ValueError as e:
                        print(f"Error parsing RSSI or Frequency on line {line_number} at index {i}: {e}")
                        continue  # Skip this network entry if it contains invalid data
                    
                    wifi_networks.append({
                        "bssid": parts[i].strip(),
                        "ssid": parts[i+1].strip(),
                        "rssi": rssi,
                        "frequency": frequency,
                        "capabilities": parts[i+4].strip()
                    })
                    
            # Append each WiFi record with its corresponding networks
            wifi_data.append({
                "timestamp": timestamp,
                "wifi_networks": wifi_networks
            })
    
    return wifi_data



##########################################################################################################################################
# Function: To read """_GPS.txt file contents
##########################################################################################################################################
def parse_gps_file(gps_file_path):
    """
    Parses a GPS sensor file and extracts the data according to the schema.
    
    Args:
        gps_file_path (str): Path to the GPS sensor file.
    
    Returns:
        list: List of dictionaries, each representing a GPS data reading.
    """
    gps_data = []
    
    # Read the GPS file line by line
    with open(gps_file_path, 'r') as file:
        lines = file.readlines()
        for line in lines:
            parts = line.strip().split(' ')
            timestamp = int(parts[0])  # First element is timestamp
            
            satellite_info = []
            
            # Parse satellite information
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
    
    return gps_data


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
            timestamp = int(parts[0])  # First element is timestamp
            number_of_entries = int(parts[3])  # Number of entries is the 4th element
            
            entries = []
            
            index = 4  # Start reading the cell entries from the 5th element
            for _ in range(number_of_entries):
                cell_type = parts[index]
                if cell_type == "LTE":
                    entries.append({
                        "cell_type": cell_type,
                        "signal_level": float(parts[index + 1]),
                        "signal_strength": float(parts[index + 2]),
                        "signal_level_1": float(parts[index + 3]),
                        "cell_id": parts[index + 4],
                        "mcc": int(parts[index + 5]),
                        "mnc": int(parts[index + 6]),
                        "physical_cell_id": float(parts[index + 7]),
                        "tracking_area_code": parts[index + 8] if index + 8 < len(parts) else None                        
                    })
                    index += 9  # Move to the next LTE entry
                elif cell_type == "GSM":
                    entries.append({
                        "cell_type": cell_type,
                        "signal_level": float(parts[index + 1]),
                        "signal_strength": float(parts[index + 2]),
                        "signal_level_1": float(parts[index + 3]),
                        "cell_id": parts[index + 4],
                        "lac": parts[index + 5],
                        "mcc": int(parts[index + 6]),
                        "mnc": int(parts[index + 7]) if index + 7 < len(parts) else None
                    })
                    index += 8  # Move to the next GSM entry
                elif cell_type == "WCDMA":
                    entries.append({
                        "cell_type": cell_type,
                        "is_registered": float(parts[index + 1]),
                        "cell_id": parts[index + 2],
                        "lac": parts[index + 3],
                        "mcc": int(parts[index + 4]),
                        "mnc": int(parts[index + 5]),
                        "psc": int(parts[index + 6]),
                        "asu_level": float(parts[index + 7]), 
                        "dbm": float(parts[index + 8]),
                        "level": float(parts[index + 9]) if index + 9 < len(parts) else None
                    })
                    index += 10  # Move to the next WCDMA entry
            
            # Append each record with cell information
            cells_data.append({
                "timestamp": timestamp,
                "number_of_entries": number_of_entries,
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
                        "coarse_label": int(parts[1]),
                        "fine_label": int(parts[2]),
                        "road_label": int(parts[3]),
                        "traffic_label": int(parts[4]),
                        "tunnels_label": int(parts[5]),
                        "social_label": int(parts[6]),
                        "food_label": int(parts[7])
                    }
                    label_data.append(label_entry)
    
    return label_data



    


















