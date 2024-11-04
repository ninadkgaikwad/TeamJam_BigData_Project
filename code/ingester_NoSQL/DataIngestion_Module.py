##########################################################################################################################################
##########################################################################################################################################
# Data Ingestion Module
##########################################################################################################################################
##########################################################################################################################################

##########################################################################################################################################
# Import Required Modules
##########################################################################################################################################

import os
from datetime import datetime
import pymongo
import time

##########################################################################################################################################
# Import Custom Modules
##########################################################################################################################################
from DataParser_Module import *

def populate_sensor_collection(db_name, collection_name, base_path, mongo_uri="mongodb://localhost:27017/", batch_size=100):
    """
    Populate a MongoDB collection with sensor data generated from the folder structure,
    process in batches if specified, and return the average time of insertion.

    Args:
        db_name (str): Name of the MongoDB database.
        collection_name (str): Name of the MongoDB collection to populate.
        base_path (str): Path where the user folders are located.
        mongo_uri (str): MongoDB URI to connect to the database.
        batch_size (int): Number of documents per batch insertion.

    Returns:
        float: Average time taken to insert each record in seconds.
    """
    from pymongo import MongoClient
    import time

    # Establish a connection to MongoDB
    client = MongoClient(mongo_uri)
    db = client[db_name]
    collection = db[collection_name]
    
    # Dynamically select the correct data creation function based on the collection name
    if collection_name == "ambientsensor":
        sensor_data_generator = create_ambient_sensor_json(base_path)
    elif collection_name == "batterysensor":
        sensor_data_generator = create_battery_json(base_path)
    elif collection_name == "apisensor":
        sensor_data_generator = create_api_recording_json(base_path)
    elif collection_name == "locationsensor":
        sensor_data_generator = create_location_sensor_json(base_path)
    elif collection_name == "motionsensor":
        sensor_data_generator = create_motion_sensor_json(base_path)
    elif collection_name == "deprcellssensor":
        sensor_data_generator = create_deprcells_json(base_path)
    elif collection_name == "wifisensor":
        sensor_data_generator = create_wifi_json(base_path)
    elif collection_name == "recordingdata":
        sensor_data_generator = create_recordings_json_with_metadata(base_path)
    elif collection_name == "gpssensor":
        sensor_data_generator = create_gps_json(base_path)
    elif collection_name == "cellssensor":
        sensor_data_generator = create_cells_json(base_path)
    elif collection_name == "labelsensor":
        sensor_data_generator = create_label_sensor_json(base_path)
    else:
        print(f"Unknown collection: {collection_name}")
        return None
    
    # List to store insertion times
    insertion_times = []
    batch = []

    # Process the sensor data in batches if specified
    for record in sensor_data_generator:
        if batch_size > 1:
            batch.append(record)
            # Insert the batch when it reaches the specified batch size
            if len(batch) >= batch_size:
                start_time = time.time()
                try:
                    collection.insert_many(batch, ordered=False)
                    end_time = time.time()
                    insertion_times.append(end_time - start_time)
                    print(f"Inserted batch of {len(batch)} documents.")
                except Exception as e:
                    print(f"Error inserting batch: {e}")
                finally:
                    batch = []  # Clear batch for next set
        else:
            # Process each record individually if batch_size is 1
            try:
                start_time = time.time()
                collection.update_one({"_id": record["_id"]}, {"$set": record}, upsert=True)
                end_time = time.time()
                insertion_times.append(end_time - start_time)
                print(f"Data inserted/updated for: {record['_id']} (Time: {end_time - start_time:.4f} seconds)")
            except Exception as e:
                print(f"Error inserting data for {record['_id']}: {e}")

    # Insert any remaining records in the final batch
    if batch:
        start_time = time.time()
        try:
            collection.insert_many(batch, ordered=False)
            end_time = time.time()
            insertion_times.append(end_time - start_time)
            print(f"Inserted final batch of {len(batch)} documents.")
        except Exception as e:
            print(f"Error inserting final batch: {e}")

    # Calculate the average insertion time
    avg_insertion_time = sum(insertion_times) / len(insertion_times) if insertion_times else 0
    print(f"Average insertion time: {avg_insertion_time:.4f} seconds")

    # Close the MongoDB connection
    client.close()
    return avg_insertion_time



##########################################################################################################################################
# Function: To create Dictionary Document for User Collection based on developed JSON Schema
##########################################################################################################################################
def create_user_recordings_json(base_path):
    """
    Create a dictionary representing user recordings structured according to the provided JSON schema.
    
    Args:
        base_path (str): The path where the user folders are located. Inside each user folder are recording folders named with the format 'ddmmyy'.
        
    Returns:
        dict: A dictionary structured according to the given JSON schema, with _id as the user folder name, 
              recording_id as the recording folder name, and the date derived from the recording folder name.
              
    Example:
        base_path = "/path/to/data"
        result = create_user_recordings_json(base_path)
        
        # Example folder structure:
        # /path/to/data/User1/120617/
        # /path/to/data/User1/130617/
        # /path/to/data/User2/110617/
        
        # The function will return:
        # {
        #     "_id": "User1",
        #     "recordings": [
        #         {
        #             "recording_id": "120617",
        #             "date": "2017-06-12"
        #         },
        #         {
        #             "recording_id": "130617",
        #             "date": "2017-06-13"
        #         }
        #     ]
        # }
    """
    user_recordings = []
    
    # Iterate through user folders
    for user_folder in os.listdir(base_path):
        user_path = os.path.join(base_path, user_folder)
        
        if os.path.isdir(user_path):  # Ensure it is a directory (user folder)
            recordings = []
            
            # Iterate through each recording folder inside the user folder
            for recording_folder in os.listdir(user_path):
                recording_path = os.path.join(user_path, recording_folder)
                
                if os.path.isdir(recording_path):  # Ensure it is a directory (recording folder)
                    try:
                        # Convert the recording folder name (ddmmyy) to a date
                        date = datetime.strptime(recording_folder, '%d%m%y').strftime('%Y-%m-%d')
                        
                        # Append the recording data to the list
                        recordings.append({
                            "recording_id": user_folder+ "-" +recording_folder,
                            "date": date
                        })
                    except ValueError:
                        # If the folder name does not match the ddmmyy format, skip it
                        continue
            
            # Add the user recordings to the main list
            user_recordings.append({
                "_id": user_folder,
                "recordings": recordings
            })
    
    return user_recordings
    
##########################################################################################################################################
# Function: To create Dictionary Document for Recordings Collection based on developed JSON Schema
##########################################################################################################################################
def create_recordings_json_with_metadata(base_path):
    """
    Create a dictionary representing recordings structured according to the provided JSON schema.
    
    Args:
        base_path (str): The path where the user folders are located. Inside each user folder are recording folders named with the format 'ddmmyy'.
        
    Returns:
        list: A list of dictionaries structured according to the given JSON schema, with _id as userfoldername-recordingfoldername,
              user_id extracted from the folder name, and the remaining fields extracted from the 00inf.txt file.
              The sensors field is populated with sensor_type and sensor_data_id fields based on files in the recording folder.
              
    Example:
        base_path = "/path/to/data"
        result = create_recordings_json_with_metadata(base_path)
    """
    all_recordings = []
    
    # Iterate through user folders
    for user_folder in os.listdir(base_path):
        user_path = os.path.join(base_path, user_folder)
        
        if os.path.isdir(user_path):  # Ensure it is a directory (user folder)
            
            # Iterate through each recording folder inside the user folder
            for recording_folder in os.listdir(user_path):
                recording_path = os.path.join(user_path, recording_folder)
                
                if os.path.isdir(recording_path):  # Ensure it is a directory (recording folder)
                    try:
                        # Extract metadata from the 00inf.txt file
                        metadata = parse_recording_info_file(recording_path)
                        
                        # Construct the _id and user_id from the folder names
                        recording_id = f"{user_folder}-{recording_folder}"
                        user_id = user_folder
                        
                        # Convert the recording folder name (ddmmyy) to a date
                        date = datetime.strptime(recording_folder, '%d%m%y').strftime('%Y-%m-%d')
                        
                        # Initialize the list for sensors
                        sensors = []
                        
                        # Iterate over files in the recording folder to populate sensors
                        for sensor_file in os.listdir(recording_path):
                            # Check if the file starts with Bag, Hand, Hips, Torso, or Label and does not include 'labels'
                            if sensor_file.startswith(('Bag', 'Hand', 'Hips', 'Torso', 'Label')) and not sensor_file.startswith('labels'):
                                sensor_type = os.path.splitext(sensor_file)[0]  # Remove the extension
                                sensor_data_id = f"{user_folder}-{recording_folder}-{sensor_type}"
                                
                                # Add to the sensors list
                                sensors.append({
                                    "sensor_type": sensor_type,
                                    "sensor_data_id": sensor_data_id
                                })
                        
                        # Construct the final dictionary for this recording
                        recording_json = {
                            "_id": recording_id,
                            "user_id": user_id,
                            "date": date,
                            "start_time_ms": metadata.get('start_time_ms', None),
                            "end_time_ms": metadata.get('end_time_ms', None),
                            "recording_length_ms": metadata.get('recording_length_ms', None),
                            "recording_id": metadata.get('recording_id', recording_folder),
                            "sensors": sensors  # Sensors list added here
                        }
                        
                        # Append to the list of all recordings
                        all_recordings.append(recording_json)
                        
                    except ValueError:
                        # If the folder name does not match the ddmmyy format or 00inf.txt file has issues, skip it
                        continue
    
    return all_recordings

##########################################################################################################################################
# Function: To create Dictionary Document for Ambient Sensor Collection based on developed JSON Schema
##########################################################################################################################################
def create_ambient_sensor_json(base_path):
    """
    Create a dictionary representing ambient sensor data structured according to the provided JSON schema.
    
    Args:
        base_path (str): The path where the user folders are located. Inside each user folder are recording folders 
                         that contain ambient sensor files like Bag_Ambient.txt, Hand_Ambient.txt, etc.
        
    Returns:
        list: A list of dictionaries structured according to the given JSON schema for ambient sensors.
              
    Example:
        base_path = "/path/to/data"
        result = create_ambient_sensor_json(base_path)
    """
    all_ambient_sensors = []
    
    # Iterate through user folders
    for user_folder in os.listdir(base_path):
        user_path = os.path.join(base_path, user_folder)
        
        if os.path.isdir(user_path):  # Ensure it is a directory (user folder)
            
            # Iterate through each recording folder inside the user folder
            for recording_folder in os.listdir(user_path):
                recording_path = os.path.join(user_path, recording_folder)
                
                if os.path.isdir(recording_path):  # Ensure it is a directory (recording folder)
                    
                    # Iterate over files in the recording folder to check for ambient sensor files
                    for sensor_file in os.listdir(recording_path):
                        if sensor_file.startswith(('Bag_Ambient', 'Hand_Ambient', 'Hips_Ambient', 'Torso_Ambient')):
                            # Parse the ambient sensor file
                            sensor_file_path = os.path.join(recording_path, sensor_file)
                            ambient_data = parse_ambient_sensor_file(sensor_file_path)
                            
                            # Construct sensor type, sensor location, and sensor data ID
                            sensor_type = os.path.splitext(sensor_file)[0]  # Remove the extension
                            sensor_location = sensor_type.split('_')[0].lower()  # Get the location (bag, hand, etc.)
                            sensor_data_id = f"{user_folder}-{recording_folder}-{sensor_type}"
                            recording_id = f"{user_folder}-{recording_folder}"
                            
                            # Construct the JSON structure for this sensor
                            ambient_sensor_json = {
                                "_id": sensor_data_id,
                                "recording_id": recording_id,
                                "sensor_location": sensor_location,
                                "ambient_data": ambient_data
                            }
                            
                            # Append to the list of all ambient sensors
                            all_ambient_sensors.append(ambient_sensor_json)
    
    return all_ambient_sensors

##########################################################################################################################################
# Function: To create Dictionary Document for Battery Sensor Collection based on developed JSON Schema
##########################################################################################################################################
def create_battery_json(base_path):
    """
    Create a dictionary representing the battery sensor data for all users and recordings.
    
    Args:
        base_path (str): The base path where the user folders are located.
        
    Returns:
        list: List of dictionaries structured according to the provided JSON schema for battery sensor data.
    """
    all_battery_data = []

    # Iterate through user folders
    for user_folder in os.listdir(base_path):
        user_path = os.path.join(base_path, user_folder)
        
        if os.path.isdir(user_path):  # Ensure it is a directory (user folder)
            
            # Iterate through each recording folder inside the user folder
            for recording_folder in os.listdir(user_path):
                recording_path = os.path.join(user_path, recording_folder)
                
                if os.path.isdir(recording_path):  # Ensure it is a directory (recording folder)
                    
                    # Look for any battery sensor files (Bag_Battery, Hand_Battery, etc.)
                    for file_name in os.listdir(recording_path):
                        if 'Battery' in file_name and file_name.endswith('.txt'):
                            try:
                                # Parse battery sensor file
                                battery_file_path = os.path.join(recording_path, file_name)
                                battery_data = parse_battery_sensor_file(battery_file_path)
                                
                                # Construct the _id, recording_id, and sensor_location
                                recording_id = f"{user_folder}-{recording_folder}"
                                sensor_location = file_name.split('_')[0].lower()  # Extract sensor location from file name
                                
                                # Create the final JSON structure for this battery sensor
                                battery_json = {
                                    "_id": f"{user_folder}-{recording_folder}-{file_name.split('.')[0]}",
                                    "recording_id": recording_id,
                                    "sensor_location": sensor_location,
                                    "battery_data": battery_data
                                }
                                
                                # Append to the list of all battery data
                                all_battery_data.append(battery_json)
                            
                            except Exception as e:
                                print(f"Error processing {battery_file_path}: {e}")
                                continue
    
    return all_battery_data
 
##########################################################################################################################################
# Function: To create Dictionary Document for API Sensor Collection based on developed JSON Schema
##########################################################################################################################################
def create_api_recording_json(base_path):
    """
    Create a dictionary representing API sensor recordings structured according to the provided JSON schema.
    
    Args:
        base_path (str): The path where the user folders are located. Inside each user folder are recording folders named with the format 'ddmmyy'.
        
    Returns:
        list: A list of dictionaries structured according to the given JSON schema, with _id as userfoldername-recordingfoldername-APIfilename,
              recording_id as userfoldername-recordingfoldername, and the API data from the parser.
    """
    all_recordings = []
    
    # Iterate through user folders
    for user_folder in os.listdir(base_path):
        user_path = os.path.join(base_path, user_folder)
        
        if os.path.isdir(user_path):  # Ensure it is a directory (user folder)
            
            # Iterate through each recording folder inside the user folder
            for recording_folder in os.listdir(user_path):
                recording_path = os.path.join(user_path, recording_folder)
                
                if os.path.isdir(recording_path):  # Ensure it is a directory (recording folder)
                    
                    # Search for API files in the recording folder
                    for sensor_file in os.listdir(recording_path):
                        if sensor_file.endswith("_API.txt"):  # Only process API sensor files
                            sensor_location = sensor_file.split("_")[0].lower()  # Extract sensor location (bag, hand, hips, torso)
                            sensor_file_path = os.path.join(recording_path, sensor_file)
                            
                            # Parse the API sensor data
                            api_data = parse_api_file(sensor_file_path)
                            
                            # Create the recording ID and _id
                            recording_id = f"{user_folder}-{recording_folder}"
                            api_id = f"{recording_id}-{sensor_file}"
                            
                            # Construct the final dictionary for this API sensor
                            api_recording_json = {
                                "_id": api_id,
                                "recording_id": recording_id,
                                "sensor_location": sensor_location,
                                "api_confidence": api_data
                            }
                            
                            # Append to the list of all API sensor recordings
                            all_recordings.append(api_recording_json)
    
    return all_recordings
    
##########################################################################################################################################
# Function: To create Dictionary Document for Location Sensor Collection based on developed JSON Schema
##########################################################################################################################################
def create_location_sensor_json(base_path):
    """
    Create a dictionary representing location sensor data structured according to the provided JSON schema.
    
    Args:
        base_path (str): The path where the user folders are located. Inside each user folder are recording folders named with the format 'ddmmyy'.
        
    Returns:
        list: A list of dictionaries structured according to the JSON schema.
    """
    all_location_sensors = []
    
    # Iterate through user folders
    for user_folder in os.listdir(base_path):
        user_path = os.path.join(base_path, user_folder)
        
        if os.path.isdir(user_path):  # Ensure it is a directory (user folder)
            
            # Iterate through each recording folder inside the user folder
            for recording_folder in os.listdir(user_path):
                recording_path = os.path.join(user_path, recording_folder)
                
                if os.path.isdir(recording_path):  # Ensure it is a directory (recording folder)
                    # Look for the Location.txt file (replace with actual naming convention as needed)
                    for file_name in os.listdir(recording_path):
                        if file_name.endswith('_Location.txt'):  # Assuming the file follows this pattern
                            location_file_path = os.path.join(recording_path, file_name)
                            location_data = parse_location_file(location_file_path)
                            
                            # Construct the _id and recording_id from the folder names
                            recording_id = f"{user_folder}-{recording_folder}"
                            sensor_location = file_name.split('_')[0].lower()  # Assuming Bag_Location.txt -> 'bag'
                            
                            # Construct the final dictionary for this location sensor
                            location_sensor_json = {
                                "_id": f"{user_folder}-{recording_folder}-{file_name}",
                                "recording_id": recording_id,
                                "sensor_location": sensor_location,
                                "location_data": location_data
                            }
                            
                            # Append to the list of all location sensors
                            all_location_sensors.append(location_sensor_json)
    
    return all_location_sensors
    
##########################################################################################################################################
# Function: To create Dictionary Document for Motion Sensor Collection based on developed JSON Schema
##########################################################################################################################################
def create_motion_sensor_json(base_path, chunk_size=10000):
    """
    Creates a list of motion sensor documents, each containing a chunk of sensor data.
    
    Args:
        base_path (str): The path where the user folders are located.
    
    Yields:
        dict: Document structured according to the JSON schema.
    """
    all_motion_sensors = []
    
    for user_folder in os.listdir(base_path):
        user_path = os.path.join(base_path, user_folder)
        
        if os.path.isdir(user_path):
            for recording_folder in os.listdir(user_path):
                recording_path = os.path.join(user_path, recording_folder)
                
                if os.path.isdir(recording_path):
                    for file_name in os.listdir(recording_path):
                        if file_name.endswith('_Motion.txt'):
                            motion_file_path = os.path.join(recording_path, file_name)
                            motion_data = parse_motion_file(motion_file_path)
                            
                            recording_id = f"{user_folder}-{recording_folder}"
                            sensor_location = file_name.split('_')[0].lower()
                            
                            chunked_data = {
                                "_id": f"{recording_id}-{sensor_location}",
                                "recording_id": recording_id,
                                "sensor_location": sensor_location,
                                "acceleration": motion_data["acceleration"][:chunk_size],
                                "gyroscope": motion_data["gyroscope"][:chunk_size],
                                "magnetometer": motion_data["magnetometer"][:chunk_size],
                                "orientation": motion_data["orientation"][:chunk_size],
                                "gravity": motion_data["gravity"][:chunk_size],
                                "linear_acceleration": motion_data["linear_acceleration"][:chunk_size],
                                "pressure": motion_data["pressure"][:chunk_size],
                                "altitude": motion_data["altitude"][:chunk_size],
                                "temperature": motion_data["temperature"][:chunk_size]
                            }
                            
                            yield chunked_data



##########################################################################################################################################
# Function: To create Dictionary Document for DeprCells Sensor Collection based on developed JSON Schema
##########################################################################################################################################    
def create_deprcells_json(base_path):
    """
    Creates a JSON dictionary for DeprCells sensor data according to the given schema.
    
    Args:
        base_path (str): The base path where the user folders are located.

    Returns:
        list: List of dictionaries structured as per the DeprCells sensor schema.
    """
    all_deprcells = []
    
    # Iterate through user folders
    for user_folder in os.listdir(base_path):
        user_path = os.path.join(base_path, user_folder)
        
        if os.path.isdir(user_path):  # Ensure it is a directory (user folder)
            
            # Iterate through each recording folder inside the user folder
            for recording_folder in os.listdir(user_path):
                recording_path = os.path.join(user_path, recording_folder)
                
                if os.path.isdir(recording_path):  # Ensure it is a directory (recording folder)
                    
                    # Look for DeprCells files in the recording folder
                    for sensor_file in os.listdir(recording_path):
                        if 'DeprCells' in sensor_file:  # Identify DeprCells sensor file
                            deprcells_file_path = os.path.join(recording_path, sensor_file)
                            sensor_location = sensor_file.split('_')[0].lower()  # e.g., "Bag" -> "bag"
                            
                            # Parse the DeprCells sensor file
                            depr_cells_data = parse_deprcells_file(deprcells_file_path)
                            
                            # Construct the DeprCells JSON according to the schema
                            deprcells_json = {
                                "_id": f"{user_folder}-{recording_folder}-{sensor_file.replace('.txt', '')}",
                                "recording_id": f"{user_folder}-{recording_folder}",
                                "sensor_location": sensor_location,
                                "depr_cells_data": depr_cells_data
                            }
                            
                            # Append to the list of all deprcells data
                            all_deprcells.append(deprcells_json)
    
    return all_deprcells

##########################################################################################################################################
# Function: To create Dictionary Document for WiFi Sensor Collection based on developed JSON Schema
##########################################################################################################################################  
def create_wifi_json(base_path):
    """
    Creates JSON documents for Wifi sensor data according to the schema,
    splitting wifi_data across multiple documents if necessary.
    
    Args:
        base_path (str): The base path where the user folders are located.

    Returns:
        list: List of dictionaries structured as per the Wifi sensor schema.
    """
    all_wifi = []
    max_chunk_size = 90000  # Adjust this to a reasonable chunk size
    
    # Iterate through user folders
    for user_folder in os.listdir(base_path):
        user_path = os.path.join(base_path, user_folder)
        
        if os.path.isdir(user_path):  # Ensure it is a directory (user folder)
            
            # Iterate through each recording folder inside the user folder
            for recording_folder in os.listdir(user_path):
                recording_path = os.path.join(user_path, recording_folder)
                
                if os.path.isdir(recording_path):  # Ensure it is a directory (recording folder)
                    
                    # Look for Wifi files in the recording folder
                    for sensor_file in os.listdir(recording_path):
                        if 'WiFi' in sensor_file:  # Identify Wifi sensor file
                            wifi_file_path = os.path.join(recording_path, sensor_file)
                            sensor_location = sensor_file.split('_')[0].lower()  # e.g., "Bag" -> "bag"
                            
                            # Parse the WiFi sensor file
                            wifi_data = parse_wifi_file(wifi_file_path)
                            
                            # Split wifi_data into chunks
                            for i in range(0, len(wifi_data), max_chunk_size):
                                wifi_chunk = wifi_data[i:i + max_chunk_size]
                                
                                wifi_json = {
                                    "_id": f"{user_folder}-{recording_folder}-{sensor_file.replace('.txt', '')}-{i // max_chunk_size}",
                                    "recording_id": f"{user_folder}-{recording_folder}",
                                    "sensor_location": sensor_location,
                                    "wifi_data": wifi_chunk
                                }
                                
                                # Append to the list of all wifi data
                                all_wifi.append(wifi_json)
    
    return all_wifi


    
##########################################################################################################################################
# Function: To create Dictionary Document for GPS Sensor Collection based on developed JSON Schema
########################################################################################################################################## 
def create_gps_json(base_path):
    """
    Creates a JSON dictionary for GPS sensor data in chunks according to the schema.
    
    Args:
        base_path (str): The base path where the user folders are located.

    Yields:
        dict: Each chunk of GPS data as a separate document.
    """
    for user_folder in os.listdir(base_path):
        user_path = os.path.join(base_path, user_folder)
        
        if os.path.isdir(user_path):  # Ensure it is a directory (user folder)
            
            # Iterate through each recording folder inside the user folder
            for recording_folder in os.listdir(user_path):
                recording_path = os.path.join(user_path, recording_folder)
                
                if os.path.isdir(recording_path):  # Ensure it is a directory (recording folder)
                    
                    # Look for GPS files in the recording folder
                    for sensor_file in os.listdir(recording_path):
                        if 'GPS' in sensor_file:  # Identify GPS sensor file
                            gps_file_path = os.path.join(recording_path, sensor_file)
                            sensor_location = sensor_file.split('_')[0].lower()  # e.g., "Bag" -> "bag"
                            
                            # Parse the GPS sensor file and create chunks
                            for i, gps_data_chunk in enumerate(parse_gps_file(gps_file_path)):
                                gps_json = {
                                    "_id": f"{user_folder}-{recording_folder}-{sensor_file.replace('.txt', '')}-chunk-{i}",
                                    "recording_id": f"{user_folder}-{recording_folder}",
                                    "sensor_location": sensor_location,
                                    "gps_data": gps_data_chunk
                                }
                                yield gps_json

    
##########################################################################################################################################
# Function: To create Dictionary Document for Cells Sensor Collection based on developed JSON Schema
##########################################################################################################################################     
def create_cells_json(base_path):
    """
    Creates a JSON dictionary for Cells sensor data according to the given schema.
    
    Args:
        base_path (str): The base path where the user folders are located.

    Returns:
        list: List of dictionaries structured as per the Cells sensor schema.
    """
    all_cells = []
    
    # Iterate through user folders
    for user_folder in os.listdir(base_path):
        user_path = os.path.join(base_path, user_folder)
        
        if os.path.isdir(user_path):  # Ensure it is a directory (user folder)
            
            # Iterate through each recording folder inside the user folder
            for recording_folder in os.listdir(user_path):
                recording_path = os.path.join(user_path, recording_folder)
                
                if os.path.isdir(recording_path):  # Ensure it is a directory (recording folder)
                    
                    # Look for Cells files in the recording folder
                    for sensor_file in os.listdir(recording_path):
                        if 'Cells' in sensor_file:  # Identify Cells sensor file
                            cells_file_path = os.path.join(recording_path, sensor_file)
                            sensor_location = sensor_file.split('_')[0].lower()  # e.g., "Bag" -> "bag"
                            
                            # Parse the Cells sensor file
                            cells_data = parse_cells_file(cells_file_path)
                            
                            # Construct the Cells JSON according to the schema
                            cells_json = {
                                "_id": f"{user_folder}-{recording_folder}-{sensor_file.replace('.txt', '')}",
                                "recording_id": f"{user_folder}-{recording_folder}",
                                "sensor_location": sensor_location,
                                "cells_data": cells_data
                            }
                            
                            # Append to the list of all Cells data
                            all_cells.append(cells_json)
    
    return all_cells

##########################################################################################################################################
# Function: To create Dictionary Document for Labels Sensor Collection based on developed JSON Schema
########################################################################################################################################## 
def create_label_sensor_json(base_path, chunk_size=100000):
    """
    Creates a list of label sensor documents, each containing a chunk of label data.

    Args:
        base_path (str): The path where the user folders are located.
        chunk_size (int): Number of label entries per chunk.
    
    Yields:
        dict: Document structured according to the JSON schema.
    """
    
    for user_folder in os.listdir(base_path):
        user_path = os.path.join(base_path, user_folder)
        
        if os.path.isdir(user_path):  # Ensure it's a directory (user folder)
            
            # Iterate through each recording folder inside the user folder
            for recording_folder in os.listdir(user_path):
                recording_path = os.path.join(user_path, recording_folder)
                
                if os.path.isdir(recording_path):  # Ensure it's a directory (recording folder)
                    
                    # Look for the Labels.txt file
                    for label_file in os.listdir(recording_path):
                        if label_file.endswith("Label.txt"):  # Assuming the file ends with "Label.txt"
                            label_file_path = os.path.join(recording_path, label_file)
                            try:
                                # Parse the label data
                                label_data = parse_labels_file(label_file_path)
                                
                                # Construct the _id and recording_id
                                label_id_base = f"{user_folder}-{recording_folder}-{label_file.split('.')[0]}"
                                recording_id = f"{user_folder}-{recording_folder}"
                                
                                # Create chunks of label data and yield each as a separate document
                                for i in range(0, len(label_data), chunk_size):
                                    chunked_data = {
                                        "_id": f"{label_id_base}-chunk-{i // chunk_size}",
                                        "recording_id": recording_id,
                                        "label_data": label_data[i:i + chunk_size]
                                    }
                                    yield chunked_data
                                
                            except Exception as e:
                                print(f"Error processing file {label_file_path}: {e}")

##########################################################################################################################################
# Function: To create Dictionary Document for Labels Sensor Collection based on developed JSON Schema
##########################################################################################################################################    


def populate_collection_with_user_recordings(db_name, collection_name, base_path, mongo_uri="mongodb://localhost:27017/"):
    """
    Populate a MongoDB collection with user recordings data generated from the folder structure,
    record the time taken for each insertion, and return the average time of insertion.
    
    Args:
        db_name (str): Name of the MongoDB database.
        collection_name (str): Name of the MongoDB collection to populate.
        base_path (str): The path where the user folders are located. Inside each user folder are recording folders.
        mongo_uri (str): MongoDB URI to connect to the database (default is "mongodb://localhost:27017/").
        
    Returns:
        float: Average time taken to insert each record in seconds.
    
    Example:
        db_name = "mydatabase"
        collection_name = "userdata"
        base_path = "/path/to/data"
        
        populate_collection_with_user_recordings(db_name, collection_name, base_path)
    """
    
    # Establish a connection to MongoDB
    client = pymongo.MongoClient(mongo_uri)
    
    # Access the specified database and collection
    db = client[db_name]
    collection = db[collection_name]
    
    # Use the existing function to create the data
    user_recordings_data = create_user_recordings_json(base_path)
    
    # List to store insertion times
    insertion_times = []
    
    # Insert the user recordings data into the collection
    if user_recordings_data:
        for record in user_recordings_data:
            try:
                # Record the start time
                start_time = time.time()
                
                # Insert or update the record (based on _id)
                collection.update_one({"_id": record["_id"]}, {"$set": record}, upsert=True)
                
                # Record the end time
                end_time = time.time()
                
                # Calculate the time taken for this insertion and add to the list
                insertion_time = end_time - start_time
                insertion_times.append(insertion_time)
                
                print(f"Data inserted/updated for user: {record['_id']} (Time: {insertion_time:.4f} seconds)")
                
            except Exception as e:
                print(f"Error inserting data for {record['_id']}: {e}")
    
    # Calculate the average insertion time
    if insertion_times:
        avg_insertion_time = sum(insertion_times) / len(insertion_times)
        print(f"Average insertion time: {avg_insertion_time:.4f} seconds")
    else:
        avg_insertion_time = 0
        print("No data was inserted.")
    
    # Close the MongoDB connection
    client.close()
    
    return avg_insertion_time
