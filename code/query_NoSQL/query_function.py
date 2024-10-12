from pymongo import MongoClient
import json
import pandas as pd
import  time



# # Example usage:
# user_id = "12345"  # Replace with the actual user_id

# # Call the function and retrieve the data as a dictionary
# sensor_data_dict = find_sensor_data_by_user_id(user_id)

def find_sensor_data_by_id(_id):
    # MongoDB connection
    uri = "placeholder"  
    client = MongoClient(uri)
    database = client.get_database("AA")  # Adjust the database name if different
    collection = database.get_collection("ambientsensor")  # Adjust the collection name

    """
    Query MongoDB for sensor data based on _id.
    
    Args:
        _id (str): The user ID to filter by.
        
    Returns:
        dict: A dictionary where each key is the recording_id, and each value is the full document.
    """
    # Query to match user_id only
    query = {
        "_id": _id  # Match the user_id
    }
    
    # Execute the query
    results = collection.find(query)

    # Initialize a dictionary to store results
    result_dict = {}

    # Process each document
    for document in results:
        # Use recording_id as the key and the full document as the value
        result_dict[document["recording_id"]] = document

    return result_dict


# # Example usage:
# _id = "12345"  # Replace with the actual _id you are searching for
# recording_id = "abc123"  # Replace with the actual recording_id

# # Call the function and retrieve results as a dictionary
# sensor_data_dict = find_sensor_data_by_recording_id(user_id, recording_id)

def find_sensor_data_by_recording_id(_id, recording_id):
    # MongoDB connection
    uri = "placeholder"  
    client = MongoClient(uri)
    database = client.get_database("AA")  # Adjust the database name if different
    collection = database.get_collection("ambientsensor")  # Adjust the collection name

    query = {
        "_id": _id,  # Match the user_id from metadata in the MongoDB documents
        "recording_id": recording_id  # Search for the specific recording_id
    }

    # Execute the query and fetch matching documents
    results = collection.find(query)

    # Convert results into a dictionary, using recording_id as the key
    result_dict = {}
    for document in results:
        result_dict[document["recording_id"]] = document  # Use recording_id as the key
    
    return result_dict


# # Example usage:
# user_id = "12345"  # Replace with the actual user_id
# recording_id = "abc123"  # Replace with the actual recording_id
# start_timestamp = 1600000000  # Replace with the start of the timestamp range
# end_timestamp = 1609999999  # Replace with the end of the timestamp range

# # Call the function and retrieve the filtered data as a dictionary
# sensor_data_dict = find_sensor_data_by_recording_id_and_timestamp(user_id, recording_id, start_timestamp, end_timestamp)

def find_sensor_data_by_recording_id_and_timestamp(_id, recording_id, start_timestamp, end_timestamp):

    # MongoDB connection
    uri = "placeholder"  
    client = MongoClient(uri)
    database = client.get_database("AA")  # Adjust the database name if different
    collection = database.get_collection("ambientsensor")  # Adjust the collection name

    """
    Query MongoDB for sensor data based on _id, recording_id, and a timestamp range.
    
    Args:
        _id (str): The user ID to filter by.
        recording_id (str): The recording ID to filter by.
        start_timestamp (int): The start of the timestamp range (in milliseconds).
        end_timestamp (int): The end of the timestamp range (in milliseconds).
        
    Returns:
        dict: A dictionary where each key is the recording_id, and each value is the filtered document
              with ambient_data that matches the timestamp range.
    """
    # Query to match user_id, recording_id, and the timestamp range within the ambient_data array
    query = {
        "_id": _id,  # Match the _id
        "recording_id": recording_id,  # Match the recording_id
        "ambient_data": {
            "$elemMatch": {
                "timestamp": {
                    "$gte": start_timestamp,  # Greater than or equal to start_timestamp
                    "$lte": end_timestamp  # Less than or equal to end_timestamp
                }
            }
        }
    }
    
    # Execute the query
    results = collection.find(query)

    # Initialize a dictionary to store results
    result_dict = {}

    # Process each document
    for document in results:
        # Filter only the ambient_data entries that match the timestamp range
        filtered_ambient_data = [
            entry for entry in document["ambient_data"]
            if start_timestamp <= entry["timestamp"] <= end_timestamp
        ]

        # Add the filtered ambient_data back into the document
        document["ambient_data"] = filtered_ambient_data

        # Use recording_id as the key and the full document as the value
        result_dict[document["recording_id"]] = document

    return result_dict



# Measure execution time for each function

# Example user ID and recording details
user_id = "12345"  # Replace with the actual _id
recording_id = "abc123"  # Replace with the actual recording_id
start_timestamp = 1600000000  # Replace with the start of the timestamp range
end_timestamp = 1609999999  # Replace with the end of the timestamp range

# Timing the find_sensor_data_by_id function
start_time = time.time()  # Start time measurement
sensor_data_by_id = find_sensor_data_by_id(user_id)
end_time = time.time()  # End time measurement
print(f"Execution time for find_sensor_data_by_id: {end_time - start_time:.4f} seconds")

# Timing the find_sensor_data_by_recording_id function
start_time = time.time()  # Start time measurement
sensor_data_by_recording_id = find_sensor_data_by_recording_id(user_id, recording_id)
end_time = time.time()  # End time measurement
print(f"Execution time for find_sensor_data_by_recording_id: {end_time - start_time:.4f} seconds")

# Timing the find_sensor_data_by_recording_id_and_timestamp function
start_time = time.time()  # Start time measurement
sensor_data_by_recording_id_and_timestamp = find_sensor_data_by_recording_id_and_timestamp(user_id, recording_id, start_timestamp, end_timestamp)
end_time = time.time()  # End time measurement
print(f"Execution time for find_sensor_data_by_recording_id_and_timestamp: {end_time - start_time:.4f} seconds")