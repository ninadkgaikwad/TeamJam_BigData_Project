from pymongo import MongoClient
import pandas as pd
import time

def find_sensor_data_by_recording_id(recording_id):
    # MongoDB connection
    uri = "mongodb://localhost:27017/"  # Replace with your actual MongoDB URI
    client = MongoClient(uri)
    database = client.get_database("SensorDatabase")  # Adjust the database name if different
    collection = database.get_collection("batterysensor")  # Adjust the collection name

    # Query to match recording_id
    query = {
        "recording_id": recording_id  # Match the specific recording_id
    }

    # Execute the query and exclude the _id field
    results = collection.find(query, {"_id": 0})

    # Convert results to a list of dictionaries
    results_list = list(results)

    # Create a pandas DataFrame from the list of documents
    df = pd.DataFrame(results_list)

    # Save the DataFrame to a CSV file
    df.to_csv(f'{recording_id}_sensor_data.csv', index=False)

    return df


def find_sensor_data_by_recording_id_and_timestamp(recording_id, start_timestamp, end_timestamp):
    # MongoDB connection
    uri = "mongodb://localhost:27017/"  # Replace with your actual MongoDB URI
    client = MongoClient(uri)
    database = client.get_database("SensorDatabase")  # Adjust the database name if different
    collection = database.get_collection("batterysensor")  # Adjust the collection name

    # Query to match recording_id and the timestamp range within the battery_data array
    query = {
        "recording_id": recording_id,  # Match the recording_id
        "battery_data": {
            "$elemMatch": {
                "timestamp.$numberLong": {
                    "$gte": str(start_timestamp),  # Greater than or equal to start_timestamp
                    "$lte": str(end_timestamp)  # Less than or equal to end_timestamp
                }
            }
        }
    }

    # Execute the query and exclude the _id field
    results = collection.find(query, {"_id": 0})

    # Convert results to a list of dictionaries
    results_list = list(results)

    # Create a pandas DataFrame from the list of documents
    df = pd.DataFrame(results_list)

    # Save the DataFrame to a CSV file
    df.to_csv(f'{recording_id}_sensor_data_{start_timestamp}_to_{end_timestamp}.csv', index=False)

    return df


def find_sensor_data_by_recording_id_and_location(recording_id, sensor_location):
    # MongoDB connection
    uri = "mongodb://localhost:27017/"  # Replace with your actual MongoDB URI
    client = MongoClient(uri)
    database = client.get_database("SensorDatabase")  # Adjust the database name if different
    collection = database.get_collection("batterysensor")  # Adjust the collection name

    # Query to match recording_id and sensor_location
    query = {
        "recording_id": recording_id,  # Match the recording_id
        "sensor_location": sensor_location  # Match the sensor_location (e.g., "hips")
    }

    # Execute the query and exclude the _id field
    results = collection.find(query, {"_id": 0})

    # Convert results to a list of dictionaries
    results_list = list(results)

    # Create a pandas DataFrame from the list of documents
    df = pd.DataFrame(results_list)

    # Save the DataFrame to a CSV file
    df.to_csv(f'{recording_id}_sensor_data_{sensor_location}.csv', index=False)

    return df


# Example usage

recording_id = "User2-140617"  # Example recording_id based on your document
start_timestamp = 1497427158140  # Start of the range
end_timestamp = 1497427899496  # End of the range
sensor_location = "hips"  # Specify "hips" as the sensor location

# Timing and executing each function
start_time = time.time()
df_by_recording_id = find_sensor_data_by_recording_id(recording_id)
end_time = time.time()
print(f"Execution time for find_sensor_data_by_recording_id: {end_time - start_time:.4f} seconds")

start_time = time.time()
df_by_timestamp = find_sensor_data_by_recording_id_and_timestamp(recording_id, start_timestamp, end_timestamp)
end_time = time.time()
print(f"Execution time for find_sensor_data_by_recording_id_and_timestamp: {end_time - start_time:.4f} seconds")

start_time = time.time()
df_by_location = find_sensor_data_by_recording_id_and_location(recording_id, sensor_location)
end_time = time.time()
print(f"Execution time for find_sensor_data_by_recording_id_and_location: {end_time - start_time:.4f} seconds")
