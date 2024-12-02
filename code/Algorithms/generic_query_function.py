from pymongo import MongoClient
import pandas as pd
import matplotlib.pyplot as plt
import time

def get_sensor_statistics_generic(collection_name, recording_id=None, sensor_location=None, 
                                  start_timestamp=None, end_timestamp=None, fields=None, 
                                  nested_field="acceleration", output_csv="GenericQuery.csv"):
    """
    Query MongoDB for basic sensor statistics based on flexible criteria and specified fields.

    Parameters:
    - collection_name (str): The name of the MongoDB collection.
    - recording_id (str, optional): The recording ID to filter data.
    - sensor_location (str, optional): The location of the sensor (e.g., "bag").
    - start_timestamp (int, optional): The starting timestamp of the query range.
    - end_timestamp (int, optional): The ending timestamp of the query range.
    - fields (list, optional): A list of field names within the nested array to calculate statistics for.
    - nested_field (str): The name of the nested field array (e.g., "ambient_data" or "api_confidence").
    - output_csv (str, optional): Path to save the results to a CSV file.

    Returns:
    - DataFrame: A pandas DataFrame containing the statistics for each specified field.
    """

    # Step 1: Connect to MongoDB
    uri = "mongodb://localhost:27017/"
    client = MongoClient(uri)
    database = client.get_database("SensorDatabase")
    collection = database.get_collection(collection_name)

    # Step 2: Create compound index for optimized querying
    collection.create_index([("recording_id", 1), (f"{nested_field}.timestamp", 1)])

    # Measure the start time for query execution
    start_time = time.time()

    # Step 3: Initialize the aggregation pipeline
    pipeline = []

    # Step 4: Build match conditions
    match_conditions = {}
    if recording_id:
        match_conditions["recording_id"] = recording_id
    if sensor_location:
        match_conditions["sensor_location"] = sensor_location
    if start_timestamp and end_timestamp:
        match_conditions[f"{nested_field}.timestamp"] = {"$gte": start_timestamp, "$lte": end_timestamp}
    
    if match_conditions:
        pipeline.append({"$match": match_conditions})

    # Step 5: Unwind the nested array
    pipeline.append({"$unwind": f"${nested_field}"})

    # Step 6: Build the group stage for aggregations
    group_stage = {
        "$group": {
            "_id": None,
            "frequency": {"$sum": 1}
        }
    }

    # Step 8: Execute the aggregation pipeline
    results = list(collection.aggregate(pipeline))
    execution_time = time.time() - start_time
    print(f"Query Execution Time: {execution_time:.4f} seconds")

    # Step 9: Convert the results to a pandas DataFrame
    if results:
        df = pd.DataFrame(results)

        # Add query execution time row
        query_time_row = pd.DataFrame({"Frequency": ["Query Execution Time (seconds)"],
                                       "Min X": [execution_time]})
        df = pd.concat([df, query_time_row], ignore_index=True)

    # Step 10: Write to CSV if specified
    if output_csv:
        df.to_csv(output_csv, index=False)
        print(f"Results saved to {output_csv}")

    return df

# Define parameters for calling the function
collection_name = "batterysensor"
recording_id = "User1-220617"
sensor_location = "bag"
start_timestamp = 1498120321440
end_timestamp = 1498154531245
fields = ["battery_level", "temperature"]
nested_field = "battery_data"
output_csv = "battery_sensor_statistics.csv"

# Call the function to get sensor statistics
df_stats = get_sensor_statistics_generic(
    collection_name=collection_name,
    recording_id=recording_id,
    sensor_location=sensor_location,
    start_timestamp=start_timestamp,
    end_timestamp=end_timestamp,
    fields=fields,
    nested_field=nested_field,
    output_csv=output_csv
)

# Display the DataFrame containing the statistics
print("DataFrame: ")
print(df_stats)
