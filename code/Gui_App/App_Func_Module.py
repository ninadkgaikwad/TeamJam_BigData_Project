#########################################################################################################
# Importing Desired Modules
#########################################################################################################

from pymongo import MongoClient
import pandas as pd
import matplotlib.pyplot as plt
import time

#########################################################################################################
# Custom Functions
#########################################################################################################

# Query: Sensor Data
def query_sensor_data(URI, collection_name, recording_id=None, sensor_location=None, 
                      start_timestamp=None, end_timestamp=None, nested_field=None):
    """
    Query MongoDB for sensor data based on flexible criteria and return a pandas DataFrame.

    Parameters:
    - collection_name (str): The name of the MongoDB collection.
    - recording_id (str, optional): The recording ID to filter data.
    - sensor_location (str, optional): The location of the sensor (e.g., "bag").
    - start_timestamp (int, optional): The starting timestamp of the query range.
    - end_timestamp (int, optional): The ending timestamp of the query range.
    - nested_field (str, optional): The name of the nested field array. If None or empty, no unwinding is done.

    Returns:
    - DataFrame: A pandas DataFrame containing the raw query results.
    """
    # Step 1: Connect to MongoDB
    client = MongoClient(URI)
    database = client.get_database("SensorDatabase")
    collection = database.get_collection(collection_name)

    # Step 2: Create compound index for optimized querying
    if nested_field:
        collection.create_index([("recording_id", 1), (f"{nested_field}.timestamp", 1)])
    else:
        collection.create_index([("recording_id", 1)])

    # Step 3: Initialize the aggregation pipeline
    pipeline = []

    # Step 4: Build match conditions
    match_conditions = {}
    if recording_id:
        match_conditions["recording_id"] = recording_id
    if sensor_location:
        match_conditions["sensor_location"] = sensor_location
    if start_timestamp and end_timestamp:
        timestamp_field = f"{nested_field}.timestamp" if nested_field else "timestamp"
        match_conditions[timestamp_field] = {"$gte": start_timestamp, "$lte": end_timestamp}
    
    if match_conditions:
        pipeline.append({"$match": match_conditions})

    # Step 5: Unwind the nested array if nested_field is provided
    if nested_field:
        pipeline.append({"$unwind": f"${nested_field}"})

    # Step 6: Execute the aggregation pipeline
    results = list(collection.aggregate(pipeline))

    # Convert the results to a pandas DataFrame
    if results:
        df = pd.DataFrame(results)
    else:
        df = pd.DataFrame()

    return df

# Algorithm: Statistics
def calculate_sensor_statistics(df, fields, nested_field=None, output_csv=None):
    """
    Calculate statistics for sensor data fields, print the transposed DataFrame, 
    and save the transposed version with labeled rows to the CSV.

    Parameters:
    - df (DataFrame): The raw data DataFrame returned by the query function.
    - fields (list): A list of field names to calculate statistics for.
    - nested_field (str, optional): The name of the nested field array. If None, fields are expected at the top level.
    - output_csv (str, optional): Path to save the results to a CSV file.

    Returns:
    - DataFrame: A transposed pandas DataFrame containing the calculated statistics.
    """
    if df.empty:
        print("No data available for statistics calculation.")
        return pd.DataFrame()

    # Measure the start time for statistics calculation
    start_time = time.time()

    # Extract relevant fields for statistics
    stats = {}
    for field in fields:
        field_path = f"{nested_field}.{field}" if nested_field else field
        field_values = df.get(field_path, pd.Series(dtype='float'))

        if field_values.empty:
            continue

        stats[f"Min {field.capitalize()}"] = field_values.min()
        stats[f"Max {field.capitalize()}"] = field_values.max()
        stats[f"Average {field.capitalize()}"] = field_values.mean()
        stats[f"{field.capitalize()} Std Dev"] = field_values.std()

    # Convert statistics to a DataFrame
    stats_df = pd.DataFrame([stats], index=["Sensor Statistics"])
    execution_time = time.time() - start_time

    # Add execution time row
    execution_time_row = pd.DataFrame(
        {"Query Execution Time (seconds)": [execution_time]},
        index=["Execution Time"]
    )

    # Combine statistics and execution time into one DataFrame
    combined_df = pd.concat([stats_df, execution_time_row], axis=0)

    # Transpose the DataFrame
    transposed_df = combined_df.transpose()

    # Print the transposed DataFrame
    print("\nCalculated Sensor Statistics (Transposed):")
    print(transposed_df)
    print(f"\nExecution Time: {execution_time:.4f} seconds")

    # Write to CSV if specified
    if output_csv:
        transposed_df.to_csv(output_csv)
        print(f"\nResults saved to {output_csv}")

    return transposed_df

