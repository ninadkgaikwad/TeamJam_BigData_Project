from pymongo import MongoClient
import pandas as pd
import matplotlib.pyplot as plt
import time

def query_sensor_data(collection_name, recording_id=None, sensor_location=None, 
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
    uri = "mongodb://localhost:27017/"
    client = MongoClient(uri)
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

# Define parameters for calling the functions
collection_name = "batterysensor"
recording_id = "User1-220617"
sensor_location = "bag"
start_timestamp = 1498137071183
end_timestamp = 1498138095009

#2017-06-22 22:51:11.183
#End Datetime (UTC): 2017-06-22 23:08:15.009
fields = ["battery_level", "temperature"]  # Assuming these are top-level fields
nested_field = "battery_data"  # Indicating no nested field
output_csv = "battery_sensor_statistics.csv"

# Step 1: Query the sensor data
df_raw = query_sensor_data(
    collection_name=collection_name,
    recording_id=recording_id,
    sensor_location=sensor_location,
    start_timestamp=start_timestamp,
    end_timestamp=end_timestamp,
    nested_field=nested_field
)

# Step 2: Calculate the sensor statistics
df_stats = calculate_sensor_statistics(
    df=df_raw,
    fields=fields,
    nested_field=nested_field,
    output_csv=output_csv
)

# Display the statistics DataFrame
print("Sensor Statistics DataFrame:")
print(df_stats)

################### ADDITIONAL NOTES ###############################

# Needs to generate a valid timestamp per sub sensor (like in motionsensor)
# Maybe have function that takes collection input name and generates min and max timestamp range for user
    # to choose from. So timestamp would be one of last things user selects to build the query
    # user select process like this: *select user*-->*select recording day*-->
        #*select sensor location (bag, hand, etc)*->*select sensor type*-->
        #*select subfield of sensor (if applicable). Like x,y,z from acceleration sensor within motion sensor.*

# Can only query for specified subfield if they are present
    #i.e.: motionsensor->acceleration-->x,y,z values from acceleration (nested fields inside subfields)
    #or: apisensor-->apiconfidence (only one subfield, no nested fields)

# Other thing to think about is potential optimizations with compound indexes (timestamp, recordingid)
    # and with using resources like Apache Spark/Map Reduce potentially.

# Idea for ML is have user select timerange and user id to predict activity for given time range
# Model uses motionsensor data and labels data to do it's prediction, relying on sensor stats
    #such as max, min, sd, freq, and using frequency spectrum analysis to capture periodicity and patterns in sensor data
    # then use activity levels to train the model (training using LSTM network or CNN)

# Optimizations potentially with Spark when we implement the ML portion
