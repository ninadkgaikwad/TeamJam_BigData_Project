from pymongo import MongoClient
import pandas as pd
import matplotlib.pyplot as plt


def get_sensor_statistics_generic(collection_name, recording_id=None, sensor_location=None, 
                                  start_timestamp=None, end_timestamp=None, fields=None, 
                                  nested_field="acceleration", output_csv=None):
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
    # Establish a connection to the MongoDB server running on localhost at port 27017.
    # If your MongoDB server is on a different host or port, update the URI accordingly.
    uri = "mongodb://localhost:27017/"
    client = MongoClient(uri)
    
    # Access the specified database and collection
    database = client.get_database("SensorDatabase")
    collection = database.get_collection(collection_name)

    # Step 2: Initialize the aggregation pipeline
    # MongoDB's aggregation pipeline allows us to perform complex data transformations and calculations.
    # We start by initializing an empty list that will hold each stage of the pipeline.
    pipeline = []

    # Step 3: Build match conditions
    # We create a dictionary to store conditions for filtering the data.
    # This dictionary is populated only if the corresponding parameters (recording_id, sensor_location, 
    # start_timestamp, end_timestamp) are provided.
    match_conditions = {}
    if recording_id:
        # Filter documents by recording_id if it is provided.
        match_conditions["recording_id"] = recording_id
    if sensor_location:
        # Filter documents by sensor_location if it is provided.
        match_conditions["sensor_location"] = sensor_location
    if start_timestamp and end_timestamp:
        # If both start and end timestamps are provided, add a timestamp range filter.
        # This filters the documents where the timestamp in the nested field falls within the range.
        match_conditions[f"{nested_field}.timestamp"] = {"$gte": start_timestamp, "$lte": end_timestamp}
    
    # If we have any filter conditions, add a `$match` stage to the pipeline to filter documents accordingly.
    if match_conditions:
        pipeline.append({"$match": match_conditions})

    # Step 4: Unwind the nested array
    # `$unwind` is a MongoDB aggregation stage that "flattens" arrays.
    # Here, it will deconstruct the specified nested array (e.g., "acceleration") so each element in 
    # the array becomes a separate document in the pipeline. This allows us to perform calculations 
    # on individual array elements rather than on the entire array.
    pipeline.append({"$unwind": f"${nested_field}"})

    # Step 5: Build the group stage for aggregations
    # This stage will calculate statistics for the specified fields. 
    # `_id: None` means that we want to aggregate all matching documents as a single group.
    # We also initialize a `frequency` count to count the number of items in the group (like a row count).
    group_stage = {
        "$group": {
            "_id": None,  # Group all matching documents together
            "frequency": {"$sum": 1}  # Count the number of documents to get the frequency
        }
    }
    
    # Step 6: Add calculations for each specified field
    # For each field in the `fields` list (e.g., ["x", "y", "z"]), we add calculations to the `$group` stage.
    # These include minimum (`$min`), maximum (`$max`), average (`$avg`), and standard deviation (`$stdDevPop`).
    for field in fields:
        # Construct the full path to the field within the nested array (e.g., "acceleration.x")
        field_path = f"{nested_field}.{field}"
        
        # Add each calculation to the group stage using the fully qualified path
        group_stage["$group"][f"min_{field}"] = {"$min": f"${field_path}"}
        group_stage["$group"][f"max_{field}"] = {"$max": f"${field_path}"}
        group_stage["$group"][f"avg_{field}"] = {"$avg": f"${field_path}"}
        group_stage["$group"][f"std_dev_{field}"] = {"$stdDevPop": f"${field_path}"}

    # Append the `$group` stage to the pipeline once all fields have been added.
    pipeline.append(group_stage)

    # Step 7: Execute the aggregation pipeline
    # Run the pipeline on the collection to get the aggregated results.
    # `collection.aggregate(pipeline)` executes the pipeline, and `list()` retrieves the output as a list.
    results = list(collection.aggregate(pipeline))

    # Step 8: Convert the results to a pandas DataFrame
    # If the results contain data, create a DataFrame from it for easy viewing and further processing.
    if results:
        df = pd.DataFrame(results)
        
        # Rename columns for clarity. For each field, set column names like "Min X", "Max X", etc.
        rename_columns = {
            "frequency": "Frequency",
        }
        for field in fields:
            # Update the column names to make them more readable in the DataFrame.
            rename_columns.update({
                f"min_{field}": f"Min {field.capitalize()}",
                f"max_{field}": f"Max {field.capitalize()}",
                f"avg_{field}": f"Average {field.capitalize()}",
                f"std_dev_{field}": f"{field.capitalize()} Std Dev"
            })
        
        # Apply the new column names to the DataFrame
        df.rename(columns=rename_columns, inplace=True)
    else:
        # If there were no results, create an empty DataFrame with the expected column names
        column_names = ["Frequency"]
        for field in fields:
            column_names.extend([
                f"Min {field.capitalize()}", f"Max {field.capitalize()}",
                f"Average {field.capitalize()}", f"{field.capitalize()} Std Dev"
            ])
        df = pd.DataFrame(columns=column_names)

    # Step 9: Write to CSV if specified
    # If an `output_csv` path is provided, save the DataFrame to a CSV file at that location.
    if output_csv:
        df.to_csv(output_csv, index=False)
        print(f"Results saved to {output_csv}")

    # Return the DataFrame containing the statistics
    return df

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