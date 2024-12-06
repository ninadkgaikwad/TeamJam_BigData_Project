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



#########################################################################################################
# Main Script
#########################################################################################################

mongo_uri = "mongodb://localhost:27017/"  # Update MongoDB URI if needed

db_name = "SensorDatabase"


# List of available collection names
""" collections = [
    "userdata",
    "recordingdata",
    "ambientsensor",
    "batterysensor",
    "apisensor",
    "locationsensor",
    "motionsensor",
    "deprcellssensor",
    "wifisensor",
    "gpssensor",
    "cellssensor",
    "labelsensor"
] """

collections = [
    "userdata",
    "recordingdata"
]


from Query_Module import *

from pymongo import MongoClient
import pandas as pd

def query_collection(
    URI,
    collection_name,
    field=None,
    recording_id=None,
    sensor_location=None,
    start_timestamp=None,
    end_timestamp=None
):
    """
    Query MongoDB for different collections based on the collection_name and return a pandas DataFrame.

    Parameters:
    - URI (str): MongoDB connection URI.
    - collection_name (str): The name of the collection to query.
    - field (str, optional): The specific field for certain collections (e.g., MotionSensor).
    - recording_id (str, optional): The recording ID to filter data.
    - sensor_location (str, optional): The location of the sensor (e.g., "bag").
    - start_timestamp (int, optional): The starting timestamp of the query range.
    - end_timestamp (int, optional): The ending timestamp of the query range.

    Returns:
    - DataFrame: A pandas DataFrame containing the extracted data or an error message for unsupported collections.
    """

    # Check for unsupported collections
    if collection_name in ["userdata", "recordingdata"]:
        return pd.DataFrame({"error": ["no time series data available in this table"]})

    # Routing based on collection_name
    if collection_name == "ambientsensor":
        return query_AmbientSensor_data(
            URI=URI,
            recording_id=recording_id,
            sensor_location=sensor_location,
            start_timestamp=start_timestamp,
            end_timestamp=end_timestamp
        )

    elif collection_name == "apisensor":
        return query_APISensor_data(
            URI=URI,
            recording_id=recording_id,
            sensor_location=sensor_location,
            start_timestamp=start_timestamp,
            end_timestamp=end_timestamp
        )

    elif collection_name == "batterysensor":
        return query_BatterySensor_data(
            URI=URI,
            recording_id=recording_id,
            sensor_location=sensor_location,
            start_timestamp=start_timestamp,
            end_timestamp=end_timestamp
        )

    elif collection_name == "deprcellssensor":
        return query_DeprCellsSensor_data(
            URI=URI,
            recording_id=recording_id,
            sensor_location=sensor_location,
            start_timestamp=start_timestamp,
            end_timestamp=end_timestamp
        )

    elif collection_name == "locationsensor":
        return query_LocationSensor_data(
            URI=URI,
            recording_id=recording_id,
            sensor_location=sensor_location,
            start_timestamp=start_timestamp,
            end_timestamp=end_timestamp
        )

    elif collection_name == "wifisensor":
        return query_WifiSensor_data(
            URI=URI,
            recording_id=recording_id,
            sensor_location=sensor_location,
            start_timestamp=start_timestamp,
            end_timestamp=end_timestamp
        )

    elif collection_name == "gpssensor":
        return query_GPSSensor_data(
            URI=URI,
            recording_id=recording_id,
            sensor_location=sensor_location,
            start_timestamp=start_timestamp,
            end_timestamp=end_timestamp
        )

    elif collection_name == "cellssensor":
        return query_CellsSensor_data(
            URI=URI,
            recording_id=recording_id,
            sensor_location=sensor_location,
            start_timestamp=start_timestamp,
            end_timestamp=end_timestamp
        )

    elif collection_name == "labelsensor":
        return query_LabelSensor_data(
            URI=URI,
            recording_id=recording_id,
            start_timestamp=start_timestamp,
            end_timestamp=end_timestamp
        )

    elif collection_name == "motionsensor":
        if not field:
            raise ValueError("Field parameter is required for MotionSensor collection.")
        return query_MotionSensor_data(
            URI=URI,
            field=field,
            recording_id=recording_id,
            sensor_location=sensor_location,
            start_timestamp=start_timestamp,
            end_timestamp=end_timestamp
        )

    else:
        # Default case for unsupported collections
        return pd.DataFrame({"error": [f"Unsupported collection: {collection_name}"]})



import pandas as pd
import numpy as np

def query_statistics(input_df):
    """
    Compute statistics (max, min, mean, standard deviation) for numeric columns in the input DataFrame.

    Parameters:
    - input_df (DataFrame): The input DataFrame with time series data.

    Returns:
    - DataFrame: A DataFrame containing statistics for numeric columns.
    """
    # Initialize a dictionary to store results
    stats_dict = {
        "max": {},
        "min": {},
        "mean": {},
        "std_dev": {}
    }

    # Iterate over each column in the DataFrame
    for column in input_df.columns:
        if column not in ["timestamp"]:  # Exclude timestamp column
            if pd.api.types.is_numeric_dtype(input_df[column]):
                # Compute statistics for numeric columns
                stats_dict["max"][column] = input_df[column].max()
                stats_dict["min"][column] = input_df[column].min()
                stats_dict["mean"][column] = input_df[column].mean()
                stats_dict["std_dev"][column] = input_df[column].std()
            else:
                # Non-numeric columns get NaN
                stats_dict["max"][column] = np.nan
                stats_dict["min"][column] = np.nan
                stats_dict["mean"][column] = np.nan
                stats_dict["std_dev"][column] = np.nan

    # Create a DataFrame from the stats dictionary
    stats_df = pd.DataFrame(stats_dict)
    stats_df.index.name = "column"

    return stats_df

# Example Usage
URI = "mongodb://localhost:27017/"
collection_name = "apisensor"
field = None
recording_id = "User1-220617"
sensor_location = "bag"
start_timestamp = 1498120405160
end_timestamp = 1498120484250

result_df = query_collection(
    URI=URI,
    collection_name=collection_name,
    field=field,
    recording_id=recording_id,
    sensor_location=sensor_location,
    start_timestamp=start_timestamp,
    end_timestamp=end_timestamp
)

print(result_df)

print(query_statistics(result_df))












