#########################################################################################################
# Importing Desired Modules
#########################################################################################################

from pymongo import MongoClient
import pandas as pd

#########################################################################################################
# Master Function
#########################################################################################################
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


#########################################################################################################
# Custom Functions
#########################################################################################################

## AmbientSensor
def query_AmbientSensor_data(
    URI,
    recording_id=None,
    sensor_location=None,
    start_timestamp=None,
    end_timestamp=None
):
    """
    Query MongoDB for AmbientSensor data and return a pandas DataFrame with timestamp, lumix, and temperature.

    Parameters:
    - URI (str): MongoDB connection URI.
    - recording_id (str, optional): The recording ID to filter data.
    - sensor_location (str, optional): The location of the sensor (e.g., "bag").
    - start_timestamp (int, optional): The starting timestamp of the query range.
    - end_timestamp (int, optional): The ending timestamp of the query range.

    Returns:
    - DataFrame: A pandas DataFrame containing the extracted data.
    """
    # Connect to MongoDB
    client = MongoClient(URI)
    database = client["SensorDatabase"]
    collection = database["ambientsensor"]

    # Build the query conditions
    match_conditions = {}
    if recording_id:
        match_conditions["recording_id"] = recording_id
    if sensor_location:
        match_conditions["sensor_location"] = sensor_location
    if start_timestamp or end_timestamp:
        match_conditions["ambient_data"] = {
            "$elemMatch": {
                "timestamp": {}
            }
        }
        if start_timestamp:
            match_conditions["ambient_data"]["$elemMatch"]["timestamp"]["$gte"] = start_timestamp
        if end_timestamp:
            match_conditions["ambient_data"]["$elemMatch"]["timestamp"]["$lte"] = end_timestamp

    # Aggregation pipeline
    pipeline = [
        {"$match": match_conditions},
        {"$unwind": "$ambient_data"},
        {
            "$match": {
                "ambient_data.timestamp": {
                    "$gte": start_timestamp if start_timestamp else 0,
                    "$lte": end_timestamp if end_timestamp else float("inf")
                }
            }
        },
        {
            "$project": {
                "_id": 0,
                "timestamp": "$ambient_data.timestamp",
                "lumix": "$ambient_data.lumix",
                "temperature": "$ambient_data.temperature"
            }
        }
    ]

    # Execute the query
    results = list(collection.aggregate(pipeline))

    # Convert results to a pandas DataFrame
    if results:
        df = pd.DataFrame(results)
    else:
        df = pd.DataFrame(columns=["timestamp", "lumix", "temperature"])

    # Close the client connection
    client.close()

    return df

## APISensor
def query_APISensor_data(
    URI,
    recording_id=None,
    sensor_location=None,
    start_timestamp=None,
    end_timestamp=None
):
    """
    Query MongoDB for APISensor data and return a pandas DataFrame with the fields:
    timestamp, still_confidence, on_foot_confidence, walking_confidence, running_confidence,
    on_bicycle_confidence, in_vehicle_confidence, tilting_confidence, unknown_confidence.

    Parameters:
    - URI (str): MongoDB connection URI.
    - recording_id (str, optional): The recording ID to filter data.
    - sensor_location (str, optional): The location of the sensor (e.g., "bag").
    - start_timestamp (int, optional): The starting timestamp of the query range.
    - end_timestamp (int, optional): The ending timestamp of the query range.

    Returns:
    - DataFrame: A pandas DataFrame containing the extracted data.
    """
    # Connect to MongoDB
    client = MongoClient(URI)
    database = client["SensorDatabase"]
    collection = database["apisensor"]

    # Build the query conditions
    match_conditions = {}
    if recording_id:
        match_conditions["recording_id"] = recording_id
    if sensor_location:
        match_conditions["sensor_location"] = sensor_location
    if start_timestamp or end_timestamp:
        match_conditions["api_confidence"] = {
            "$elemMatch": {
                "timestamp": {}
            }
        }
        if start_timestamp:
            match_conditions["api_confidence"]["$elemMatch"]["timestamp"]["$gte"] = start_timestamp
        if end_timestamp:
            match_conditions["api_confidence"]["$elemMatch"]["timestamp"]["$lte"] = end_timestamp

    # Aggregation pipeline
    pipeline = [
        {"$match": match_conditions},
        {"$unwind": "$api_confidence"},
        {
            "$match": {
                "api_confidence.timestamp": {
                    "$gte": start_timestamp if start_timestamp else 0,
                    "$lte": end_timestamp if end_timestamp else float("inf")
                }
            }
        },
        {
            "$project": {
                "_id": 0,
                "timestamp": "$api_confidence.timestamp",
                "still_confidence": "$api_confidence.still_confidence",
                "on_foot_confidence": "$api_confidence.on_foot_confidence",
                "walking_confidence": "$api_confidence.walking_confidence",
                "running_confidence": "$api_confidence.running_confidence",
                "on_bicycle_confidence": "$api_confidence.on_bicycle_confidence",
                "in_vehicle_confidence": "$api_confidence.in_vehicle_confidence",
                "tilting_confidence": "$api_confidence.tilting_confidence",
                "unknown_confidence": "$api_confidence.unknown_confidence"
            }
        }
    ]

    # Execute the query
    results = list(collection.aggregate(pipeline))

    # Convert results to a pandas DataFrame
    if results:
        df = pd.DataFrame(results)
    else:
        df = pd.DataFrame(
            columns=[
                "timestamp",
                "still_confidence",
                "on_foot_confidence",
                "walking_confidence",
                "running_confidence",
                "on_bicycle_confidence",
                "in_vehicle_confidence",
                "tilting_confidence",
                "unknown_confidence"
            ]
        )

    # Close the client connection
    client.close()

    return df

## BatterySensor
def query_BatterySensor_data(
    URI,
    recording_id=None,
    sensor_location=None,
    start_timestamp=None,
    end_timestamp=None
):
    """
    Query MongoDB for BatterySensor data and return a pandas DataFrame with the fields:
    timestamp, battery_level, and temperature.

    Parameters:
    - URI (str): MongoDB connection URI.
    - recording_id (str, optional): The recording ID to filter data.
    - sensor_location (str, optional): The location of the sensor (e.g., "bag").
    - start_timestamp (int, optional): The starting timestamp of the query range.
    - end_timestamp (int, optional): The ending timestamp of the query range.

    Returns:
    - DataFrame: A pandas DataFrame containing the extracted data.
    """
    # Connect to MongoDB
    client = MongoClient(URI)
    database = client["SensorDatabase"]
    collection = database["batterysensor"]

    # Build the query conditions
    match_conditions = {}
    if recording_id:
        match_conditions["recording_id"] = recording_id
    if sensor_location:
        match_conditions["sensor_location"] = sensor_location
    if start_timestamp or end_timestamp:
        match_conditions["battery_data"] = {
            "$elemMatch": {
                "timestamp": {}
            }
        }
        if start_timestamp:
            match_conditions["battery_data"]["$elemMatch"]["timestamp"]["$gte"] = start_timestamp
        if end_timestamp:
            match_conditions["battery_data"]["$elemMatch"]["timestamp"]["$lte"] = end_timestamp

    # Aggregation pipeline
    pipeline = [
        {"$match": match_conditions},
        {"$unwind": "$battery_data"},
        {
            "$match": {
                "battery_data.timestamp": {
                    "$gte": start_timestamp if start_timestamp else 0,
                    "$lte": end_timestamp if end_timestamp else float("inf")
                }
            }
        },
        {
            "$project": {
                "_id": 0,
                "timestamp": "$battery_data.timestamp",
                "battery_level": "$battery_data.battery_level",
                "temperature": "$battery_data.temperature"
            }
        }
    ]

    # Execute the query
    results = list(collection.aggregate(pipeline))

    # Convert results to a pandas DataFrame
    if results:
        df = pd.DataFrame(results)
    else:
        df = pd.DataFrame(columns=["timestamp", "battery_level", "temperature"])

    # Close the client connection
    client.close()

    return df

## DeprCellsSensor
def query_DeprCellsSensor_data(
    URI,
    recording_id=None,
    sensor_location=None,
    start_timestamp=None,
    end_timestamp=None
):
    """
    Query MongoDB for DeprCellsSensor data and return a pandas DataFrame with the fields:
    timestamp, network_bsonType, cid, lac, dbm, mcc, and mns.

    Parameters:
    - URI (str): MongoDB connection URI.
    - recording_id (str, optional): The recording ID to filter data.
    - sensor_location (str, optional): The location of the sensor (e.g., "bag").
    - start_timestamp (int, optional): The starting timestamp of the query range.
    - end_timestamp (int, optional): The ending timestamp of the query range.

    Returns:
    - DataFrame: A pandas DataFrame containing the extracted data.
    """
    # Connect to MongoDB
    client = MongoClient(URI)
    database = client["SensorDatabase"]
    collection = database["deprcellssensor"]

    # Build the query conditions
    match_conditions = {}
    if recording_id:
        match_conditions["recording_id"] = recording_id
    if sensor_location:
        match_conditions["sensor_location"] = sensor_location
    if start_timestamp or end_timestamp:
        match_conditions["depr_cells_data"] = {
            "$elemMatch": {
                "timestamp": {}
            }
        }
        if start_timestamp:
            match_conditions["depr_cells_data"]["$elemMatch"]["timestamp"]["$gte"] = start_timestamp
        if end_timestamp:
            match_conditions["depr_cells_data"]["$elemMatch"]["timestamp"]["$lte"] = end_timestamp

    # Aggregation pipeline
    pipeline = [
        {"$match": match_conditions},
        {"$unwind": "$depr_cells_data"},
        {
            "$match": {
                "depr_cells_data.timestamp": {
                    "$gte": start_timestamp if start_timestamp else 0,
                    "$lte": end_timestamp if end_timestamp else float("inf")
                }
            }
        },
        {
            "$project": {
                "_id": 0,
                "timestamp": "$depr_cells_data.timestamp",
                "network_bsonType": "$depr_cells_data.network_bsonType",
                "cid": "$depr_cells_data.cid",
                "lac": "$depr_cells_data.lac",
                "dbm": "$depr_cells_data.dbm",
                "mcc": "$depr_cells_data.mcc",
                "mns": "$depr_cells_data.mns"
            }
        }
    ]

    # Execute the query
    results = list(collection.aggregate(pipeline))

    # Convert results to a pandas DataFrame
    if results:
        df = pd.DataFrame(results)
    else:
        df = pd.DataFrame(columns=["timestamp", "network_bsonType", "cid", "lac", "dbm", "mcc", "mns"])

    # Close the client connection
    client.close()

    return df

## LocationSensor
def query_LocationSensor_data(
    URI,
    recording_id=None,
    sensor_location=None,
    start_timestamp=None,
    end_timestamp=None
):
    """
    Query MongoDB for LocationSensor data and return a pandas DataFrame with the fields:
    timestamp, accuracy, latitude, longitude, and altitude.

    Parameters:
    - URI (str): MongoDB connection URI.
    - recording_id (str, optional): The recording ID to filter data.
    - sensor_location (str, optional): The location of the sensor (e.g., "bag").
    - start_timestamp (int, optional): The starting timestamp of the query range.
    - end_timestamp (int, optional): The ending timestamp of the query range.

    Returns:
    - DataFrame: A pandas DataFrame containing the extracted data.
    """
    # Connect to MongoDB
    client = MongoClient(URI)
    database = client["SensorDatabase"]
    collection = database["locationsensor"]

    # Build the query conditions
    match_conditions = {}
    if recording_id:
        match_conditions["recording_id"] = recording_id
    if sensor_location:
        match_conditions["sensor_location"] = sensor_location
    if start_timestamp or end_timestamp:
        match_conditions["location_data"] = {
            "$elemMatch": {
                "timestamp": {}
            }
        }
        if start_timestamp:
            match_conditions["location_data"]["$elemMatch"]["timestamp"]["$gte"] = start_timestamp
        if end_timestamp:
            match_conditions["location_data"]["$elemMatch"]["timestamp"]["$lte"] = end_timestamp

    # Aggregation pipeline
    pipeline = [
        {"$match": match_conditions},
        {"$unwind": "$location_data"},
        {
            "$match": {
                "location_data.timestamp": {
                    "$gte": start_timestamp if start_timestamp else 0,
                    "$lte": end_timestamp if end_timestamp else float("inf")
                }
            }
        },
        {
            "$project": {
                "_id": 0,
                "timestamp": "$location_data.timestamp",
                "accuracy": "$location_data.accuracy",
                "latitude": "$location_data.latitude",
                "longitude": "$location_data.longitude",
                "altitude": "$location_data.altitude"
            }
        }
    ]

    # Execute the query
    results = list(collection.aggregate(pipeline))

    # Convert results to a pandas DataFrame
    if results:
        df = pd.DataFrame(results)
    else:
        df = pd.DataFrame(columns=["timestamp", "accuracy", "latitude", "longitude", "altitude"])

    # Close the client connection
    client.close()

    return df

## WifiSensor
def query_WifiSensor_data(
    URI,
    recording_id=None,
    sensor_location=None,
    start_timestamp=None,
    end_timestamp=None
):
    """
    Query MongoDB for WifiSensor data and return a pandas DataFrame with the fields:
    timestamp, bssid, ssid, rssi, frequency, and capabilities.

    Parameters:
    - URI (str): MongoDB connection URI.
    - recording_id (str, optional): The recording ID to filter data.
    - sensor_location (str, optional): The location of the sensor (e.g., "bag").
    - start_timestamp (int, optional): The starting timestamp of the query range.
    - end_timestamp (int, optional): The ending timestamp of the query range.

    Returns:
    - DataFrame: A pandas DataFrame containing the extracted data.
    """
    # Connect to MongoDB
    client = MongoClient(URI)
    database = client["SensorDatabase"]
    collection = database["wifisensor"]

    # Build the query conditions
    match_conditions = {}
    if recording_id:
        match_conditions["recording_id"] = recording_id
    if sensor_location:
        match_conditions["sensor_location"] = sensor_location

    # Aggregation pipeline
    pipeline = [
        {"$match": match_conditions},
        {"$unwind": "$wifi_data"},
        {
            "$match": {
                "$expr": {
                    "$and": [
                        {"$gte": ["$wifi_data.timestamp", start_timestamp if start_timestamp else 0]},
                        {"$lte": ["$wifi_data.timestamp", end_timestamp if end_timestamp else float("inf")]}]
                }
            }
        },
        {"$unwind": "$wifi_data.wifi_networks"},
        {
            "$project": {
                "_id": 0,
                "timestamp": "$wifi_data.timestamp",
                "bssid": "$wifi_data.wifi_networks.bssid",
                "ssid": "$wifi_data.wifi_networks.ssid",
                "rssi": "$wifi_data.wifi_networks.rssi",
                "frequency": "$wifi_data.wifi_networks.frequency",
                "capabilities": "$wifi_data.wifi_networks.capabilities"
            }
        }
    ]

    # Execute the query
    results = list(collection.aggregate(pipeline))

    # Convert results to a pandas DataFrame
    if results:
        df = pd.DataFrame(results)
    else:
        df = pd.DataFrame(columns=["timestamp", "bssid", "ssid", "rssi", "frequency", "capabilities"])

    # Close the client connection
    client.close()

    return df

## GPSSensor
def query_GPSSensor_data(
    URI,
    recording_id=None,
    sensor_location=None,
    start_timestamp=None,
    end_timestamp=None
):
    """
    Query MongoDB for GPSSensor data and return a pandas DataFrame with the fields:
    timestamp, id, snr, azimuth, and elevation.

    Parameters:
    - URI (str): MongoDB connection URI.
    - recording_id (str, optional): The recording ID to filter data.
    - sensor_location (str, optional): The location of the sensor (e.g., "bag").
    - start_timestamp (int, optional): The starting timestamp of the query range.
    - end_timestamp (int, optional): The ending timestamp of the query range.

    Returns:
    - DataFrame: A pandas DataFrame containing the extracted data.
    """
    # Connect to MongoDB
    client = MongoClient(URI)
    database = client["SensorDatabase"]
    collection = database["gpssensor"]

    # Build the query conditions
    match_conditions = {}
    if recording_id:
        match_conditions["recording_id"] = recording_id
    if sensor_location:
        match_conditions["sensor_location"] = sensor_location

    # Aggregation pipeline
    pipeline = [
        {"$match": match_conditions},
        {"$unwind": "$gps_data"},
        {
            "$match": {
                "$expr": {
                    "$and": [
                        {"$gte": ["$gps_data.timestamp", start_timestamp if start_timestamp else 0]},
                        {"$lte": ["$gps_data.timestamp", end_timestamp if end_timestamp else float("inf")]}]
                }
            }
        },
        {"$unwind": "$gps_data.satellite_info"},
        {
            "$project": {
                "_id": 0,
                "timestamp": "$gps_data.timestamp",
                "id": "$gps_data.satellite_info.id",
                "snr": "$gps_data.satellite_info.snr",
                "azimuth": "$gps_data.satellite_info.azimuth",
                "elevation": "$gps_data.satellite_info.elevation"
            }
        }
    ]

    # Execute the query
    results = list(collection.aggregate(pipeline))

    # Convert results to a pandas DataFrame
    if results:
        df = pd.DataFrame(results)
    else:
        df = pd.DataFrame(columns=["timestamp", "id", "snr", "azimuth", "elevation"])

    # Close the client connection
    client.close()

    return df

## CellsSensor
def query_CellsSensor_data(
    URI,
    recording_id=None,
    sensor_location=None,
    start_timestamp=None,
    end_timestamp=None
):
    """
    Query MongoDB for CellsSensor data and return three pandas DataFrames for LTE, GSM, and WCDMA data.

    Parameters:
    - URI (str): MongoDB connection URI.
    - recording_id (str, optional): The recording ID to filter data.
    - sensor_location (str, optional): The location of the sensor (e.g., "bag").
    - start_timestamp (int, optional): The starting timestamp of the query range.
    - end_timestamp (int, optional): The ending timestamp of the query range.

    Returns:
    - tuple: Three pandas DataFrames (lte_df, gsm_df, wcdma_df).
    """
    from pymongo import MongoClient
    import pandas as pd

    # Connect to MongoDB
    client = MongoClient(URI)
    database = client["SensorDatabase"]
    collection = database["cellssensor"]

    # Build base conditions
    match_conditions = {}
    if recording_id:
        match_conditions["recording_id"] = recording_id
    if sensor_location:
        match_conditions["sensor_location"] = sensor_location

    # Add timestamp filtering if specified
    if start_timestamp is not None or end_timestamp is not None:
        timestamp_filter = {}
        if start_timestamp:
            timestamp_filter["$gte"] = start_timestamp
        if end_timestamp:
            timestamp_filter["$lte"] = end_timestamp
        match_conditions["cells_data.timestamp"] = timestamp_filter

    # Base pipeline
    base_pipeline = [
        {"$match": match_conditions},
        {"$unwind": "$cells_data"},
        {"$match": {"cells_data.timestamp": match_conditions.get("cells_data.timestamp", {})}},
        {"$unwind": "$cells_data.entries"}
    ]

    # Helper to generate pipeline for each type
    def generate_pipeline(cell_type, fields):
        return base_pipeline + [
        {"$match": {"cells_data.entries.cell_bsonType": cell_type}},
        {
            "$project": {
                **fields,
                "_id": 0,
                "timestamp": "$cells_data.timestamp"  # Add timestamp explicitly
            }
        }
    ]

    # Define projections for each cell type
    lte_fields = {
        "timestamp": "$cells_data.timestamp",
        "cell_bsonType": "$cells_data.entries.cell_bsonType",
        "signal_level": "$cells_data.entries.signal_level",
        "signal_strength": "$cells_data.entries.signal_strength",
        "signal_level_1": "$cells_data.entries.signal_level_1",
        "cell_id": "$cells_data.entries.cell_id",
        "mcc": "$cells_data.entries.mcc",
        "mnc": "$cells_data.entries.mnc",
        "physical_cell_id": "$cells_data.entries.physical_cell_id",
        "tracking_area_code": "$cells_data.entries.tracking_area_code",
        "double_of_entries": "$cells_data.double_of_entries"
    }
    gsm_fields = {
        "timestamp": "$cells_data.timestamp",
        "cell_bsonType": "$cells_data.entries.cell_bsonType",
        "signal_level": "$cells_data.entries.signal_level",
        "signal_strength": "$cells_data.entries.signal_strength",
        "signal_level_1": "$cells_data.entries.signal_level_1",
        "cell_id": "$cells_data.entries.cell_id",
        "lac": "$cells_data.entries.lac",
        "mcc": "$cells_data.entries.mcc",
        "mnc": "$cells_data.entries.mnc",
        "double_of_entries": "$cells_data.double_of_entries"
    }
    wcdma_fields = {
        "timestamp": "$cells_data.timestamp",
        "is_registered": "$cells_data.entries.is_registered",
        "cell_bsonType": "$cells_data.entries.cell_bsonType",
        "cell_id": "$cells_data.entries.cell_id",
        "lac": "$cells_data.entries.lac",
        "mcc": "$cells_data.entries.mcc",
        "mnc": "$cells_data.entries.mnc",
        "psc": "$cells_data.entries.psc",
        "asu_level": "$cells_data.entries.asu_level",
        "dbm": "$cells_data.entries.dbm",
        "level": "$cells_data.entries.level",
        "double_of_entries": "$cells_data.double_of_entries"
    }

    # Pipelines
    lte_pipeline = generate_pipeline("LTE", lte_fields)
    gsm_pipeline = generate_pipeline("GSM", gsm_fields)
    wcdma_pipeline = generate_pipeline("WCDMA", wcdma_fields)

    # Execute pipelines
    try:
        lte_results = list(collection.aggregate(lte_pipeline))
        gsm_results = list(collection.aggregate(gsm_pipeline))
        wcdma_results = list(collection.aggregate(wcdma_pipeline))
    except Exception as e:
        print(f"Error during aggregation: {e}")
        lte_results, gsm_results, wcdma_results = [], [], []

    # Convert results to DataFrames
    lte_df = pd.DataFrame(lte_results) if lte_results else pd.DataFrame()
    gsm_df = pd.DataFrame(gsm_results) if gsm_results else pd.DataFrame()
    wcdma_df = pd.DataFrame(wcdma_results) if wcdma_results else pd.DataFrame()    

    
    # Add prefixes to distinguish LTE, GSM, and WCDMA fields
    lte_df = lte_df.rename(columns=lambda col: f"L-{col}" if col not in ["timestamp", "cell_bsonType"] else col)
    gsm_df = gsm_df.rename(columns=lambda col: f"G-{col}" if col not in ["timestamp", "cell_bsonType"] else col)
    wcdma_df = wcdma_df.rename(columns=lambda col: f"W-{col}" if col not in ["timestamp", "cell_bsonType"] else col)

    # Combine all three DataFrames by outer join on 'timestamp' and 'cell_bsonType'
    df = pd.concat([lte_df, gsm_df, wcdma_df], axis=0, ignore_index=True)

    # Sort by timestamp for better organization
    df = df.sort_values(by="timestamp").reset_index(drop=True)

    client.close()
    return df

## LabelSensor
def query_LabelSensor_data(
    URI,
    recording_id=None,
    start_timestamp=None,
    end_timestamp=None
):
    """
    Query MongoDB for LabelSensor data and return a pandas DataFrame.

    Parameters:
    - URI (str): MongoDB connection URI.
    - recording_id (str, optional): The recording ID to filter data.
    - start_timestamp (int, optional): The starting timestamp of the query range.
    - end_timestamp (int, optional): The ending timestamp of the query range.

    Returns:
    - DataFrame: A pandas DataFrame containing the label data.
    """
    # Connect to MongoDB
    client = MongoClient(URI)
    database = client["SensorDatabase"]
    collection = database["labelsensor"]

    # Build match conditions
    match_conditions = {}
    if recording_id:
        match_conditions["recording_id"] = recording_id
    if start_timestamp is not None or end_timestamp is not None:
        match_conditions["label_data.timestamp"] = {}
        if start_timestamp is not None:
            match_conditions["label_data.timestamp"]["$gte"] = start_timestamp
        if end_timestamp is not None:
            match_conditions["label_data.timestamp"]["$lte"] = end_timestamp

    # Aggregation pipeline
    pipeline = [
        {"$match": match_conditions},
        {"$unwind": "$label_data"},
        {
            "$match": {
                "$expr": {
                    "$and": [
                        {"$gte": ["$label_data.timestamp", start_timestamp if start_timestamp else 0]},
                        {"$lte": ["$label_data.timestamp", end_timestamp if end_timestamp else float("inf")]}
                    ]
                }
            }
        },
        {
            "$project": {
                "_id": 0,
                "timestamp": "$label_data.timestamp",
                "coarse_label": "$label_data.coarse_label",
                "fine_label": "$label_data.fine_label",
                "road_label": "$label_data.road_label",
                "traffic_label": "$label_data.traffic_label",
                "tunnels_label": "$label_data.tunnels_label",
                "social_label": "$label_data.social_label",
                "food_label": "$label_data.food_label"
            }
        }
    ]

    # Execute the pipeline
    try:
        results = list(collection.aggregate(pipeline))
    except Exception as e:
        print(f"Error during aggregation: {e}")
        results = []

    # Convert results to a pandas DataFrame
    df = pd.DataFrame(results) if results else pd.DataFrame(columns=[
        "timestamp", "coarse_label", "fine_label", "road_label",
        "traffic_label", "tunnels_label", "social_label", "food_label"
    ])

    # Close the client connection
    client.close()

    return df

## MotionSensor
def query_MotionSensor_data(
    URI,
    field,
    recording_id=None,
    sensor_location=None,
    start_timestamp=None,
    end_timestamp=None
):
    from pymongo import MongoClient
    import pandas as pd

    client = MongoClient(URI)
    database = client["SensorDatabase"]
    collection = database["motionsensor"]

    # Validate field input
    valid_fields = [
        "acceleration", "gyroscope", "magnetometer", "orientation",
        "gravity", "linear_acceleration", "pressure", "altitude", "temperature"
    ]
    if field not in valid_fields:
        raise ValueError(f"Invalid field: {field}. Valid fields are {', '.join(valid_fields)}")

    # Build match conditions
    match_conditions = {}
    if recording_id:
        match_conditions["recording_id"] = recording_id
    if sensor_location:
        match_conditions["sensor_location"] = sensor_location
    if start_timestamp is not None or end_timestamp is not None:
        match_conditions[f"{field}.timestamp"] = {}
        if start_timestamp is not None:
            match_conditions[f"{field}.timestamp"]["$gte"] = start_timestamp
        if end_timestamp is not None:
            match_conditions[f"{field}.timestamp"]["$lte"] = end_timestamp

    # Aggregation pipeline
    pipeline = [
        {"$match": match_conditions},
        {
            "$project": {
                "_id": 0,
                f"{field}": {
                    "$filter": {
                        "input": f"${field}",
                        "as": "item",
                        "cond": {
                            "$and": [
                                {"$gte": ["$$item.timestamp", start_timestamp if start_timestamp else 0]},
                                {"$lte": ["$$item.timestamp", end_timestamp if end_timestamp else float("inf")]}
                            ]
                        }
                    }
                }
            }
        },
        {"$unwind": f"${field}"},
        {
            "$project": {
                "timestamp": f"${field}.timestamp",
                **{
                    key: f"${field}.{key}" for key in ["x", "y", "z", "w", "value"]
                    if key in database["motionsensor"].find_one()[field][0]
                }
            }
        }
    ]

    # Execute the query
    try:
        results = list(collection.aggregate(pipeline))
    except Exception as e:
        print(f"Error during aggregation: {e}")
        results = []

    # Convert results to a pandas DataFrame
    df = pd.DataFrame(results) if results else pd.DataFrame(columns=["timestamp"])

    # Close the client connection
    client.close()

    return df