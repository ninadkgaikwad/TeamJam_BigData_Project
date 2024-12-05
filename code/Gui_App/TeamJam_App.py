from pymongo import MongoClient
from datetime import datetime, timezone
import pandas as pd
from dash import Dash, dcc, html, Input, Output, State, dash_table
import dash_bootstrap_components as dbc
import plotly.express as px

import App_Func_Module as AppFuncs

URI = "mongodb://localhost:27017/"
DATA_DF = pd.DataFrame()

app = Dash(__name__, external_stylesheets=[dbc.themes.SOLAR], suppress_callback_exceptions=True)

app.layout = dbc.Container([
    dcc.Store(id="sensor_field_store"),
    dcc.Store(id="sensor_name_store"),
    dcc.Store(id="recording_id_store"),
    dcc.Store(id="sensor_position_store"),

    dbc.Row([dbc.Col([html.H1("Sussex-Huawei Locomotion Dataset", className='text-center text-primary mb-4')], width=12)]),
    dbc.Row([dbc.Col([html.Br()], width=12)]),

    dbc.Row([dbc.Col([html.Button('1. Connect To Database ', id='Connect_DB_Button', className="btn btn-primary btn-lg col-12")], width=12)]),
    dbc.Row([dbc.Col([html.Br()], width=12)]),

    dbc.Row([
        dbc.Col([html.Label("2. Select Sensor Name:", className='text-left text-secondary mb-4')], width=3),
        dbc.Col([dcc.Dropdown(id='Sensor_Name_DropDown', options=[], value=None)], width=3),
        dbc.Col([html.Label("3. Select Recording ID:", className='text-left text-secondary mb-4')], width=3),
        dbc.Col([dcc.Dropdown(id='Recording_ID_DropDown', options=[], value=None)], width=3),
    ], justify="center", align="center"),
    dbc.Row([dbc.Col([html.Br()], width=12)]),

    dbc.Row([
        dbc.Col([html.Label("4. Select Sensor Position:", className='text-left text-secondary mb-4')], width=3),
        dbc.Col([dcc.Dropdown(id='Sensor_Position_DropDown', options=[], value=None)], width=3),
        dbc.Col([html.Label("5. Select Sensor Field:", className='text-left text-secondary mb-4')], width=3),
        dbc.Col([dcc.Dropdown(id='Sensor_Field_DropDown', options=[], value=None)], width=3),
    ], justify="center", align="center"),
    dbc.Row([dbc.Col([html.Br()], width=12)]),

    dbc.Row([
        dbc.Col([html.Label("Starting Date Time of Selected Data:", className='text-left text-secondary mb-4')], width=3),
        dbc.Col([html.Label("MM/DD/YYYY HH:MM:SS", id='Start_Datetime_Label')], width=3),
        dbc.Col([html.Label("Ending Date Time of Selected Data:", className='text-left text-secondary mb-4')], width=3),
        dbc.Col([html.Label("MM/DD/YYYY HH:MM:SS", id='End_Datetime_Label')], width=3),
    ]),
    dbc.Row([dbc.Col([html.Br()], width=12)]),

    dbc.Row([
        dbc.Col([html.Label("6. Enter Start Timestamp (UNIX):", className='text-left text-secondary mb-4')], width=6),
        dbc.Col([
            dcc.Input(
                id="Start_Timestamp_Input",
                type="number",
                placeholder="Enter start UNIX timestamp"
            )
        ], width=6),
    ], justify="center", align="center"),
    dbc.Row([
        dbc.Col([html.Label("7. Enter End Timestamp (UNIX):", className='text-left text-secondary mb-4')], width=6),
        dbc.Col([
            dcc.Input(
                id="End_Timestamp_Input",
                type="number",
                placeholder="Enter end UNIX timestamp"
            )
        ], width=6),
    ], justify="center", align="center"),
    dbc.Row([dbc.Col([html.Br()], width=12)]),

    dbc.Row([dbc.Col([html.Button('8. Compute Data Statistics', id='Statistics_Button', className="btn btn-primary btn-lg col-12")], width=12)]),
    dbc.Row([dbc.Col([html.Br()], width=12)]),
    dbc.Row([
        dbc.Col([html.H3("Data Statistics Table:", className='text-left text-secondary mb-4')]),
        dbc.Col([dash_table.DataTable(id='Data_Statistics_Table', columns=[], data=[], editable=True)], width=12),
    ], justify="center", align="center"),
    dbc.Row([dbc.Col([html.Br()], width=12)]),
    dbc.Row([
        dbc.Col([html.H3("Data Time Series Plot:", className='text-left text-secondary mb-4')], width=12),
        dbc.Col([dcc.Graph(id='Data_TimeSeries_Plot', figure={})], width=12),
    ], justify="center", align="center"),
], fluid=True)

LABEL_MAPPINGS = {
    "coarse_label": {
        0: "Null", 1: "Still", 2: "Walking", 3: "Running",
        4: "Biking", 5: "Car", 6: "Bus", 7: "Train", 8: "Subway"
    },
    "fine_label": {
        0: "Null", 1: "Still", 2: "Walking", 3: "Running",
        4: "Jogging", 5: "Fast Walking", 6: "Sprinting", 7: "Climbing",
        8: "Descending", 9: "Biking", 10: "Driving", 11: "Riding",
        12: "Standing", 13: "Sitting", 14: "Lying Down",
        15: "Cooking", 16: "Eating", 17: "Drinking", 18: "Typing"
    },
    "road_label": {
        0: "Null", 1: "City", 2: "Motorway", 3: "Countryside", 4: "Dirt Road"
    },
    "traffic_label": {0: "Null", 1: "Heavy Traffic"},
    "tunnels_label": {0: "Null", 1: "Tunnel"},
    "social_label": {0: "Null", 1: "Social"},
    "food_label": {1: "Eating", 2: "Drinking", 3: "Both", 4: "Null"}
}

def map_labels(df):
    for label_field, mapping in LABEL_MAPPINGS.items():
        if label_field in df.columns:
            df[f"{label_field}_description"] = df[label_field].map(mapping)
    return df

########################################################################################################################################################

# Callbacks

@app.callback(
    Output("sensor_name_store", "data"),
    Input("Connect_DB_Button", "n_clicks"),
    prevent_initial_call=True
)
def load_sensor_names(n_clicks):
    try:
        client = MongoClient(URI)
        database = client.get_database("SensorDatabase")
        return database.list_collection_names()
    except Exception as e:
        print(f"Error loading sensor names: {e}")
        return []

@app.callback(
    Output("Sensor_Name_DropDown", "options"),
    Input("sensor_name_store", "data")
)
def populate_sensor_name_dropdown(sensor_names):
    # Filter out 'userdata' and 'recording_data'
    filtered_sensor_names = [name for name in (sensor_names or []) if name not in ["userdata", "recordingdata"]]
    return [{"label": name, "value": name} for name in filtered_sensor_names]



@app.callback(
    Output("recording_id_store", "data"),
    Input("Sensor_Name_DropDown", "value"),
    prevent_initial_call=True
)
def load_recording_ids(sensor_name):
    try:
        client = MongoClient(URI)
        database = client.get_database("SensorDatabase")
        return database[sensor_name].distinct("recording_id")
    except Exception as e:
        print(f"Error loading recording IDs: {e}")
        return []

@app.callback(
    Output("Recording_ID_DropDown", "options"),
    Input("recording_id_store", "data")
)
def populate_recording_id_dropdown(recording_ids):
    return [{"label": rid, "value": rid} for rid in recording_ids or []]

@app.callback(
    Output("sensor_position_store", "data"),
    Input("Recording_ID_DropDown", "value"),
    State("Sensor_Name_DropDown", "value"),
    prevent_initial_call=True
)
def load_sensor_positions(recording_id, sensor_name):
    try:
        client = MongoClient(URI)
        database = client.get_database("SensorDatabase")
        
        # Return None or a default value if LabelSensor is selected
        if sensor_name.lower() == "labelsensor":
            return None  # or return ["default"] if you need a list format
        
        # For other sensors, query distinct sensor locations
        return database[sensor_name].distinct("sensor_location", {"recording_id": recording_id})
    except Exception as e:
        print(f"Error loading sensor positions: {e}")
        return []


@app.callback(
    Output("Sensor_Position_DropDown", "options"),
    Input("sensor_position_store", "data")
)
def populate_sensor_position_dropdown(positions):
    if positions is None:  # Handle the LabelSensor case
        return [{"label": "Not Applicable", "value": "default"}]
    return [{"label": pos, "value": pos} for pos in positions or []]


@app.callback(
    Output("sensor_field_store", "data"),
    Input("Sensor_Position_DropDown", "value"),
    State("Recording_ID_DropDown", "value"),
    State("Sensor_Name_DropDown", "value"),
    prevent_initial_call=True
)
def load_sensor_fields(sensor_position, recording_id, sensor_name):
    try:
        client = MongoClient(URI)
        database = client.get_database("SensorDatabase")
        collection = database[sensor_name]

        # Special handling for LabelSensor
        if sensor_name.lower() == "labelsensor":
            if recording_id:
                # Return label_data directly
                return ["label_data"]

        # Default behavior for other sensors
        fields = set()
        query_filter = {"recording_id": recording_id}
        if sensor_position and sensor_position != "default":
            query_filter["sensor_location"] = sensor_position

        for doc in collection.find(query_filter, {"_id": 0}):
            fields.update(doc.keys())

        fields.discard("sensor_location")
        return sorted(fields)

    except Exception as e:
        print(f"Error loading sensor fields: {e}")
        return []




@app.callback(
    Output("Sensor_Field_DropDown", "options"),
    Input("sensor_field_store", "data")
)
def populate_sensor_field_dropdown(fields):
    print("Dropdown fields:", fields) 
    return [{"label": field, "value": field} for field in fields or []]

def validate_parameter(param, param_name):
    if not isinstance(param, str) or not param.strip():
        raise ValueError(f"{param_name} must be a non-empty string. Received: {param}")

@app.callback(
    Output("Start_Timestamp_Input", "style"),
    Output("End_Timestamp_Input", "style"),
    Input("Start_Timestamp_Input", "value"),
    Input("End_Timestamp_Input", "value"),
    State("Start_Datetime_Label", "children"),
    State("End_Datetime_Label", "children"),
)
def validate_timestamps(start_input, end_input, min_timestamp_label, max_timestamp_label):
    try:
        # Extract the numeric min and max timestamps
        min_timestamp = float(min_timestamp_label.split(": ")[-1])
        max_timestamp = float(max_timestamp_label.split(": ")[-1])

        # Default styles
        valid_style = {"borderColor": "green"}
        invalid_style = {"borderColor": "red"}

        # Validate start and end timestamps
        start_style = valid_style if min_timestamp <= (start_input or 0) <= max_timestamp else invalid_style
        end_style = valid_style if min_timestamp <= (end_input or 0) <= max_timestamp else invalid_style

        return start_style, end_style
    except (ValueError, IndexError):
        # Handle errors in parsing or missing timestamps
        error_style = {"borderColor": "red"}
        return error_style, error_style


def query_sensor_data(uri, collection_name, recording_id=None, sensor_location=None, 
                      start_timestamp=None, end_timestamp=None, nested_field=None):
    """
    Query MongoDB for sensor data based on flexible criteria and dynamically extract nested fields.

    Parameters:
    - uri (str): MongoDB connection URI.
    - collection_name (str): Name of the MongoDB collection.
    - recording_id (str, optional): Recording ID to filter data.
    - sensor_location (str, optional): Sensor location to filter data.
    - start_timestamp (int, optional): Starting timestamp of the query range.
    - end_timestamp (int, optional): Ending timestamp of the query range.
    - nested_field (str, optional): Name of the nested field array to unwind.

    Returns:
    - DataFrame: A pandas DataFrame containing the flattened results.
    - list: List of numeric fields in the DataFrame.
    - list: List of string fields in the DataFrame (excluding specified fields).
    """
    client = MongoClient(uri)
    database = client.get_database("SensorDatabase")
    collection = database.get_collection(collection_name)

    pipeline = []

    # Build match conditions
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

    # Unwind the nested field if provided
    if nested_field:
        pipeline.append({"$unwind": f"${nested_field}"})

    # Dynamically project all fields within the nested field
    if nested_field:
        pipeline.append({
            "$project": {
                "_id": 0,
                "recording_id": 1,
                "sensor_location": 1,
                f"{nested_field}": 1  # Include the entire nested document
            }
        })

    # Execute the query
    results = list(collection.aggregate(pipeline))
    if not results:
        print("No data found for the given criteria.")
        return pd.DataFrame(), [], []

    # Convert results to a DataFrame
    df = pd.DataFrame(results)

    # Flatten the nested field if it exists
    if nested_field and nested_field in df.columns:
        nested_data = pd.json_normalize(df[nested_field])  # Flatten nested JSON
        df = pd.concat([df.drop(columns=[nested_field]), nested_data], axis=1)

    # Identify numeric and string fields
    numeric_fields = [col for col in df.columns if pd.api.types.is_numeric_dtype(df[col])]
    excluded_string_fields = {"timestamp", "recording_id", "sensor_location"}
    string_fields = [
        col for col in df.columns
        if pd.api.types.is_string_dtype(df[col]) and col not in excluded_string_fields
    ]

    print("Extracted DataFrame Head:")
    print(df.head())
    print("Numeric Fields:", numeric_fields)
    print("String Fields:", string_fields)

    return df, numeric_fields, string_fields



# Calculate Sensor Statistics
def calculate_sensor_statistics(df):
    """
    Calculate statistics for numeric and string fields in a DataFrame.

    Parameters:
    - df (DataFrame): The input DataFrame.

    Returns:
    - DataFrame: A DataFrame containing calculated statistics for each field.
    """
    stats = []

    for column in df.columns:
        if pd.api.types.is_numeric_dtype(df[column]):
            stats.append({
                "Field": column,
                "Min": df[column].min(),
                "Max": df[column].max(),
                "Mean": df[column].mean(),
                "Std Dev": df[column].std(),
                "Frequency": len(df[column].dropna())
            })
        elif pd.api.types.is_string_dtype(df[column]):
            stats.append({
                "Field": column,
                "Value Counts": df[column].value_counts().to_dict()
            })

    return pd.DataFrame(stats)


def query_label_sensor_data(uri, collection_name, start_timestamp=None, end_timestamp=None):
    """
    Query MongoDB for LabelSensor data.

    Parameters:
    - uri (str): MongoDB connection URI.
    - collection_name (str): Name of the MongoDB collection.
    - start_timestamp (int, optional): Starting timestamp of the query range.
    - end_timestamp (int, optional): Ending timestamp of the query range.

    Returns:
    - DataFrame: A pandas DataFrame containing label sensor data.
    """
    client = MongoClient(uri)
    database = client.get_database("SensorDatabase")
    collection = database[collection_name]

    # Build aggregation pipeline
    pipeline = []

    # Match documents within the timestamp range
    match_stage = {"$match": {}}
    if start_timestamp and end_timestamp:
        match_stage["$match"]["label_data.timestamp"] = {"$gte": start_timestamp, "$lte": end_timestamp}
    if match_stage["$match"]:
        pipeline.append(match_stage)

    # Transform non-list label_data into list
    pipeline.append({
        "$addFields": {
            "label_data": {
                "$cond": {
                    "if": {"$isArray": "$label_data"},
                    "then": "$label_data",
                    "else": {"$literal": ["$label_data"]}
                }
            }
        }
    })

    # Unwind the label_data field to create one document per label entry
    pipeline.append({"$unwind": "$label_data"})

    # Project the necessary fields
    pipeline.append({
        "$project": {
            "timestamp": "$label_data.timestamp",
            "coarse_label": "$label_data.coarse_label",
            "fine_label": "$label_data.fine_label",
            "road_label": "$label_data.road_label",
            "traffic_label": "$label_data.traffic_label",
            "tunnels_label": "$label_data.tunnels_label",
            "social_label": "$label_data.social_label",
            "food_label": "$label_data.food_label",
            "recording_id": 1
        }
    })

    # Execute the query
    results = list(collection.aggregate(pipeline))
    if not results:
        return pd.DataFrame()

    # Convert results to DataFrame
    df = pd.DataFrame(results)
    df = map_labels(df)
    return df



@app.callback(
    Output("Data_Statistics_Table", "data"),
    Output("Data_Statistics_Table", "columns"),
    Output("Data_TimeSeries_Plot", "figure"),
    Input("Statistics_Button", "n_clicks"),
    State("Sensor_Name_DropDown", "value"),
    State("Recording_ID_DropDown", "value"),
    State("Sensor_Position_DropDown", "value"),
    State("Sensor_Field_DropDown", "value"),
    State("Start_Timestamp_Input", "value"),
    State("End_Timestamp_Input", "value"),
    prevent_initial_call=True
)
def compute_statistics_and_plot(n_clicks, sensor_name, recording_id, sensor_position, sensor_field, start_timestamp, end_timestamp):
    # Validate inputs
    if not all([sensor_name, recording_id, start_timestamp, end_timestamp]):
        return [], [], px.bar(title="Invalid Input: Please fill all fields")

    # Specialized handling for LabelSensor collection
    if sensor_name.lower() == "labelsensor":
        df = query_label_sensor_data(URI, sensor_name, start_timestamp, end_timestamp)
        if df.empty:
            return [], [], px.bar(title="No Data Available for LabelSensor")

        # Compute label statistics
        label_stats = []
        for label_field, mapping in LABEL_MAPPINGS.items():
            if label_field in df.columns:
                frequencies = df[label_field].value_counts().sort_index()
                for value, freq in frequencies.items():
                    label_stats.append({
                        "Label": mapping.get(value, "Unknown"),
                        "Mean": df[label_field].mean(),
                        "Min": df[label_field].min(),
                        "Max": df[label_field].max(),
                        "Std Dev": df[label_field].std(),
                        "Frequency": freq
                    })
        stats_df = pd.DataFrame(label_stats)

        # Format table data and columns
        stats_data = stats_df.to_dict("records")
        stats_columns = [{"name": col, "id": col} for col in stats_df.columns]

        # Create bar chart for label frequency
        plot_data = []
        for label_field, mapping in LABEL_MAPPINGS.items():
            if label_field in df.columns:
                field_counts = df[label_field].value_counts().reset_index()
                field_counts.columns = ["Value", "Frequency"]
                field_counts["Label"] = field_counts["Value"].map(mapping)
                field_counts["Field"] = label_field
                plot_data.append(field_counts)
        
        plot_data = pd.concat(plot_data, ignore_index=True)
        fig = px.bar(
            plot_data,
            x="Frequency",
            y="Label",
            color="Field",
            orientation="h",
            title="Label Frequency Distribution",
            hover_data={"Value": True, "Field": True}
        )
        return stats_data, stats_columns, fig

    # General handling for other collections
    df, numeric_fields, string_fields = query_sensor_data(
        URI,
        sensor_name,
        recording_id=recording_id,
        sensor_location=sensor_position,
        start_timestamp=start_timestamp,
        end_timestamp=end_timestamp,
        nested_field=sensor_field
    )

    if df.empty:
        return [], [], px.bar(title="No Data Available")

    # Compute statistics for general collections
    stats = []
    for field in numeric_fields:
        stats.append({
            "Field": field,
            "Min": df[field].min(),
            "Max": df[field].max(),
            "Mean": df[field].mean(),
            "Std Dev": df[field].std(),
            "Frequency": len(df[field].dropna())
        })
    stats_df = pd.DataFrame(stats)

    # Format table data and columns
    stats_data = stats_df.to_dict("records")
    stats_columns = [{"name": col, "id": col} for col in stats_df.columns]

    # Prepare plot data for numeric or string fields
    if string_fields:
        # Prepare plot data for string fields
        plot_data = pd.DataFrame()
        for field in string_fields:
            counts = df[field].value_counts().reset_index()
            counts.columns = ["Value", "Frequency"]
            counts["Field"] = field
            plot_data = pd.concat([plot_data, counts], ignore_index=True)

        # Create bar chart for string fields
        fig = px.bar(
            plot_data,
            x="Frequency",
            y="Value",
            color="Field",
            orientation="h",
            title=f"Frequency Distribution of String Fields in {sensor_field}"
        )
    else:
        # Prepare plot data for numeric fields
        plot_data = pd.DataFrame()
        for field in numeric_fields:
            field_data = pd.DataFrame({
                "Field": [field] * len(df),
                "Value": df[field]
            })
            plot_data = pd.concat([plot_data, field_data], ignore_index=True)

        # Create a bar chart with fields on the x-axis and counts on the y-axis
        if not plot_data.empty:
            fig = px.histogram(
                plot_data,
                x="Field",  # X-axis displays field names
                color="Field",  # Color-coordinate bars based on field names
                labels={"x": "Fields", "y": "Count"},
                title="Count of Values per Field",
                nbins=len(numeric_fields)  # Ensure proper binning for numeric fields
            )
        else:
            fig = px.bar(title="No Numeric Fields Available")

    return stats_data, stats_columns, fig




@app.callback(
    Output("Start_Datetime_Label", "children"),
    Output("End_Datetime_Label", "children"),
    Input("Sensor_Field_DropDown", "value"),
    State("Sensor_Position_DropDown", "value"),
    State("Recording_ID_DropDown", "value"),
    State("Sensor_Name_DropDown", "value"),
    prevent_initial_call=True
)
def display_timestamps(sensor_field, sensor_position, recording_id, sensor_name):
    try:
        # Connect to the database
        client = MongoClient(URI)
        database = client.get_database("SensorDatabase")
        collection = database[sensor_name]

        # Initialize an empty list for timestamps
        timestamps = []

        # Special handling for LabelSensor
        if sensor_name.lower() == "labelsensor" and sensor_field == "label_data":
            # Query the label_data array directly
            for doc in collection.find(
                {"recording_id": recording_id},
                {"label_data.timestamp": 1, "_id": 0}
            ):
                if "label_data" in doc:
                    for entry in doc["label_data"]:
                        if "timestamp" in entry:
                            try:
                                timestamp = float(entry["timestamp"])
                                if timestamp > 0:  # Ensure valid positive timestamp
                                    timestamps.append(timestamp)
                            except ValueError:
                                print(f"Invalid timestamp encountered: {entry['timestamp']}")
        
        else:
            # Default behavior for other sensors
            for doc in collection.find(
                {"recording_id": recording_id, "sensor_location": sensor_position},
                {f"{sensor_field}.timestamp": 1, "_id": 0}
            ):
                if sensor_field in doc:
                    for entry in doc[sensor_field]:
                        if "timestamp" in entry:
                            try:
                                timestamp = float(entry["timestamp"])
                                if timestamp > 0:  # Ensure valid positive timestamp
                                    timestamps.append(timestamp)
                            except ValueError:
                                print(f"Invalid timestamp encountered: {entry['timestamp']}")

        # Process timestamps
        if timestamps:
            timestamps.sort()
            min_time = timestamps[0]
            max_time = timestamps[-1]
            return f"Min Timestamp: {min_time}", f"Max Timestamp: {max_time}"
        return "Not available", "Not available"

    except Exception as e:
        print(f"Error retrieving timestamps: {e}")
        return "Error occurred", "Error occurred"

    
    
# Running the App
if __name__ == '__main__':
    app.run_server(debug=True, port=4010)