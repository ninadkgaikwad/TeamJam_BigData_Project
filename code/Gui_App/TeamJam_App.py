# Importing Desired Modules
from pymongo import MongoClient
from datetime import datetime, timezone
import pandas as pd
from dash import Dash, dcc, html, Input, Output, State, dash_table
import dash_bootstrap_components as dbc
import plotly.express as px

# Importing Custom Modules
import App_Func_Module as AppFuncs

# Defining App Constants
URI = "mongodb://localhost:27017/"
DATA_DF = pd.DataFrame()  # Placeholder for queried data

# Creating the App Object and Selecting Theme
app = Dash(__name__, external_stylesheets=[dbc.themes.SOLAR])

# Creating the App Layout
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
        dbc.Col([html.Label("3. Select Recording ID:")], width=3),
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
        dbc.Col([html.Label("Starting Date Time of Selected Data:")], width=3),
        dbc.Col([html.Label("mm/dd/yyyy HH:MM:SS", id='Start_Datetime_Label')], width=3),
        dbc.Col([html.Label("Ending Date Time of Selected Data:")], width=3),
        dbc.Col([html.Label("mm/dd/yyyy HH:MM:SS", id='End_DateTime_Label')], width=3),
    ]),

    dbc.Row([dbc.Col([html.Br()], width=12)]),

    dbc.Row([
        dbc.Col([html.Button('8. Compute Data Statistics', id='Statistics_Button', className="btn btn-primary btn-lg col-12")], width=12)
    ], justify="center", align="center"),

    dbc.Row([dbc.Col([html.Br()], width=12)]),

    dbc.Row([
        dbc.Col([html.H3("Data Time Series Plot:", className='text-left text-secondary mb-4')], width=12),
        dbc.Col([dcc.Graph(id='Data_TimeSeries_Plot', figure={})], width=12),
    ], justify="center", align="center"),
], fluid=True)

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
    return [{"label": name, "value": name} for name in sensor_names or []]

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
        return database[sensor_name].distinct("sensor_location", {"recording_id": recording_id})
    except Exception as e:
        print(f"Error loading sensor positions: {e}")
        return []

@app.callback(
    Output("Sensor_Position_DropDown", "options"),
    Input("sensor_position_store", "data")
)
def populate_sensor_position_dropdown(positions):
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
        fields = set()
        for doc in collection.find({"recording_id": recording_id, "sensor_location": sensor_position}, {"_id": 0}):
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
    return [{"label": field, "value": field} for field in fields or []]

@app.callback(
    Output("Start_Datetime_Label", "children"),
    Output("End_DateTime_Label", "children"),
    Input("Sensor_Field_DropDown", "value"),
    State("Sensor_Position_DropDown", "value"),
    State("Recording_ID_DropDown", "value"),
    State("Sensor_Name_DropDown", "value"),
    prevent_initial_call=True
)
def display_timestamps(sensor_field, sensor_position, recording_id, sensor_name):
    try:
        client = MongoClient(URI)
        database = client.get_database("SensorDatabase")
        collection = database[sensor_name]
        timestamps = []
        for doc in collection.find({"recording_id": recording_id, "sensor_location": sensor_position}, {f"{sensor_field}.timestamp": 1, "_id": 0}):
            if sensor_field in doc:
                timestamps += [entry["timestamp"] for entry in doc[sensor_field] if "timestamp" in entry]
        timestamps.sort()
        if timestamps:
            min_time = datetime.fromtimestamp(timestamps[0] / 1000, tz=timezone.utc).strftime("%m/%d/%Y %H:%M:%S")
            max_time = datetime.fromtimestamp(timestamps[-1] / 1000, tz=timezone.utc).strftime("%m/%d/%Y %H:%M:%S")
            return min_time, max_time
        return "Not available", "Not available"
    except Exception as e:
        print(f"Error retrieving timestamps: {e}")
        return "Not available", "Not available"

# Running the App
if __name__ == '__main__':
    app.run_server(port=4030)
