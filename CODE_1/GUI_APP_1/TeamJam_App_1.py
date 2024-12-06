#########################################################################################################
# Importing Desired Modules
#########################################################################################################

from pymongo import MongoClient
from dash import Dash, dcc, html, Input, Output, State, dash_table
import dash_bootstrap_components as dbc
import plotly.express as px
from datetime import datetime
import pandas as pd

#########################################################################################################
# Importing Custom Modules
#########################################################################################################

import Query_Module as AppFuncs

#########################################################################################################
# Defining App Constants
#########################################################################################################

# Database Location
URI = "mongodb://localhost:27017/"

# Empty Dataframe for Queried Data
DATA_DF = pd.DataFrame()

#########################################################################################################
# Creating the App Object and Selecting Theme
#########################################################################################################

app = Dash(__name__, external_stylesheets=[dbc.themes.SOLAR])

#########################################################################################################
# Creating the App Layout - For Design
#########################################################################################################

app.layout = dbc.Container([
    
    # Row 1
    dbc.Row([
        
        dbc.Col([
            
            html.H1("Sussex-Huawei Locomotion Dataset", 
                    className = 'text-center text-primary mb-4')
            
            ], xs = 12, sm = 12, md = 12, lg = 12, xl = 12), # width = 12
        
        ], justify = "center", align = "center"),

    # Break Row
    dbc.Row([
        
        dbc.Col([
            
            html.Br()
            
            ], width = 12),
        
        ]),  

    # Row 2
    dbc.Row([
        
        dbc.Col([
            
            html.Button('1. Connect To Database ', id = 'Connect_DB_Button', 
                        className = "btn btn-primary btn-lg col-12") ,
            
            ], xs = 12, sm = 12, md = 12, lg = 12, xl = 12), # width = 12
        
        ], justify = "center", align = "center"), 

    # Break Row
    dbc.Row([
        
        dbc.Col([
            
            html.Br()
            
            ], width = 12),
        
        ]),
    
    # Row 3
    dbc.Row([
        
        dbc.Col([
            
            html.H3("Query Parameters:",
                    className = 'text-left text-secondary mb-4')
            
            ], xs = 12, sm = 12, md = 12, lg = 12, xl = 12), # width = 12     
        
        ], justify = "left", align = "center"),

    # Break Row
    dbc.Row([
        
        dbc.Col([
            
            html.Br()
            
            ], width = 12),
        
        ]),
    
    # Row 4
    dbc.Row([
        
        dbc.Col([
            
            html.Label("2. Select Sensor Name:",
                    className = 'text-left text-secondary mb-4'), 
            
            ], xs = 6, sm = 6, md = 6, lg = 3, xl = 3), # width = 12         
        
        dbc.Col([
            
            dcc.Dropdown(options = [],
                            value = None,
                            id = 'Sensor_Name_DropDown',
                            ),  
            
            ], xs = 6, sm = 6, md = 6, lg = 3, xl = 3), # width = 12         
        
        dbc.Col([
            
            html.Label("3. Select Recording ID:"),  
            
            ], xs = 6, sm = 6, md = 6, lg = 3, xl = 3), # width = 12           
        
        dbc.Col([
            
            dcc.Dropdown(options = [],
                            value = None,
                            id = 'Recording_ID_DropDown',
                            ),  
            
            ], xs = 6, sm = 6, md = 6, lg = 3, xl = 3), # width = 12            
        
        ], justify = "center", align = "center"),  
    
    # Break Row
    dbc.Row([
        
        dbc.Col([
            
            html.Br()
            
            ], width = 12),
        
        ]),
    
   
    
    # Row 5
    dbc.Row([
        
        dbc.Col([
            
            html.Label("4. Select Sensor Position:",
                    className = 'text-left text-secondary mb-4'), 
            
            ], xs = 6, sm = 6, md = 6, lg = 3, xl = 3), # width = 12         
        
        dbc.Col([
            
            dcc.Dropdown(options = [],
                            value = None,
                            id = 'Sensor_Position_DropDown',
                            ),  
            
            ], xs = 6, sm = 6, md = 6, lg = 3, xl = 3), # width = 12  

        dbc.Col([
            
            html.Label("5. Select Sensor Field:",
                    className = 'text-left text-secondary mb-4'), 
            
            ], xs = 6, sm = 6, md = 6, lg = 3, xl = 3), # width = 12         
        
        dbc.Col([
            
            dcc.Dropdown(options = [],
                            value = None,
                            id = 'Sensor_Field_DropDown',
                            ),  
            
            ], xs = 6, sm = 6, md = 6, lg = 3, xl = 3), # width = 12        
        
                   
        
        ], justify = "center", align = "center"),  
    
    # Break Row
    dbc.Row([
        
        dbc.Col([
            
            html.Br()
            
            ], width = 12),
        
        ]),    

    # Row 6
    dbc.Row([

        dbc.Col([
            
            html.Label("Starting Date Time of Selected Data:"),  
            
            ], xs = 6, sm = 6, md = 6, lg = 3, xl = 3), # width = 12           
        
        dbc.Col([
            
            html.Label("mm/dd/yyyy HH:MM:SS",
                            id = 'Starting_DateTime_Range_Label',
                            ),  
            
            ], xs = 6, sm = 6, md = 6, lg = 3, xl = 3), # width = 12 

        dbc.Col([
            
            html.Label("Ending Date Time of Selected Data:"),  
            
            ], xs = 6, sm = 6, md = 6, lg = 3, xl = 3), # width = 12           
        
        dbc.Col([
            
            html.Label("mm/dd/yyyy HH:MM:SS",
                            id = 'Ending_DateTime_Range_Label',
                            ),  
            
            ], xs = 6, sm = 6, md = 6, lg = 3, xl = 3), # width = 12 
        
                    
        
        ], justify = "center", align = "center"),  

    # Break Row
    dbc.Row([
        
        dbc.Col([
            
            html.Br()
            
            ], width = 12),
        
        ]),  

    # Row 7
    dbc.Row([
        
        dbc.Col([
            
            html.Label("6. Enter Start Datetime:",
                    className = 'text-left text-secondary mb-4'), 
            
            ], xs = 6, sm = 6, md = 6, lg = 3, xl = 3), # width = 12         
        
        dbc.Col([
            
            dcc.Input(
                id = "Start_Datetime_Input",
                type = "text",
                placeholder = "mm/dd/yyyy HH:MM:SS",
            ),  
            
            ], xs = 6, sm = 6, md = 6, lg = 3, xl = 3), # width = 12         
        
        dbc.Col([
            
            html.Label("7. Enter End Datetime:",
                    className = 'text-left text-secondary mb-4'), 
            
            ], xs = 6, sm = 6, md = 6, lg = 3, xl = 3), # width = 12         
        
        dbc.Col([
            
            dcc.Input(
                id = "End_Datetime_Input",
                type = "text",
                placeholder = "mm/dd/yyyy HH:MM:SS",
            ),  
            
            ], xs = 6, sm = 6, md = 6, lg = 3, xl = 3), # width = 12 
        
        ], justify = "center", align = "center"), 
    
    # Break Row
    dbc.Row([
        
        dbc.Col([
            
            html.Br()
            
            ], width = 12),
        
        ]),  

    # Row 8
    dbc.Row([
        
        dbc.Col([
            
            html.Button('8. Compute Data Statistics ', id = 'Statistics_Button', 
                        className = "btn btn-primary btn-lg col-12") ,
            
            ], xs = 12, sm = 12, md = 12, lg = 12, xl = 12), # width = 12
        
        ], justify = "center", align = "center"), 

    # Break Row
    dbc.Row([
        
        dbc.Col([
            
            html.Br()
            
            ], width = 12),
        
        ]),  

    # Row 9
    dbc.Row([
        
        dbc.Col([
            
            html.H3("Data Statistics Table:",
                    className = 'text-left text-secondary mb-4')
            
            ], xs = 12, sm = 12, md = 12, lg = 12, xl = 12), # width = 12     
        
        ], justify = "left", align = "center"),

    # Break Row
    dbc.Row([
        
        dbc.Col([
            
            html.Br()
            
            ], width = 12),
        
        ]),  

    # Row 10
    dbc.Row([
        
        dbc.Col([
            
            dash_table.DataTable(
                                id='Data_Statistics_Table',
                                columns=[],
                                data=[],
                                editable=True
                            ),
            
            ], xs = 12, sm = 12, md = 12, lg = 12, xl = 12), # width = 12     
        
        ], justify = "left", align = "center"),

    # Break Row
    dbc.Row([
        
        dbc.Col([
            
            html.Br()
            
            ], width = 12),
        
        ]),
    
    # Row 11
    dbc.Row([
        
        dbc.Col([
            
            html.Label("9. Select Data Sub-Field for Plotting:",
                    className = 'text-left text-secondary mb-4'), 
            
            ], xs = 6, sm = 6, md = 6, lg = 6, xl = 6), # width = 12         
        
        dbc.Col([
            
            dcc.Dropdown(options = [],
                            value = None,
                            id = 'Subfield_DropDown',
                            multi=True,
                            ),  
            
            ], xs = 6, sm = 6, md = 6, lg = 6, xl = 6), # width = 12               
                   
        
        ], justify = "center", align = "center"), 

    # Break Row
    dbc.Row([
        
        dbc.Col([
            
            html.Br()
            
            ], width = 12),
        
        ]),  

    # Row 12
    dbc.Row([
        
        dbc.Col([
            
            html.Button('10. Plot Data Time Series ', id = 'Plot_Button', 
                        className = "btn btn-primary btn-lg col-12") ,
            
            ], xs = 12, sm = 12, md = 12, lg = 12, xl = 12), # width = 12
        
        ], justify = "center", align = "center"), 

    # Break Row
    dbc.Row([
        
        dbc.Col([
            
            html.Br()
            
            ], width = 12),
        
        ]),  

    # Row 13
    dbc.Row([
        
        dbc.Col([
            
            html.H3("Data Time Series Plot:",
                    className = 'text-left text-secondary mb-4')
            
            ], xs = 12, sm = 12, md = 12, lg = 12, xl = 12), # width = 12     
        
        ], justify = "left", align = "center"),

    # Break Row
    dbc.Row([
        
        dbc.Col([
            
            html.Br()
            
            ], width = 12),
        
        ]),  

    # Row 14
    dbc.Row([
        
        dbc.Col([
            
            dcc.Graph(id = 'Data_TimeSeries_Plot', figure ={}),
            
            ], xs = 12, sm = 12, md = 12, lg = 12, xl = 12), # width = 12
        
        ], justify = "center", align = "center"),    
    
    
], fluid = False)

#########################################################################################################
# Creating App Callbacks -  For Functionality
#########################################################################################################

## 1 - Connect to Database (Button) Interaction -> Polpulates Select Sensor Name Dropdown
@app.callback(    
    Output(component_id = 'Sensor_Name_DropDown', component_property = 'options'),
    Input(component_id = 'Connect_DB_Button', component_property = 'n_clicks'),  
    prevent_initial_call = False)
def ConnectDB_Button_Interaction(N_Clicks):

    # Connect to MongoDB
    client = MongoClient(URI)

    # Access the database
    database = client.get_database("SensorDatabase")

    # Get all collection names
    collection_names = database.list_collection_names()
    
    return collection_names 

## 2 - Select Sensor Name (DropDown) Interaction -> Polpulates Select Recording ID Dropdown
@app.callback(    
    Output(component_id = 'Recording_ID_DropDown', component_property = 'options'),
    Input(component_id = 'Sensor_Name_DropDown', component_property = 'value'),  
    prevent_initial_call = False)
def SensorName_DropDown_Interaction(Sensor_Name_DropDown_Value):

    # Connect to MongoDB
    client = MongoClient(URI)

    # Access the database
    database = client.get_database("SensorDatabase")

    # Access the collection    
    collection = database[Sensor_Name_DropDown_Value]

    # Find all unique recording_id values
    unique_recording_ids = collection.distinct("recording_id")
    
    return unique_recording_ids 

## 3 - Select Recording ID (DropDown) Interaction -> Polpulates Select Sensor Position Dropdown
@app.callback(    
    Output(component_id = 'Sensor_Position_DropDown', component_property = 'options'),
    Input(component_id = 'Recording_ID_DropDown', component_property = 'value'),  
    State(component_id = 'Sensor_Name_DropDown', component_property = 'value'),
    prevent_initial_call = False)
def RecordingID_DropDown_Interaction(Recording_ID_DropDown_Value, Sensor_Name_DropDown_Value):

    # Connect to MongoDB
    client = MongoClient(URI)

    # Access the database
    database = client.get_database("SensorDatabase")

    # Access the collection
    collection = database[Sensor_Name_DropDown_Value]

    # Query to find all documents for the given recording_id
    query = {"recording_id": Recording_ID_DropDown_Value}

    # Use distinct to find unique sensor_location values, handling cases where it's missing
    unique_sensor_locations = collection.distinct("sensor_location", query)

    # Check if the result is empty or sensor_location is not present
    if not unique_sensor_locations:
        unique_sensor_locations = [None]
    
    return unique_sensor_locations 

## 4 - Select Sensor Position (DropDown) Interaction -> Polpulates Select Sensor Field Dropdown
@app.callback(    
    Output(component_id = 'Sensor_Field_DropDown', component_property = 'options'),
    Input(component_id = 'Sensor_Position_DropDown', component_property = 'value'),
    State(component_id = 'Recording_ID_DropDown', component_property = 'value'),  
    State(component_id = 'Sensor_Name_DropDown', component_property = 'value'),
    prevent_initial_call = False)
def SensorField_DropDown_Interaction(Sensor_Position_DropDown_Value, Recording_ID_DropDown_Value, Sensor_Name_DropDown_Value):

    # Connect to MongoDB
    client = MongoClient(URI)

    # Access the database and collection
    database = client.get_database("SensorDatabase")
    collection = database[Sensor_Name_DropDown_Value]

    # Query to filter documents
    query = {"recording_id": Recording_ID_DropDown_Value, "sensor_location": Sensor_Position_DropDown_Value}

    # Retrieve distinct top-level properties
    properties = set()
    for doc in collection.find(query, {"_id": 0}):  # Exclude '_id' from the projection
        properties.update(doc.keys())

    # Remove 'sensor_location' from the list
    properties.discard("sensor_location")

    # Convert to a sorted list for clarity
    unique_properties = sorted(properties)
    
    return unique_properties 

## 5 - Select Sensor Field (DropDown) Interaction -> Polpulates Starting/Ending Data Time of Selected Data Labels
@app.callback(
    Output(component_id='Starting_DateTime_Range_Label', component_property='children'),
    Output(component_id='Ending_DateTime_Range_Label', component_property='children'),
    Input(component_id='Sensor_Field_DropDown', component_property='value'),
    State(component_id='Sensor_Position_DropDown', component_property='value'),
    State(component_id='Recording_ID_DropDown', component_property='value'),
    State(component_id='Sensor_Name_DropDown', component_property='value'),
    prevent_initial_call=False
)
def SensorField_DropDown_Interaction(Sensor_Field_DropDown_Value, Sensor_Position_DropDown_Value, Recording_ID_DropDown_Value, Sensor_Name_DropDown_Value):

    # Connect to MongoDB
    client = MongoClient(URI)

    # Access the database and collection
    database = client.get_database("SensorDatabase")
    collection = database[Sensor_Name_DropDown_Value]  # Assume this is the collection name

    # Query to filter documents
    query = {
        "recording_id": Recording_ID_DropDown_Value,
        "sensor_location": Sensor_Position_DropDown_Value
    }

    # Retrieve all timestamps from the documents
    timestamps = []
    for doc in collection.find(query, {f"{Sensor_Field_DropDown_Value}.timestamp": 1, "_id": 0}):
        if Sensor_Field_DropDown_Value in doc:
            for entry in doc[Sensor_Field_DropDown_Value]:
                if "timestamp" in entry:
                    timestamps.append(entry["timestamp"])

    # Ensure timestamps are sorted
    timestamps.sort()

    # Convert first and last timestamps to "mm/dd/yyyy HH:MM:SS" format
    if timestamps:
        first_timestamp = datetime.utcfromtimestamp(timestamps[0] / 1000).strftime("%m/%d/%Y %H:%M:%S")
        last_timestamp = datetime.utcfromtimestamp(timestamps[-1] / 1000).strftime("%m/%d/%Y %H:%M:%S")
    else:
        first_timestamp = "Not available"
        last_timestamp = "Not available"

    return first_timestamp, last_timestamp

## 6 - Compute Data Statistics (Button) Interaction -> Polpulates Data_Statistics_Table (DashTable) and the Select Data Sub-Field for Plotting (DropDown)
@app.callback(
    Output(component_id='Data_Statistics_Table', component_property='data'),
    Output(component_id='Data_Statistics_Table', component_property='columns'),
    Output(component_id='Subfield_DropDown', component_property='options'),
    Input(component_id = 'Statistics_Button', component_property = 'n_clicks'),  
    State(component_id='Sensor_Field_DropDown', component_property='value'),
    State(component_id='Sensor_Position_DropDown', component_property='value'),
    State(component_id='Recording_ID_DropDown', component_property='value'),
    State(component_id='Sensor_Name_DropDown', component_property='value'),
    State(component_id='Start_Datetime_Input', component_property='value'),
    State(component_id='End_Datetime_Input', component_property='value'),
    prevent_initial_call=False
)
def Statistics_Button_Interaction(N_Clicks, Sensor_Field_DropDown_Value, Sensor_Position_DropDown_Value, Recording_ID_DropDown_Value, Sensor_Name_DropDown_Value, Start_Datetime_Input, End_Datetime_Input):

    # Convert Start_Datetime_Input, End_Datetime_Input to Unix Epoch
    Start_Datetime_Input_Unix = int((datetime.strptime(Start_Datetime_Input, "%m/%d/%Y %H:%M:%S").timestamp())*1000)
    End_Datetime_Input_Unix = int((datetime.strptime(End_Datetime_Input, "%m/%d/%Y %H:%M:%S").timestamp())*1000)

    # Calling Custom Function to get queried Data DF
    #Queried_Data_DF = AppFuncs.query_sensor_data(URI, Sensor_Name_DropDown_Value, recording_id=Recording_ID_DropDown_Value, sensor_location=Sensor_Position_DropDown_Value, 
    #                 start_timestamp=Start_Datetime_Input_Unix, end_timestamp=End_Datetime_Input_Unix, nested_field=Sensor_Field_DropDown_Value)

    Queried_Data_DF = AppFuncs.query_collection(
                                        URI,
                                        Sensor_Name_DropDown_Value,
                                        field=Sensor_Field_DropDown_Value,
                                        recording_id=Recording_ID_DropDown_Value,
                                        sensor_location=Sensor_Position_DropDown_Value,
                                        start_timestamp=Start_Datetime_Input_Unix,
                                        end_timestamp=End_Datetime_Input_Unix
                                    )

    # Saving Queried_Data_DF in the App DATA_DF
    global DATA_DF

    DATA_DF = pd.DataFrame()

    DATA_DF = Queried_Data_DF

    # Getting SubField_List
    SubField_List = [col for col in Queried_Data_DF.columns if col != "timestamp"]

    # Calling Custom Function to get Statistics
    Queried_Data_Statistics_DF = AppFuncs.query_statistics(Queried_Data_DF)

    # Getting DataTable_Data, DataTable_Columns
    DataTable_Data = Queried_Data_Statistics_DF.reset_index().to_dict("records")

    DataTable_Columns = [{"name": col, "id": col} for col in Queried_Data_Statistics_DF.reset_index().columns]

    return DataTable_Data, DataTable_Columns, SubField_List

## 7 - Plot Data Time Series (Button) Interaction -> Polpulates Data_TimeSeries_Plot (Figure Plot) 
@app.callback(
    Output(component_id='Data_TimeSeries_Plot', component_property='figure'),
    Input(component_id = 'Plot_Button', component_property = 'n_clicks'),  
    State(component_id='Subfield_DropDown', component_property='value'),
    prevent_initial_call=False
)
def Plot_Button_Interaction(n_clicks, subfield_dropdown_value):
    # Ensure DATA_DF exists and is a DataFrame

    global DATA_DF

    if DATA_DF is None or DATA_DF.empty:
        return px.line(title="No Data Available")

    # Convert 'timestamp' from UNIX time to readable datetime format
    DATA_DF["timestamp"] = pd.to_datetime(DATA_DF["timestamp"], unit='ms')

    # Check if Subfield_DropDown_Value is a list
    if not isinstance(subfield_dropdown_value, list):
        subfield_dropdown_value = [subfield_dropdown_value]  # Convert to a list if a single value is passed

    # Ensure all selected columns exist in the DataFrame
    valid_columns = [col for col in subfield_dropdown_value if col in DATA_DF.columns]
    if not valid_columns:
        return px.line(title="Selected columns are not in the dataset")

    # Plot using Plotly Express
    fig = px.line(
        DATA_DF,
        x="timestamp",
        y=valid_columns,
        labels={"timestamp": "Timestamp"} | {col: col for col in valid_columns},
        title="Data Time Series Plot"
    )

    return fig

#########################################################################################################
# Running the App
#########################################################################################################

if __name__ == '__main__': 
    app.run_server(port=4050)