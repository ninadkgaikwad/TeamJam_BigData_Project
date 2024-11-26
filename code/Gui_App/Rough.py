## 1 - Connect to Database (Button) Interaction -> Polpulates Select Sensor Name Dropdown
from pymongo import MongoClient
from datetime import datetime

@app.callback(
    Output(component_id='Data_TimeSeries_Plot', component_property='figure'),
    Input(component_id = 'Plot_Button', component_property = 'n_clicks'),  
    State(component_id='Subfield_DropDown', component_property='value'),
    prevent_initial_call=False
)
def Plot_Button_Interaction(n_clicks, subfield_dropdown_value):
    # Ensure DATA_DF exists and is a DataFrame

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


    

## 2 - Select Sensor Name (DropDown) Interaction -> Polpulates Select Recording ID Dropdown


## 3 - Select Recording ID (DropDown) Interaction -> Polpulates Select Sensor Position Dropdown


## 4 - Select Sensor Position (DropDown) Interaction -> Polpulates Select Sensor Field Dropdown


## 5 - Select Sensor Field (DropDown) Interaction -> Polpulates Starting/Ending Data Time of Selected Data Labels


## 6 - Compute Data Statistics (Button) Interaction -> Polpulates Data_Statistics_Table (DashTable) and the Select Data Sub-Field for Plotting (DropDown)


## 7 - Plot Data Time Series (Button) Interaction -> Polpulates Data_TimeSeries_Plot (Figure Plot) 
