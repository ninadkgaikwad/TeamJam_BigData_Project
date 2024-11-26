#########################################################################################################
# Importing Desired Modules
#########################################################################################################

from dash import Dash, dcc, html, Input, Output, State, dash_table
import dash_bootstrap_components as dbc
import plotly.express as px

#########################################################################################################
# Importing Custom Modules
#########################################################################################################

import App_Func_Module as AppFuncs

#########################################################################################################
# Defining App Constants
#########################################################################################################

# Database Location
URI = "mongodb://localhost:27017/"

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
                            id = 'DateTime_Range_Label',
                            ),  
            
            ], xs = 6, sm = 6, md = 6, lg = 3, xl = 3), # width = 12 

        dbc.Col([
            
            html.Label("Ending Date Time of Selected Data:"),  
            
            ], xs = 6, sm = 6, md = 6, lg = 3, xl = 3), # width = 12           
        
        dbc.Col([
            
            html.Label("mm/dd/yyyy HH:MM:SS",
                            id = 'DateTime_Range_Label',
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
                                data={},
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
            
            html.Label("2. Select Data Sub-Field for Plotting:",
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


#########################################################################################################
# Running the App
#########################################################################################################

if __name__ == '__main__': 
    app.run_server(port=4050)