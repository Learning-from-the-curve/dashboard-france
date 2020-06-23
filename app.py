import datetime
import json
import dash
import pickle
import os
from pathlib import Path
import plotly.graph_objects as go
import dash_core_components as dcc
import dash_html_components as html
import dash_bootstrap_components as dbc
import dash_table as dt
from dash.dependencies import Input, Output, State
import numpy as np
import pandas as pd
import dash_bootstrap_components as dbc
# Custom functions
from layout_functions import gen_map, draw_singleCountry_Scatter, draw_stats, draw_share, list_item
from pickle_functions import unpicklify

#####################################################################################################################################
# Boostrap CSS and font awesome . Option 1) Run from codepen directly Option 2) Copy css file to assets folder and run locally
#####################################################################################################################################
external_stylesheets = [dbc.themes.FLATLY]

#Insert your javascript here. In this example, addthis.com has been added to the web app for people to share their webpage

app = dash.Dash(__name__, external_stylesheets = external_stylesheets)

app.title = 'COVID-19 - France dashboard'

flask_app = app.server

config = {'displayModeBar': False}

#for heroku to run correctly
server = app.server

#############################################################################
# UNPICKLIFICATION TIME - load the datasets in the variables
#############################################################################

pickles_list = [
    'tot_nazione_ospedalizzati',
    'tot_nazione_dimessi_guariti',
    'tot_nazione_casi',
    'tot_nazione_deceduti',
    'tot_regioni_ospedalizzati',
    'tot_regioni_dimessi_guariti',
    'tot_regioni_casi',
    'tot_regioni_deceduti',
    'tab_right_df',
    'tot_province_casi',
    'sorted_regioni_casi',
    'sorted_regioni_deceduti',
    'sorted_regioni_ospedalizzati',
    'sorted_regioni_dimessi_guariti',
    'geo',
    'tot_nazione',
    'tot_regioni',
    'pop',
]

tot_nazione_ospedalizzati = unpicklify(pickles_list[0])
tot_nazione_dimessi_guariti = unpicklify(pickles_list[1])
tot_nazione_casi = unpicklify(pickles_list[2])
tot_nazione_deceduti = unpicklify(pickles_list[3])
tot_regioni_ospedalizzati = unpicklify(pickles_list[4])
tot_regioni_dimessi_guariti = unpicklify(pickles_list[5])
tot_regioni_casi = unpicklify(pickles_list[6])
tot_regioni_deceduti = unpicklify(pickles_list[7])
tab_right_df = unpicklify(pickles_list[8])
tot_province_casi = unpicklify(pickles_list[9])
sorted_regioni_casi = unpicklify(pickles_list[10])
sorted_regioni_deceduti = unpicklify(pickles_list[11])
sorted_regioni_ospedalizzati = unpicklify(pickles_list[12])
sorted_regioni_dimessi_guariti = unpicklify(pickles_list[13])
geo = unpicklify(pickles_list[14])
tot_nazione = unpicklify(pickles_list[15])
tot_regioni = unpicklify(pickles_list[16])
pop = unpicklify(pickles_list[17])

##############################################################
#FIXME to transform into functions 
daily_regioni_casi = pd.DataFrame(columns=['Region', 'total_cases'])
daily_regioni_deceduti = pd.DataFrame(columns=['Region', 'deceased'])

for count, regione in enumerate(set(tot_regioni_casi['Region'])):
    daily_regioni_casi.loc[count] = [regione, int(tot_regioni_casi.loc[tot_regioni_casi["Region"]==regione, "total_cases"][-1:])-int(tot_regioni_casi.loc[tot_regioni_casi["Region"]==regione, "total_cases"][-2:-1])]

for count, regione in enumerate(set(tot_regioni_deceduti['Region'])):
    daily_regioni_deceduti.loc[count] = [regione, int(tot_regioni_deceduti.loc[tot_regioni_deceduti["Region"]==regione, 'deceased'][-1:])-int(tot_regioni_deceduti.loc[tot_regioni_deceduti["Region"]==regione, 'deceased'][-2:-1])]
    
#print(daily_regioni_casi)

sorted_daily_regioni_casi = daily_regioni_casi.copy().sort_values(by=['total_cases'], ascending = False).set_index('Region')
sorted_daily_regioni_deceduti = daily_regioni_deceduti.copy().sort_values(by=['deceased'], ascending = False).set_index('Region')
    
#print()


################################################################
#tab_casi_left
tab_casi_left = dbc.Card(
    html.Div([
        html.Ul([
            html.Li([
                html.Div([
                        dbc.ListGroupItem([
                            html.Span([f'{regione} '], className = "spanCountryName"),
                            html.Span([f'{int(tot_regioni_casi.loc[tot_regioni_casi["Region"]==regione, "total_cases"][-1:]):,d}'], className = "spanConfirmed"),
                        ], 
                        className="items"
                        ) for regione in sorted_regioni_casi.index
                ],
                className='media-body'
                ),
            ],
            className='media'
            ),   
        ],
        className='list-unstyled'
        ),
    ],
     
    className="tabcard overflow-auto"
    ),
className="border-0",
)

#tab_deceduti_left
tab_deceduti_left = dbc.Card(
    html.Div([
        html.Ul([
            html.Li([
                html.Div([
                        dbc.ListGroupItem([
                            html.Span([f'{regione} '], className = "spanCountryName"),
                            html.Span([f'{int(tot_regioni_deceduti.loc[tot_regioni_deceduti["Region"]==regione,"deceased"][-1:]):,d}'], className = "spanDeaths"),
                        ],
                        className="items"
                        ) for regione in sorted_regioni_deceduti.index
                ],
                className='media-body'
                ),
            ],
            className='media'
            ),   
        ],
        className='list-unstyled'
        ),
    ],
     
    className="tabcard overflow-auto"
    ),
className="border-0",
)

#tab_ospedalizzati_left
tab_ospedalizzati_left = dbc.Card(
    html.Div([
        html.Ul([
            html.Li([
                html.Div([
                        dbc.ListGroupItem([
                            html.Span([f'{regione} '], className = "spanCountryName"),
                            html.Span([f'{int(daily_regioni_casi.loc[daily_regioni_casi["Region"]==regione, "total_cases"]):,d}'], className = "spanConfirmed") 
                        ], 
                        className="items"
                        ) for regione in sorted_daily_regioni_casi.index
                ],
                className='media-body'
                ),
            ],
            className='media'
            ),   
        ],
        className='list-unstyled'
        ),
    ],  
    className="tabcard overflow-auto"
    ),
className="border-0",
)

#tab_dimessi_guariti_left
tab_dimessi_guariti_left = dbc.Card(
    html.Div([
        html.Ul([
            html.Li([
                html.Div([
                        dbc.ListGroupItem([
                            html.Span([f'{regione} '], className = "spanCountryName"),
                            html.Span([f'{int(daily_regioni_deceduti.loc[daily_regioni_deceduti["Region"]==regione, "deceased"]):,d}'], className = "spanDeaths"),
                        ], 
                        className="items"
                        ) for regione in sorted_daily_regioni_deceduti.index
                ],
                className='media-body'
                ),
            ],
            className='media'
            ),   
        ],
        className='list-unstyled'
        ),
    ],
     
    className="tabcard overflow-auto"
    ),
className="border-0",
)

################################################################

################################################################
#tab_nazione_right
tab_nazione_right = dbc.Card(
    html.Div([
        html.Ul([
            html.Li([
                html.Div([
                        dbc.ListGroupItem([
                            list_item(f'{info.replace("_"," ").title()}: ', value, ''),
                        ],
                        className="items"
                        ) for info, value in zip(tab_right_df.columns, tab_right_df.iloc[0])
                ],
                className='media-body'
                ),
            ],
            className='media'
            ),   
        ],
        className='list-unstyled'
        ),
    ],
     
    className="tabr overflow-auto"
    ),
className="border-0",
)


markdown_relevant_info = html.Div([
    html.P([
        "We focus on this dashboard on the French COVID-19 pandemic. This dashboard is part of a larger set of dashboards available ",
        dcc.Link('on our website', href='https://www.learningfromthecurve.net/dashboards/', target="_top"),
    ]),
    html.P([
        "Articles by members of the Learning from the Curve team reporting daily information on COVID-19 are available ",
        dcc.Link('here', href='https://www.learningfromthecurve.net/commentaries/', target="_top"),
    ]),
    html.P([
        "Please, report any bug at the following contact address: ",
        dcc.Link('learningfromthecurve.info@gmail.com', href='mailto:learningfromthecurve.info@gmail.com'),
    ]),
])

markdown_data_info = html.Div([
    html.P([
        "The dashboard is updated daily following new daily releases of data from the data sources listed below.",
    ]),
    html.P([
        "Data source daily updated:",
        html.Ul([
            html.Li(dcc.Link('National, Regions, Departments', href='https://raw.githubusercontent.com/opencovid19-fr/data/master/dist/chiffres-cles.csv', target="_blank"),),
        ])
    ]),
    html.P([
        "Other data:",
        html.Ul([
            html.Li(dcc.Link('Geojson', href='https://github.com/gregoiredavid/france-geojson/blob/master/departements.geojson', target="_blank"),),
            html.Li(dcc.Link('Regional population data', href='https://www.ined.fr/fichier/s_rubrique/159/en_population.region.departement.xls', target="_blank"),),
        ])
    ]),
])


############################
# Bootstrap Grid Layout
############################

app.layout = html.Div([ #Main Container   
    #Header TITLE
    html.Div([
        #Info Modal Button LEFT
        #dbc.Button("Relevant info", id="open-centered-left", className="btn "),
        dbc.ButtonGroup(
            [
                dbc.Button("Home", href="https://www.learningfromthecurve.net/", target="_top", external_link=True, className="py-2"),
                dbc.Button("Dashboards", href="https://www.learningfromthecurve.net/Dashboards/", target="_top", external_link=True, className="py-2"),
            ],
            vertical=True,
            size="sm",
        ),
        #H2 Title
        html.H2(
            children='COVID-19 - France',
            className="text-center",
        ),
        #Info Modal Button RIGHT
        #dbc.Button("Datasets info", id="open-centered-right", className="btn "),
        dbc.ButtonGroup(
            [
                dbc.Button("Info", id="open-centered-left", className="py-2"),
                dbc.Button("Datasets", id="open-centered-right", className="py-2"),
            ],
            vertical=True,
            size="sm",
        ),
        dbc.Modal(
            [
                dbc.ModalHeader("Information on datasets used"),
                dbc.ModalBody(children = markdown_data_info),
                dbc.ModalFooter(
                    dbc.Button(
                        "Close", id="close-centered-right", className="ml-auto"
                    )
                ),
            ],
            id="modal-centered-right",
            centered=True,
        ),
        dbc.Modal(
            [
                dbc.ModalHeader("Relevant information"),
                dbc.ModalBody(children = markdown_relevant_info),
                dbc.ModalFooter(
                    dbc.Button("Close", id="close-centered-left", className="ml-auto")
                ),
            ],
            id="modal-centered-left",
            centered=True,
        ),
    ],
    className="topRow d-flex justify-content-between align-items-center mb-2"
    ),
    
      #First Row CARDS 3333
    dbc.Row([
        dbc.Col([
            #Card 1
            dbc.Card([
                # Card 1 body
                html.H4(children='Confirmed Cases: '),
                html.H2(f"{tot_nazione_casi.iloc[-1, -1]:,d}"),
                html.P('New daily confirmed cases: ' + f'{int(tot_nazione_casi.iloc[-1, -1]-tot_nazione_casi.iloc[-2, -1]):,d}'),
               ],
            className='cards cases'
            ),
        ],
        lg = 3, xs = 12
        ),     
        dbc.Col([
            #Card 2
            dbc.Card([
                # Card 2 body
                    html.H4(children='Deaths: '),
                    html.H2(f"{tot_nazione_deceduti.iloc[-1, -1]:,d}"),
                    html.P('New daily deaths: ' + f"{int(tot_nazione_deceduti.iloc[-1, -1]-tot_nazione_deceduti.iloc[-2, -1]):,d}"),
            ],
            className='cards deaths'
            ),
        ],
        lg = 3, xs = 12
        ),   
        dbc.Col([
            #Card 3
            dbc.Card([
                # Card 3 body
                html.H4(children='In Hospital: '),
                html.H2(f"{tot_nazione_ospedalizzati.iloc[-1, -1]:,d}", className="text-warning"),
                html.P('Daily difference: ' + f"{int(tot_nazione_ospedalizzati.iloc[-1, -1]-tot_nazione_ospedalizzati.iloc[-2, -1]):,d}", className="text-warning"),
            ],
            className='cards'
            ),
        ],
        lg = 3, xs = 12
        ),        
        dbc.Col([
            #Card 4
            dbc.Card([
                # Card 4 body
                html.H4(children='Released from hospital: '),
                html.H2(f"{tot_nazione_dimessi_guariti.iloc[-1, -1]:,d}", className="text-info"),
                html.P('New daily released from hospital: ' + f"{int(tot_nazione_dimessi_guariti.iloc[-1, -1]-tot_nazione_dimessi_guariti.iloc[-2, -1]):,d}", className="text-info"),
             ],
            className='cards'
            ),
        ],
        lg = 3, xs = 12
        ),     
        ],
        className = "midRow d-flex"
        ),

    #Second Row 363
    dbc.Row([
        #Col2 Left
        dbc.Col([
            dbc.Card([
                dbc.Tabs([
                    dbc.Tab(tab_casi_left, label="Cases"),
                    dbc.Tab(tab_deceduti_left, label="Deaths"),
                ],
                className="nav-justified"
                ),
            ],
            className="card my-2 ",
            id="worldStats",
            ),
            dbc.Card([
                dbc.Tabs([
                    dbc.Tab(tab_ospedalizzati_left, label="Daily cases"),
                    dbc.Tab(tab_dimessi_guariti_left, label="Daily deaths")
                ],
                className="nav-justified"
                ),
            ],
            className="card my-2 ",
            id="worldStats_daily",
            )
        ],
        #align = "stretch",
        lg = 3, md = 12 
        ),

    #Col6 Middle
        dbc.Col([
            #Map, Table
            html.Div([
                html.Div([
                    dcc.Graph(id='france_map', figure = gen_map(tot_province_casi, geo), config=config)
                ],
                #className=' h-100',
                id="worldMap",
                ),
            ],
            className='my-2 '
            ),
        ],
        #className="col-md-6 order-md-2"
        lg = 6, xs = 12
        ),

        #Col2 Right
        dbc.Col([
            dbc.Card([
                dbc.Tabs([
                    dbc.Tab(tab_nazione_right, label="National Statistics"),
                ],
                className="nav-justified",
                id = 'info_tab_right'
                )
            ],
            className="items my-2 ",
            id="countriesStats",
            ),
            dbc.Tooltip(children = [
                html.P([
                    "This tab shows a set of statistics for the countries selected in the dropdown menu."
                ],),
                html.P([
                    "All the reported statistics are updated to the current day."
                ],),],
                target="info_tab_right",
                style= {'opacity': '0.9'}
            ),
        ],
        #className= "h-100",
        lg = 3, xs = 12
        ),

    ],
    className = "botRow d-flex"
    ),

    #GRAPH GRID ROW 1
    dbc.Row([
        dbc.Col([
            #GRAPH 1 Line Graph Confirmed
            html.Div([
                html.Div([
                    dcc.Graph(figure = draw_singleCountry_Scatter(tot_nazione),config=config, className='graph-flourish')
                ],
                className='p-1'
                ),
            ],
            className='card my-2 '
            ),
        ],
        lg = 12
        ),

    ]),
    dbc.Row([
        dbc.Col([

            #Variable Dropdown Fatality
            html.Div([
                html.H4(
                    children='Evolution of statistics after first case for each Region',
                    className='text-center my-2',
                ),
                html.Div([
                    dcc.Dropdown(
                        id='variable-dropdown',
                        options=[{'label': i, 'value': i} for i in sorted_regioni_casi.index],
                        multi=False,
                        value = sorted_regioni_casi.index.values[0],
                    ), 
                ],
                className ='card-body text-center'
                ),
            ],
            className='card my-2 '
            ),
        ],
        lg = 12
        )
    ], 
    justify="center"
    ),

    #GRAPH GRID ROW 3
    dbc.Row([
        dbc.Col([
            #GRAPH 5 Line stats
            html.Div([
                html.Div([
                    dcc.Graph(id='line-graph-stats',config=config)
                ],
                className='p-1'
                ),
            ],
            className='card my-2 '
            ),
        ],
        lg = 6, md = 12
        ),
        dbc.Col([
            #GRAPH 6 Line share
            html.Div([
                html.Div([
                    dcc.Graph(id='line-graph-share',config=config)
                ],
                className='p-1'
                ),
            ],
            style={},
            className='card my-2 '
            ),
        ],
        lg = 6, md = 12
        )
    ])

],
className="container-fluid cf py-2"
)

# draw the graph for the selected statistic from mortality rate/Share of infected population/Growth rate confirmed cases/Growth rate deaths
@app.callback(
    [Output('line-graph-stats', 'figure'),
    Output('line-graph-share', 'figure')],
    [Input('variable-dropdown', 'value')])
def line_selection2(dropdown):
    fig1 = draw_stats(df = tot_regioni,  selected_region = dropdown)
    fig2 = draw_share(df = tot_regioni,  selected_region = dropdown, pop = pop)
    return fig1, fig2

# open/close the left modal
@app.callback(
    Output("modal-centered-left", "is_open"),
    [Input("open-centered-left", "n_clicks"), Input("close-centered-left", "n_clicks")],
    [State("modal-centered-left", "is_open")],)
def toggle_modal_left(n1, n2, is_open):
    if n1 or n2:
        return not is_open
    return is_open

# open/close the right modal
@app.callback(
    Output("modal-centered-right", "is_open"),
    [Input("open-centered-right", "n_clicks"), Input("close-centered-right", "n_clicks")],
    [State("modal-centered-right", "is_open")],)
def toggle_modal_right(n1, n2, is_open):
    if n1 or n2:
        return not is_open
    return is_open

if __name__ == '__main__':
   app.run_server(debug=False)
