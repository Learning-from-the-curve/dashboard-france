import numpy as np 
import pandas as pd
from pickle_functions import unpicklify
import plotly.graph_objects as go
import dash_core_components as dcc
import dash_html_components as html
import dash_bootstrap_components as dbc
import dash_table as dt
from dash.dependencies import Input, Output, State
from pickle_functions import unpicklify
from process_functions import write_log

def list_item(opening, data, ending):
    '''
    input: 
    info data about a statistic for a country
    a string describing it
    a string of eventual text after data
    output: 
    if the data is valid returns an item, otherwise nothing
    '''
    if pd.isna(data) or data == 'None' or data == 0:
        return
    else:
        return dbc.ListGroupItemText(f'{opening}{data}{ending}')

def gen_map(map_data,geo):
    '''
    Function to generate and plot the world map with the # of confirmed cases for each country as the Z parameter
    '''
    mapbox_access_token = 'pk.eyJ1IjoiYnJpdmlkbzkxIiwiYSI6ImNrYjQ5bHo2MTBkancyeXBiYzY5ZXdvbWYifQ.S22SYHHVCJgA-1Cq8dGu7A'
    zoom = 4.7
    lat = 46.1313
    lon = 3.1884
    #print([f"Province: {map_data.iloc[indice]['denominazione_provincia']} <br>Number of cases: {int(map_data.iloc[indice]['totale_casi']):,}" for indice in range(len(map_data['denominazione_provincia']))])
    return {
        "data": [{
            "type": "choroplethmapbox",  #specify the type of data to generate
            "locations": list(map_data['maille_code']),
            "geojson": geo,
            "featureidkey": 'properties.code',
            "z": np.log(map_data['total_cases']),
            "hoverinfo": "text",         
            "hovertext": [f"Province: {map_data.iloc[indice]['maille_nom']} <br>Number of cases: {int(map_data.iloc[indice]['total_cases']):,}" for indice in range(len(map_data['maille_nom']))],
            'colorbar': dict(thickness=20, ticklen=3),
            'colorscale': 'Geyser',
            'autocolorscale': False,
            'showscale': False,
        },
        ],
        "layout":{
            'paper_bgcolor': 'white',
            'height': 660,
            'margin': {
                'l':0,
                'r':0,
                't':0,
                'b':0,
            },
            'hovermode':"closest",
            'mapbox': {
                'accesstoken': mapbox_access_token,
                'style':'mapbox://styles/mapbox/dark-v10',
                'center':{                    
                    'lon': lon,
                    'lat': lat,
                },
                'zoom': zoom,
            },
        }    
    }

def draw_singleCountry_Scatter(df):
    '''
    Function to generate and plot a scatterplot for confirmed/deaths with linear or log scale for the selected countries
    '''
    fig = go.Figure()
    stats = df.columns.drop(['date'])
    x = df['date']
    for stat in stats:
            fig.add_trace(go.Scatter(x =  x, y = df[stat],
                                mode='lines+markers',
                                name=stat,
                                line=dict(width=3), 
                                marker = dict(size = 3, line = dict(width = 1,color = 'DarkSlateGrey')), 
                                hoverinfo = "text",
                                hovertext = [f"{stat}: {df.iloc[indice][stat]} <br>Date: {x.iloc[indice]}" for indice in range(len(df))]))
            fig.update_xaxes(tickformat = '%d %B (%a)<br>%Y')
            fig.update_yaxes(tickformat = ',')
    fig.update_layout(title= f'National Cumulative Statistics')

    fig.update_layout(
        hovermode='closest',
        legend=dict(
            x = 0, 
            y=-0.3, 
            orientation = 'h',
            traceorder="normal",
            font=dict(
                family="sans-serif",
                size=12,
                ),
            borderwidth=0,
        ),
        margin=dict(l=0, r=0, t=65, b=0),
        #height=350,
        yaxis = {'type': 'linear' },
        plot_bgcolor = 'white',
        paper_bgcolor = 'white',
        xaxis = dict(
            tickangle = -45
        )
    )
    fig.update_xaxes(showgrid=True, gridwidth=1, gridcolor='lightgrey')
    fig.update_yaxes(showgrid=True, gridwidth=1, gridcolor='lightgrey')
    fig.update_yaxes(zeroline=True, zerolinewidth=2, zerolinecolor='black')

    return fig

def draw_stats(df, selected_region):
    '''
    Function to generate and plot a scatterplot for mortality rate/Share of infected population/Growth rate confirmed cases/Growth rate deaths
    with date or days scale for the selected countries
    '''
    fig = go.Figure()
    stats = df.columns.drop(['date','Region'])
    x = df['date']
    dates = df.loc[df['Region'] == selected_region,'date']
    for stat in stats:
        y = list(df.loc[df['Region'] == selected_region,stat])
        x = [x for x in range(len(df.loc[df['Region'] == selected_region,stat]))]
        fig.add_trace(go.Scatter(x =  x, y = y,
                                mode='lines+markers',
                                name=stat,
                                line=dict(width=3), marker = dict(size = 3, line = dict(width = 1,color = 'DarkSlateGrey')), hoverinfo = "text",
                                hovertext = [f"{stat}: {y[indice]} <br>Date: {dates.iloc[indice]} <br>Days: {x[indice]}" for indice in range(len(y))]))
    fig.update_layout(title= 'Cumulative Data')

    fig.update_layout(
        hovermode='closest',
        legend=dict(
            x = 0, 
            y=-0.3, 
            orientation = 'h',
            traceorder="normal",
            font=dict(
                family="sans-serif",
                size=12,
            ),
            borderwidth=0,
            #x=0,
            #y=-0.4,
            #orientation="h"
        ),
        plot_bgcolor = 'white',
        paper_bgcolor = 'white',
        margin=dict(l=0, r=0, t=65, b=0),
    )
    fig.update_xaxes(showgrid=True, gridwidth=1, gridcolor='lightgrey')
    fig.update_yaxes(showgrid=True, gridwidth=1, gridcolor='lightgrey')
    fig.update_yaxes(zeroline=True, zerolinewidth=2, zerolinecolor='black')

    return fig
def draw_share(df, selected_region, pop):
    '''
    Function to generate and plot a scatterplot for mortality rate/Share of infected population/Growth rate confirmed cases/Growth rate deaths
    with date or days scale for the selected countries
    '''
    fig = go.Figure()
    stats = df.columns.drop(['date','Region'])
    x = df['date']
    dates = df.loc[df['Region'] == selected_region,'date']
    dividend = int(pop.loc[pop['Region']== selected_region, 'Value'])
    for stat in stats:
        y = list(df.loc[df['Region'] == selected_region,stat])
        y = [n / dividend for n in y]
        x = [x for x in range(len(df.loc[df['Region'] == selected_region,stat]))]
        fig.add_trace(go.Scatter(x =  x, y = y,
                                mode='lines+markers',
                                name=stat,
                                line=dict(width=3), marker = dict(size = 3, line = dict(width = 1,color = 'DarkSlateGrey')), hoverinfo = "text",
                                hovertext = [f"{stat}: {y[indice]*100:.3f}% <br>Date: {dates.iloc[indice]} <br>Days: {x[indice]}" for indice in range(len(y))]))
    fig.update_layout(title= 'Cumulative Share')

    fig.update_layout(
        hovermode='closest',
        legend=dict(
            x = 0, 
            y=-0.3, 
            orientation = 'h',
            traceorder="normal",
            font=dict(
                family="sans-serif",
                size=12,
            ),
            borderwidth=0,
            #x=0,
            #y=-0.4,
            #orientation="h"
        ),
        plot_bgcolor = 'white',
        paper_bgcolor = 'white',
        margin=dict(l=0, r=0, t=65, b=0),
    )
    fig.update_xaxes(showgrid=True, gridwidth=1, gridcolor='lightgrey')
    fig.update_yaxes(showgrid=True, gridwidth=1, gridcolor='lightgrey')
    fig.update_yaxes(zeroline=True, zerolinewidth=2, zerolinecolor='black')

    return fig
