import warnings
warnings.filterwarnings("ignore")
from datetime import datetime
import pickle
import json
import pandas as pd
import numpy as np
from pathlib import Path
from process_functions import write_log
from pickle_functions import picklify, unpicklify

######################################
# Retrieve data
######################################

# Paths

path_geo = Path.cwd() / 'input'/ 'departements.geojson'

path_france = Path.cwd() / 'input' / 'chiffres-cles.csv'

path_population = Path.cwd() / 'input' / 'france_population_data_2013.csv' #source: https://en.wikipedia.org/wiki/List_of_French_departments_by_population#List_of_departments_by_descending_population

# get data directly from github.
url_france= 'https://raw.githubusercontent.com/opencovid19-fr/data/master/dist/chiffres-cles.csv'

pop = pd.read_csv(path_population)

###########################################################################

#load old data

df_france_backup = pd.read_csv(path_france)
old_df_france = df_france_backup['date']


#load new data

df_france = pd.read_csv(url_france)
new_df_france = df_france['date']


#compute difference of rows and columns
nazione_date_diff = set(new_df_france).symmetric_difference(set(old_df_france))

#print(regioni_date_diff,regioni_name_diff)


#write log and load the backup df if there is new data until the next update
#for nazione
write_log('--- national file check'.upper())

if len(nazione_date_diff) > 1:
    write_log('multiple new dates added: ' + str(nazione_date_diff))
elif len(nazione_date_diff) == 1:
    write_log('new date added: ' + str(nazione_date_diff))
else:
    write_log('no new date added')



df_france.to_csv(path_france, index = None)

#########################################################################################
# Data preprocessing for getting useful data and shaping data compatible to plotly plot
#########################################################################################
# columns names for reference

#['date', 'granularite', 'maille_code', 'maille_nom', 'cas_confirmes', 'cas_ehpad', 'cas_confirmes_ehpad', 'cas_possibles_ehpad', 'deces',
#       'deces_ehpad', 'reanimation', 'hospitalises', 'nouvelles_hospitalisations', 'nouvelles_reanimations', 'gueris',
#       'depistes', 'source_nom', 'source_url', 'source_archive', 'source_type']

#print(df_france)

#formatting dates

df_france['date'] = pd.to_datetime(df_france.date, errors = 'coerce')
df_france = df_france[pd.isnull(df_france.date) == False]
df_france['date'] = df_france['date'].dt.strftime('%Y/%m/%d')
df_france['total_cases'] = df_france.loc[:,['cas_confirmes','cas_confirmes_ehpad']].sum(axis=1)
df_france['deceased'] = df_france.loc[:,['deces','deces_ehpad']].sum(axis=1)
df_france.rename(columns={
    'hospitalises': 'total_hospitalized', 
    'gueris': 'discharged_healed',
    }, inplace=True)


df_nazione = df_france[df_france['granularite'].str.contains("pays")]
tab_right_df2 = df_nazione.drop(['granularite', 'maille_code', 'maille_nom','depistes', 'source_nom', 'source_url', 'source_archive','source_type'], axis=1)
#print(tab_right_df)
tab_right_df2 = tab_right_df2.drop_duplicates(subset=['date'], keep='first')
valid_indexes = tab_right_df2.notna()[::-1].idxmax()
tab_right_df = tab_right_df2[-1:]
for col in tab_right_df.columns:
#    print(tab_right_df[col][valid_indexes[col]])
    try:
        tab_right_df[col] = int(tab_right_df2[col][valid_indexes[col]])
    except:
        tab_right_df[col] = tab_right_df2[col][valid_indexes[col]]

#print(tab_right_df)
#dropping useless columns
df_france = df_france.drop([ 'deces','cas_confirmes','cas_ehpad', 'cas_confirmes_ehpad', 'cas_possibles_ehpad',
       'deces_ehpad', 'reanimation', 'source_nom','source_url','source_archive','source_type',
       'nouvelles_hospitalisations', 'nouvelles_reanimations', 'depistes'], axis=1)


#split df_france in dataframes for nation, regions, provinces AND world
df_regioni = df_france[df_france['granularite'].str.contains("region")]
df_province = df_france[df_france['granularite'].str.contains("departement")]
df_nazione = df_nazione.fillna(0)
df_nazione = df_nazione.drop_duplicates(subset=['date'], keep='first')
df_nazione = df_nazione.drop(['granularite','maille_code','maille_nom'], axis=1)
for col in df_nazione.columns:
    try:
        df_nazione[col] = df_nazione[col].astype(int)
    except:
        pass
df_regioni = df_regioni.drop(['granularite','maille_code'], axis=1)

df_regioni = df_regioni.drop_duplicates(subset=['date','maille_nom'], keep='first')
df_regioni.rename(columns={
    'maille_nom': 'Region'
    }, inplace=True)
df_regioni = df_regioni.fillna(0)

for col in df_regioni.columns:
    try:
        df_regioni[col] = df_regioni[col].astype(int)
    except:
        pass
#print(df_regioni)


# for national counts


tot_nazione_ospedalizzati = df_nazione[['date', 'total_hospitalized']]
tot_nazione_dimessi_guariti = df_nazione[['date', 'discharged_healed']]
tot_nazione_casi = df_nazione[['date', 'total_cases']]
tot_nazione_deceduti = df_nazione[['date', 'deceased']]
tot_nazione = df_nazione[['date','total_hospitalized', 'discharged_healed', 'total_cases', 'deceased']]
print(tot_nazione_casi)

# for tab card left and plots

tot_regioni = df_regioni[['date', 'Region', 'total_hospitalized', 'discharged_healed', 'total_cases', 'deceased']]

tot_regioni = tot_regioni[~(df_regioni['Region'] == 'La Réunion')] 
tot_regioni = tot_regioni[~(df_regioni['Region'] == 'Guadeloupe')] 
tot_regioni = tot_regioni[~(df_regioni['Region'] == 'Martinique')] 
tot_regioni = tot_regioni[~(df_regioni['Region'] == 'Guyane')] 
tot_regioni = tot_regioni[~(df_regioni['Region'] == 'Auvergne-Rhône-Alpes')] 
tot_regioni = tot_regioni[~(df_regioni['Region'] == 'Mayotte')] 

tot_regioni = tot_regioni.reset_index(drop=True)

tot_regioni.drop(tot_regioni[tot_regioni['total_cases'] < 1].index, inplace=True)
tot_regioni_ospedalizzati = tot_regioni[['date','Region', 'total_hospitalized']]
tot_regioni_dimessi_guariti = tot_regioni[['date','Region', 'discharged_healed']]
tot_regioni_casi = tot_regioni[['date','Region', 'total_cases']]
tot_regioni_deceduti = tot_regioni[['date','Region', 'deceased']]
#print(tot_regioni_casi)

#sorted versions
sorted_regioni_casi = tot_regioni_casi.copy().groupby('Region').last()
sorted_regioni_casi = tot_regioni_casi.copy().groupby('Region').last()

sorted_regioni_casi = sorted_regioni_casi.drop(['date'], axis=1)
sorted_regioni_casi = sorted_regioni_casi.sort_values(by=['total_cases'], ascending = False)

sorted_regioni_deceduti = tot_regioni_deceduti.copy().groupby('Region').last()
sorted_regioni_deceduti = sorted_regioni_deceduti.drop(['date'], axis=1)
sorted_regioni_deceduti = sorted_regioni_deceduti.sort_values(by=['deceased'], ascending = False)

sorted_regioni_ospedalizzati = tot_regioni_ospedalizzati.copy().groupby('Region').last()
sorted_regioni_ospedalizzati = sorted_regioni_ospedalizzati.drop(['date'], axis=1)
sorted_regioni_ospedalizzati = sorted_regioni_ospedalizzati.sort_values(by=['total_hospitalized'], ascending = False)

sorted_regioni_dimessi_guariti = tot_regioni_dimessi_guariti.copy().groupby('Region').last()
sorted_regioni_dimessi_guariti = sorted_regioni_dimessi_guariti.drop(['date'], axis=1)
sorted_regioni_dimessi_guariti = sorted_regioni_dimessi_guariti.sort_values(by=['discharged_healed'], ascending = False)

# for tab card right
tab_right_df.rename(columns={
    'cas_confirmes': 'cases',
    'cas_ehpad':'nursing home cases', 
    'cas_confirmes_ehpad': 'nursing home confirmed cases', 
    'cas_possibles_ehpad': 'nursing home possible cases', 
    'deces': 'deceased',
    'deces_ehpad': 'nursing home deceased', 
    'reanimation': 'intensive care', 
    'hospitalises':'total_hospitalized',
    'nouvelles_hospitalisations':'new hospitalizations', 
    'nouvelles_reanimations':'new intensive care', 
    'gueris':'discharged_healed',
    #'depistes': 'screened',
    }, inplace=True)

#for province map
tot_province_casi = df_province[['maille_code','maille_nom','total_cases']]
tot_province_casi['maille_code'] = tot_province_casi['maille_code'].str[4:]
tot_province_casi = tot_province_casi.groupby(['maille_code', 'maille_nom']).sum().reset_index()
#print(tot_province_casi)



with open(path_geo, encoding='latin-1') as f:
    geo = json.load(f)


####################################################################

#store the pickles for all the df needed
dataframe_list = [
    [tot_nazione_ospedalizzati, 'tot_nazione_ospedalizzati'],
    [tot_nazione_dimessi_guariti, 'tot_nazione_dimessi_guariti'],
    [tot_nazione_casi, 'tot_nazione_casi'],
    [tot_nazione_deceduti, 'tot_nazione_deceduti'],
    [tot_regioni_ospedalizzati, 'tot_regioni_ospedalizzati'],
    [tot_regioni_dimessi_guariti, 'tot_regioni_dimessi_guariti'],
    [tot_regioni_casi, 'tot_regioni_casi'],
    [tot_regioni_deceduti, 'tot_regioni_deceduti'],
    [tab_right_df, 'tab_right_df'],
    [tot_province_casi, 'tot_province_casi'],
    [sorted_regioni_casi, 'sorted_regioni_casi'],
    [sorted_regioni_deceduti, 'sorted_regioni_deceduti'],
    [sorted_regioni_ospedalizzati, 'sorted_regioni_ospedalizzati'],
    [sorted_regioni_dimessi_guariti, 'sorted_regioni_dimessi_guariti'],
    [geo,'geo'],
    [tot_nazione, 'tot_nazione'],
    [tot_regioni, 'tot_regioni'],
    [pop, 'pop'],

]

for dataframe, name in dataframe_list:
    picklify(dataframe, name)
