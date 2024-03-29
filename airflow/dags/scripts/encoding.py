import pandas as pd
from sklearn import preprocessing
import numpy as np

def label_encode(df,columns):
    for column in df.columns:
        if  column in columns :
            df[column] = preprocessing.LabelEncoder().fit_transform(df[column])

def ordinal_encode(df,columns,mappings):
    for column in df.columns:
        if column in columns:
            currentMap = mappings[column]
            df[column] = df[column].map(currentMap)

def one_hot_encode(df,columns):    
    for column in columns:
        labels= df[column].unique()
        for label in labels:
            df[column + '_' + str(label)] = np.where(df[column] == label, 1, 0)

def calculate_top_categories(df, variable, how_many):
    return [
        x for x in df[variable].value_counts().sort_values(
            ascending=False).head(how_many).index
    ]

def one_hot_encode_top_categories(df, variable, top_x_labels):
    for label in top_x_labels:
        df[variable + '_' + label] = np.where(df[variable] == label, 1, 0)

def encode (df):
    label=['weather_conditions','road_surface_conditions','light_conditions', 'special_conditions_at_site','carriageway_hazards']
    one_hot=['urban_or_rural_area','did_police_officer_attend_scene_of_accident','trunk_road_flag']
    one_hot_top_cat={"pedestrian_crossing_physical_facilities" : 3}
    ordinal=['accident_severity']
    mapping={'accident_severity': {'Slight': 0, 'Serious': 1, 'Fatal': 2}}
    if(len(label) > 0):
        label_encode(df, label)

    if(len(one_hot) > 0): 
        one_hot_encode(df, one_hot)

    for column in df.columns:
        if column in one_hot_top_cat:
            topN = one_hot_top_cat.get(column)
            top_x_cat = calculate_top_categories(df, column, topN)
            one_hot_encode_top_categories(df, column, top_x_cat)
            
    if (len(ordinal) > 0):
        ordinal_encode(df, ordinal, mapping)
