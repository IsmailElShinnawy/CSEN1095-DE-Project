from scipy import stats
import numpy as np

def get_z_score(df, col_name):
  return np.abs(df[col_name] - df[col_name].mean()) / df[col_name].std()

def handle_outliers(df):
  z = get_z_score(df, 'speed_limit')
  filtered_entries = z < 3
  df = df[filtered_entries]

  z = get_z_score(df, 'number_of_casualties')
  filtered_entries = z < 3
  df = df[filtered_entries]

  z = get_z_score(df, 'number_of_vehicles')
  filtered_entries = z < 3
  df = df[filtered_entries]
  
