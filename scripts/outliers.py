from scipy import stats
import numpy as np

def handle_outliers(df):
  z = np.abs(stats.zscore(df['speed_limit']))
  filtered_entries = z < 3
  df = df[filtered_entries]

  z = np.abs(stats.zscore(df['number_of_casualties']))
  filtered_entries = z < 3
  df = df[filtered_entries]

  z = np.abs(stats.zscore(df['number_of_vehicles']))
  filtered_entries = z < 3
  df = df[filtered_entries]
  
