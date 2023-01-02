from sklearn.preprocessing import StandardScaler
from sklearn.preprocessing import MinMaxScaler

def apply_ZScore_Scaling(df, column):
  result = StandardScaler().fit_transform(df[[column]])
  return result

def apply_MinMax(df, column):
  scaler = MinMaxScaler()
  scaled_data = scaler.fit_transform(df[[column]])
  return scaled_data

def scale_Data(df):
    df['longitude'] = apply_MinMax(df,"longitude")
    df['latitude'] = apply_MinMax(df,"latitude")
    df['location_northing_osgr'] = apply_MinMax(df,"location_northing_osgr")
    df['location_easting_osgr'] = apply_MinMax(df,"location_easting_osgr")
