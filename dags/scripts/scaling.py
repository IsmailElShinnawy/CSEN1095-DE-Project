from sklearn.preprocessing import StandardScaler
from sklearn.preprocessing import MinMaxScaler

def apply_z_score_scaling(df, column):
  result = StandardScaler().fit_transform(df[[column]])
  return result

def apply_min_max_sclaing(df, column):
  scaler = MinMaxScaler()
  scaled_data = scaler.fit_transform(df[[column]])
  return scaled_data

def scale_data(df):
    df['longitude'] = apply_min_max_sclaing(df,"longitude")
    df['latitude'] = apply_min_max_sclaing(df,"latitude")
    df['location_northing_osgr'] = apply_min_max_sclaing(df,"location_northing_osgr")
    df['location_easting_osgr'] = apply_min_max_sclaing(df,"location_easting_osgr")
