from sklearn.preprocessing import StandardScaler
from sklearn.preprocessing import MinMaxScaler
def applyZScoreSaling(df, column):
  result = StandardScaler().fit_transform(df[[column]])
  return result

def applyMinMax(df, column):
  scaler = MinMaxScaler()
  scaled_data = scaler.fit_transform(df[[column]])
  return scaled_data