def hanle_missing_data(df):
    df.drop_duplicates(subset=[ 'first_road_number', 'date', 'time','number_of_vehicles'])