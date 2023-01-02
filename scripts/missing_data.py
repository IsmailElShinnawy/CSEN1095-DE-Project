import pandas as pd

def impute_attribute_using_class_mode(row, impute_attr_name, class_attr_name, df, mode_per_class, global_mode):
    if pd.isna(row[impute_attr_name]):
        mode = mode_per_class[row[class_attr_name]]
        if not isinstance(mode, str):
            if len(mode) == 0: # No mode
                # Get the value from the first row that have the same class
                new_mode = df[df[class_attr_name] == row[class_attr_name]].iloc[0][impute_attr_name]
                # If value found is still NA then we impute using a global mode for the attribute
                if pd.isna(new_mode):
                    mode = global_mode
                else:
                    mode = new_mode
            else: # Feature has multiple modes, so we just take the first one
                mode = mode[0]
        row[impute_attr_name] = mode
    return row[impute_attr_name]

def impute_using_arbitrary_value(df, attr_name, value):
    return df[attr_name].fillna(value)

def get_mode_per_class(df, mode_attr_name, class_attr_name):
    return df.groupby([class_attr_name])[mode_attr_name].agg(pd.Series.mode)

def drop_rows_below_missing_threshold(df, threshold = 0.2):
    mean_na = df.isna().mean()
    columns = df.columns
    subset = []
    for col in columns:
        if(mean_na.loc[col] < threshold):
            subset.append(col)
    if(len(subset) > 0):
        df.dropna(subset=subset, inplace = True)


def impute_missing_data(df):
    df['junction_control'] = impute_using_arbitrary_value(df, 'junction_control', "NOT_A_JUNCTION")
    df['second_road_number'] = impute_using_arbitrary_value(df, 'second_road_number', "NO_SECOND_ROAD")
    df['second_road_class'] = impute_using_arbitrary_value(df, 'second_road_class', "NO_SECOND_ROAD")
    df['second_road_number'] = df['second_road_number'].replace("first_road_class is C or Unclassified. These roads do not have official numbers so recorded as zero ", '0')
    
    trunk_flag_per_road_number = get_mode_per_class(df, 'trunk_road_flag', 'first_road_number')
    GLOBAL_TRUNK_FLAG_MODE = df['trunk_road_flag'].mode()[0]
    df['trunk_road_flag'] = df.apply(impute_attribute_using_class_mode, axis = 1, args = ['trunk_road_flag', 'first_road_number', df, trunk_flag_per_road_number, GLOBAL_TRUNK_FLAG_MODE])

    location_per_district = get_mode_per_class(df, 'lsoa_of_accident_location', 'local_authority_district')
    GLOBAL_LOCATION_MODE = df['lsoa_of_accident_location'].mode()[0]
    df['lsoa_of_accident_location'] = df.apply(impute_attribute_using_class_mode, axis = 1, args = ['lsoa_of_accident_location', 'local_authority_district', df, location_per_district, GLOBAL_LOCATION_MODE])

    weather_conditions_per_location = get_mode_per_class(df, 'weather_conditions', 'lsoa_of_accident_location')
    GLOBAL_WEATHER_CONDITIONS_MODE = df['weather_conditions'].mode()[0]
    df['weather_conditions'] = df.apply(impute_attribute_using_class_mode, axis = 1, args = ['weather_conditions', 'lsoa_of_accident_location', df, weather_conditions_per_location, GLOBAL_WEATHER_CONDITIONS_MODE])

    drop_rows_below_missing_threshold(df)