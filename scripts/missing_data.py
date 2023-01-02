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