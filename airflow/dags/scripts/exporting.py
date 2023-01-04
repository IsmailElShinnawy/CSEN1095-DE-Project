def write_df_to_parquet_file(df, file_name):
    file_name = file_name + ".parquet"
    df.to_parquet(file_name, index = False)

def write_df_to_csv_file(df, file_name):
    file_name = file_name + ".csv"
    df.to_csv(file_name, index = False)