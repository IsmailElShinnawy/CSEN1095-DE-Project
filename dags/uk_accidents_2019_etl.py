from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator

import pandas as pd

from scripts.missing_data import impute_missing_data
from scripts.duplicate_data import handle_duplicate_data
from scripts.outliers import handle_outliers
from scripts.discretization import discretize_dates
from scripts.encoding import encode
from scripts.scaling import scale_data
from scripts.augmentation import augment_df
from scripts.exporting import write_df_to_csv_file

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    'start_date': days_ago(2),
    "retries": 1,
}

def extract_transform_load(filename):
    df = pd.read_csv(filename, index_col = 0, parse_dates = ['date'], na_values = ["Data missing or out of range", -1])
    impute_missing_data(df)
    handle_duplicate_data(df)
    handle_outliers(df)
    discretize_dates(df)
    encode(df)
    scale_data(df)
    augment_df(df)
    write_df_to_csv_file(df, 'uk_accidents_2019_transformed')

with DAG(
    dag_id = 'uk_accidents_2019_etl_pipeline',
    schedule_interval = '@once',
    default_args = default_args,
    description = 'UK Accidents 2019 ETL Pipeline',
    tags = ['uk-accidents-2019-pipeline'],
) as dag:
    extract_transform_load_task = PythonOperator(
        task_id = 'extract_transform_load_task',
        python_callable = extract_transform_load,
        op_kwargs={
            "filename": '/opt/airflow/data/2019_Accidents_UK.csv'
        },
    )