from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator

import pandas as pd

from sqlalchemy import create_engine

import dash
import dash_core_components as dcc
import dash_html_components as html
import plotly.express as px

import os

from scripts.missing_data import impute_missing_data
from scripts.duplicate_data import handle_duplicate_data
from scripts.outliers import handle_outliers
from scripts.discretization import discretize_dates
from scripts.encoding import encode
from scripts.scaling import scale_data
from scripts.augmentation import augment_df
from scripts.exporting import write_df_to_csv_file

from scripts.web_scraping import augment_df_with_population_data

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    'start_date': days_ago(2),
    "retries": 1,
}

def web_scrape():
    if(os.path.exists("/opt/airflow/data/uk_accidents_2019_transformed_and_augmented.csv")):
        return

    df = pd.read_csv('/opt/airflow/data/uk_accidents_2019_transformed.csv')
    augment_df_with_population_data(df)
    write_df_to_csv_file(df, '/opt/airflow/data/uk_accidents_2019_transformed_and_augmented') 

def extract_transform_load(filename):
    if(os.path.exists("/opt/airflow/data/uk_accidents_2019_transformed.csv")):
        return
    df = pd.read_csv(filename, index_col = 0, parse_dates = ['date'], na_values = ["Data missing or out of range", -1])
    impute_missing_data(df)
    handle_duplicate_data(df)
    handle_outliers(df)
    discretize_dates(df)
    encode(df)
    scale_data(df)
    augment_df(df)
    write_df_to_csv_file(df, '/opt/airflow/data/uk_accidents_2019_transformed')

def load_to_postgres(filename, lookup_filename):
    df = pd.read_csv(filename)
    lookup = pd.read_csv(lookup_filename)
    
    engine = create_engine('postgresql://root:root@pgdatabase:5432/uk_accidents_2019')
    
    try:
        df.to_sql('UK_Accidents_2019', con = engine, if_exists = 'fail', index = False)
        lookup.to_sql('lookup_table', con = engine, if_exists = 'fail', index = False)
    except:
        pass

def create_dashboard(filename):
    df = pd.read_csv(filename)

    # Number of accidents per light condition
    fig = px.histogram(df, x = 'light_conditions', title = 'Number of accidents per light condition')
    fig.update_layout(
        xaxis_title = 'Light Condition',
        yaxis_title = 'Number of Accidents'
    )

    # Number of accidents per weekend and weekday
    fig2 = px.histogram(df, x = 'week_end', title = 'Number of accidents per weekend and weekday')
    fig2.update_layout(
        xaxis_title = 'Type of Day',
        yaxis_title = 'Number of Accidents'
    )

    # Number of accidents per road type
    fig3 = px.histogram(df, x = 'road_type', title = 'Number of accidents per road type')
    fig3.update_layout(
        xaxis_title = 'Road Type',
        yaxis_title = 'Number of Accidents'
    )

    # Number of accidents per weather condition
    fig4 = px.histogram(df, x = 'weather_conditions', title = 'Number of accidents per weather condition')
    fig4.update_layout(
        xaxis_title = 'Weather Condition',
        yaxis_title = 'Number of Accidents'
    )

    # Number of accidents per accident severity
    fig5 = px.histogram(df, x = 'accident_severity', title = 'Number of accidents per accident severity')
    fig5.update_layout(
        xaxis_title = 'Accident Severity',
        yaxis_title = 'Number of Accidents'
    )
    
    app = dash.Dash(__name__)

    app.layout = html.Div(children = [
        html.H1(children = 'UK Accidents 2019 Dashboard'),
        html.H2(children = 'Number of accidents per light condition'),
        dcc.Graph(figure = fig),
        html.H2(children = 'Number of accidents per type of day'),
        dcc.Graph(figure = fig2),
        html.H2(children = 'Number of accidents per road type'),
        dcc.Graph(figure = fig3),
        html.H2(children = 'Number of accidents per weather condition'),
        dcc.Graph(figure = fig4),
        html.H2(children = 'Number of accidents per accident severity'),
        dcc.Graph(figure = fig5)
    ])

    app.run_server(host='0.0.0.0', port=8050, debug=False)


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

    web_scraping_task = PythonOperator(
        task_id = 'augment_df_task',
        python_callable = web_scrape,
        op_kwargs={
            "filename": '/opt/airflow/data/uk_accidents_2019_transformed.csv'
        }
    )

    load_to_postgres_task = PythonOperator(
        task_id = 'load_to_postgres_task',
        python_callable = load_to_postgres,
        op_kwargs={
            "filename": '/opt/airflow/data/uk_accidents_2019_transformed_and_augmented.csv',
            "lookup_filename": '/opt/airflow/data/lookup_table.csv'
        }
    )

    create_dashboard_task = PythonOperator(
        task_id = 'create_dashboard_task',
        python_callable = create_dashboard,
        op_kwargs={
            "filename": '/opt/airflow/data/uk_accidents_2019_transformed_and_augmented.csv'
        }
    )

    extract_transform_load_task >> web_scraping_task >> load_to_postgres_task >> create_dashboard_task