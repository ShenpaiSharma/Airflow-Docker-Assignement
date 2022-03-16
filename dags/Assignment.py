from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator

from data_proceesing import data_processing
from create_psql_table import create_table_weather_data
from insert_weather_data import load_weather_data

# initialising the default ags for DAG
default_args = {
    'owner': 'Airflow',
    'start_date': datetime(2022, 3, 16),
    'retries': 1,
    'retry_delay': timedelta(seconds=5)
}

dag = DAG('Airflow_assignment',
          default_args=default_args,
          schedule_interval='0 6 * * *',  # setting the schedule of DAG to run at 6:00 AM
          template_searchpath=['/usr/local/airflow/sql_files'],
          catchup=False)

t1 = PythonOperator(task_id='store_data_in_csv', python_callable=data_processing, dag=dag)

# Using PostgresOperator to create the table with same columns in csv file
t2 = PostgresOperator(task_id='create_postgres_table', postgres_conn_id='postgres_conn', sql="create_table.sql", dag=dag)

# Using PostgresOperator to insert the data from csv file into the postgres table
t3 = PostgresOperator(task_id='insert_into_table', postgres_conn_id='postgres_conn', sql="insert_into_table.sql", dag=dag)

# t2 = PythonOperator(task_id="create_table", python_callable=create_table_weather_data, dag=dag)
#
# t3 = PythonOperator(task_id="read_csv_load_data", python_callable=load_weather_data, dag=dag)

t1 >> t2 >> t3
