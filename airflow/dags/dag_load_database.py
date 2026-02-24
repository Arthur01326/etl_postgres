from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python_operator import PythonOperator

from datetime import datetime, timedelta
import pandas as pd
import logging


# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def load_raw_date():
    # Подключение
    hook = PostgresHook(postgres_conn_id='postgres_data_conn')
    engine = hook.get_sqlalchemy_engine()

    # Загрузка пачками
    chunk_size = 100000

    total_1 = 0
    total_2 = 0

    try:
        logger.info('---Процесс загрузки 1 файла---')
        for chunk in pd.read_csv('/opt/airflow/data/nyc.csv', chunksize=chunk_size):
            chunk['load_date'] = datetime.now()
            chunk.to_sql('raw_nyc', engine, schema='ods', if_exists='append', index=False)
            total_1 += len(chunk)
            logger.info(f'В процессе загрузки {total_1} строк')

        logger.info(f'Загружено {total_1} строк')


        logger.info('---Процесс загрузки 2 файла---')
        for chunk_1 in pd.read_csv('/opt/airflow/data/weather_data.csv', chunksize=chunk_size):
            chunk_1['load_date'] = datetime.now()
            chunk_1.to_sql('raw_weather', engine, schema='ods', if_exists='append', index=False)
            total_2 += len(chunk_1)
            logger.info(f'В процессе загрузки {total_2} строк')

        logger.info(f'Загружено {total_2} строк')

    except Exception as e:
        logger.error(f'Ошибка при загрузке данных: {e}')
        raise



default_args = {
    'owner': 'airflow',
    'start_date': datetime(2026, 2, 18),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
        dag_id='dag_load_database',
        default_args=default_args,
        schedule_interval='@yearly',
        catchup=False,
        tags=['DWH', 'postgres']
) as dag:

    load_raw = PythonOperator(
        task_id='load_raw',
        python_callable=load_raw_date
    )

    insert_date_taxi_trip = PostgresOperator(
        task_id='insert_date_taxi_trip',
        postgres_conn_id='postgres_data_conn',
        sql="""SELECT dds.insert_taxi_trip();"""
    )

    insert_date_weather_nyc = PostgresOperator(
        task_id='insert_date_weather_nyc',
        postgres_conn_id='postgres_data_conn',
        sql="""SELECT dds.insert_weather_nyc();"""
    )

    create_and_insert_dim_date = PostgresOperator(
        task_id='create_and_insert_dim_date',
        postgres_conn_id='postgres_data_conn',
        sql="""SELECT dds.create_and_insert_dim_date();"""
    )

    create_dm_taxi_trip_and_weather_nyc = PostgresOperator(
        task_id='create_dm_taxi_trip_and_weather_nyc',
        postgres_conn_id='postgres_data_conn',
        sql="""SELECT dm.dm_taxi_trip_and_weather_nyc();"""
    )

    load_raw >> insert_date_taxi_trip >> insert_date_weather_nyc >> create_and_insert_dim_date >> create_dm_taxi_trip_and_weather_nyc
