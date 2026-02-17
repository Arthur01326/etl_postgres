from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
}

def test_connection_with_hook():
    """Тестируем подключение через PostgresHook"""
    hook = PostgresHook(postgres_conn_id='postgres_data_conn')
    connection = hook.get_conn()
    cursor = connection.cursor()
    cursor.execute("SELECT current_database(), current_user, version();")
    result = cursor.fetchone()
    print(f"✅ Подключение успешно!")
    print(f"   База данных: {result[0]}")
    print(f"   Пользователь: {result[1]}")
    print(f"   Версия: {result[2][:50]}...")
    cursor.close()
    connection.close()

with DAG(
    'test_postgres_connection',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:

    # Тест через PostgresOperator
    test_sql = PostgresOperator(
        task_id='test_simple_query',
        postgres_conn_id='postgres_data_conn',
        sql="SELECT 'Hello from Airflow!' as message, NOW() as current_time;"
    )

    # Тест через PythonOperator с Hook
    test_hook = PythonOperator(
        task_id='test_with_hook',
        python_callable=test_connection_with_hook
    )

    test_sql >> test_hook