import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from steps.churn import create_table, extract, transform, load
from steps.messages import send_telegram_success_message, send_telegram_failure_message

# Определение DAG с контекстным менеджером
with DAG(
    dag_id='alt_prepare_churn_dataset',
    schedule_interval='@once',
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    tags=["ETL"],
    on_success_callback=send_telegram_success_message,  # Успешное выполнение
    on_failure_callback=send_telegram_failure_message   # Ошибка выполнения
) as dag:

    # Создание таблицы
    create_table_task = PythonOperator(
        task_id='create_table',
        python_callable=create_table  # Функция из модуля churn.py
        )

    # Извлечение данных
    extract_task = PythonOperator(
        task_id='extract_data',
        python_callable=extract,  # Функция из модуля churn.py
    )

    # Преобразование данных
    transform_task = PythonOperator(
        task_id='transform_data',
        python_callable=transform,  # Функция из модуля churn.py
    )

    # Загрузка данных
    load_task = PythonOperator(
        task_id='load_data',
        python_callable=load,  # Функция из модуля churn.py
    )

    # Определение порядка выполнения задач
    create_table_task >> extract_task >> transform_task >> load_task
