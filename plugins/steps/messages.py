from airflow.providers.telegram.hooks.telegram import TelegramHook # импортируем хук телеграма
import os


access_token = os.getenv("TELEGRAM_API")
chat_id = os.getenv("TELEGRAM_CHAT_ID")


def send_telegram_success_message(context): # на вход принимаем словарь со контекстными переменными
    hook = TelegramHook(
                    telegram_conn_id='test',
                    token=access_token,
                    chat_id=chat_id
                    )
    
    dag_id = context['dag'].dag_id
    run_id = context['run_id']
    
    message = f'Исполнение DAG "{dag_id}" с run_id="{run_id}" прошло успешно!'
    
    # Отправляем сообщение в указанный чат
    hook.send_message(
        {'chat_id': chat_id,  # Укажите ваш chat_id
        'text': message}
    )


def send_telegram_failure_message(context):
    hook = TelegramHook(
        telegram_conn_id='test',
        token=access_token,
        chat_id=chat_id
    )

    dag_id = context['dag'].dag_id
    run_id = context['run_id']
    task_id = context['task_instance'].task_id
    
    message = (
        f'Неудачное исполнение DAG "{dag_id}" с run_id="{run_id}". '
        f'Задача "{task_id}" завершилась с ошибкой.'
    )
    
    # Отправляем сообщение в указанный чат
    hook.send_message(
        {'chat_id': chat_id,  # Укажите ваш chat_id
        'text': message}
    )