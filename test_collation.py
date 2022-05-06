from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator

from test_collation.do_something import do_something

default_args = {
    'owner': 'tri',
    'email_on_failure': False,
    'email_on_retry': False,
    'email': 'admin@localhost.com',
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}


with DAG(
    'test_collation_mysql',
    start_date=datetime(2021, 11, 1),
    schedule_interval=None,
    default_args=default_args,
    catchup=False,
    tags=['test'],
) as dag:
    test_collation = PythonOperator(
        task_id='test_collation',
        python_callable=do_something,
    )

    [
        test_collation,
    ]
