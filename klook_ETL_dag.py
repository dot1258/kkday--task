import datetime as dt
from airflow import DAG
from airflow.operators.python import PythonOperator
from klook_ETL import klook_scraper

default_args = {
    'owner': 'airflow',
}

with DAG(
        'klook_ETL',
        default_args=default_args,
        schedule_interval=None,
        CORNTAB_FORMAT='0 0 * * *',
        start_date=dt.datetime(2022, 1, 2),
        catchup=False,
        tags=['klook_Camping & glamping'],
) as dag:

    ks = klook_scraper()

    def extract() -> None:
        ks.extract()

    def transform_then_load() -> None:
        ks.transform_then_load()

    extract_task = PythonOperator(
        task_id='transform',
        python_callable=extract,
    )

    transform_then_load_task = PythonOperator(
        task_id='transform then load',
        python_callable=transform_then_load,
    )

    extract_task >> transform_then_load_task
