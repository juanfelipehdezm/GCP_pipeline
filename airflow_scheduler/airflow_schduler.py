import datetime as dt
from airflow import DAG
from airflow.contrib.operators.dataflow_operator import DataFlowPythonOperator

DEFAULT_ARGS = {
    "owner" : "juanfelipehdez",
    "start_date" : dt.datetime(2023,11,12),
    "email_on_failure" : True,
    "email_on_retry" : False,
    "email" : "juanfelipehdezm@gmail.com",
    "retries" : 2,
    "retry_delay" : dt.timedelta(minutes=3),
    #dataflow related args
    "dataflow_default_options": {
        "project": "coral-sonar-395901",
        "region": "us-central1",
        "runner" : "DataFlowRunner"
    }

}

with DAG("beam_food_pipeline", default_args=DEFAULT_ARGS,
         schedule_interval="@daily", catchup=True,
         dagrun_timeout=dt.timedelta(minutes=5)) as dag:
    
    execute_beam_pipeline = DataFlowPythonOperator(
        task_id = "beam_pipeline",
        py_file = "gs://bigquery_ingestion_bucket/apache_beam_pipeline.py",
        options= {
            "input" : "gs://bigquery_ingestion_bucket/food_daily.csv"
        }
    )