from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

import sys
sys.path.append("/home/manish/Documents/Spark-Tutorial/DE_Complete_Project_1/lakehouse-pipeline")
from layers.bronze_layer import BronzeLayer
from layers.silver_layer import SilverLayer
from layers.gold_layer import GoldLayer
from utils.spark_session import get_spark_session

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
with DAG(
    dag_id='lakehouse_pipeline',
    default_args=default_args,
    description='ETL pipeline using Bronze, Silver, Gold layers',
    schedule=None,  # Can set to '@daily' or '0 12 * * *'
    catchup=False,
    tags=['lakehouse', 'spark'],
) as dag:

    def run_bronze():
        spark = get_spark_session()
        BronzeLayer(spark).run()

    def run_silver():
        spark = get_spark_session()
        SilverLayer(spark).run()

    def run_gold():
        spark = get_spark_session()
        GoldLayer(spark).run()

    bronze_task = PythonOperator(task_id="bronze_layer", python_callable=run_bronze)
    silver_task = PythonOperator(task_id="silver_layer", python_callable=run_silver)
    gold_task = PythonOperator(task_id="gold_layer", python_callable=run_gold)

    ## Set task dependencies
    bronze_task >> silver_task >> gold_task