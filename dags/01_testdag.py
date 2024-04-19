from datetime import timedelta
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.amazon.aws.operators.redshift_data import RedshiftDataOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
        'create_and_load_dim',
        default_args=default_args,
        description='ETL for food delivery data into Redshift',
        schedule_interval=None,
        start_date=days_ago(1),
        catchup=False
) as dag:

    create_schema = PostgresOperator(
        task_id='create_schema',
        postgres_conn_id='redshift',
        sql="CREATE SCHEMA IF NOT EXISTS food_delivery_db;",
    )

create_schema
