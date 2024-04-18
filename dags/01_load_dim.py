from airflow import DAG
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
from airflow.operators.dagrun_operator import TriggerDagRunOperator


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

    # Create schema if it doesn't exist
    create_schema = PostgresOperator(
        task_id='create_schema',
        postgres_conn_id='redshift',
        sql="CREATE SCHEMA IF NOT EXISTS food_delivery_db;",
    )

    # Drop tables if they exist
    drop_dimCustomers = PostgresOperator(
        task_id='drop_dimCustomers',
        postgres_conn_id='redshift',
        sql="DROP TABLE IF EXISTS food_delivery_db.dimCustomers;",
    )

    drop_dimRestaurants = PostgresOperator(
        task_id='drop_dimRestaurants',
        postgres_conn_id='redshift',
        sql="DROP TABLE IF EXISTS food_delivery_db.dimRestaurants;",
    )

    drop_dimDeliveryDrivers = PostgresOperator(
        task_id='drop_dimDeliveryDrivers',
        postgres_conn_id='redshift',
        sql="DROP TABLE IF EXISTS food_delivery_db.dimDeliveryDrivers;",
    )

    drop_factOrders = PostgresOperator(
        task_id='drop_factOrders',
        postgres_conn_id='redshift',
        sql="DROP TABLE IF EXISTS food_delivery_db.factOrders;",
    )

    # Create dimension and fact tables
    create_dimCustomers = PostgresOperator(
        task_id='create_dimCustomers',
        postgres_conn_id='redshift',
        sql="""
            CREATE TABLE food_delivery_db.dimCustomers (
                CustomerID INT PRIMARY KEY,
                CustomerName VARCHAR(255),
                CustomerEmail VARCHAR(255),
                CustomerPhone VARCHAR(50),
                CustomerAddress VARCHAR(500),
                RegistrationDate DATE
            );
        """,
    )

    create_dimRestaurants = PostgresOperator(
        task_id='create_dimRestaurants',
        postgres_conn_id='redshift',
        sql="""
            CREATE TABLE food_delivery_db.dimRestaurants (
                RestaurantID INT PRIMARY KEY,
                RestaurantName VARCHAR(255),
                CuisineType VARCHAR(100),
                RestaurantAddress VARCHAR(500),
                RestaurantRating DECIMAL(3,1)
            );
        """,
    )

    create_dimDeliveryDrivers = PostgresOperator(
        task_id='create_dimDeliveryDrivers',
        postgres_conn_id='redshift',
        sql="""
            CREATE TABLE food_delivery_db.dimDeliveryDrivers (
                DriverID INT PRIMARY KEY,
                DriverName VARCHAR(255),
                DriverPhone VARCHAR(50),
                DriverVehicleType VARCHAR(50),
                VehicleID VARCHAR(50),
                DriverRating DECIMAL(3,1)
            );
        """,
    )

    create_factOrders = PostgresOperator(
        task_id='create_factOrders',
        postgres_conn_id='redshift',
        sql="""
            CREATE TABLE food_delivery_db.factOrders (
                OrderID INT PRIMARY KEY,
                CustomerID INT REFERENCES food_delivery_db.dimCustomers(CustomerID),
                RestaurantID INT REFERENCES food_delivery_db.dimRestaurants(RestaurantID),
                DriverID INT REFERENCES food_delivery_db.dimDeliveryDrivers(DriverID),
                OrderDate TIMESTAMP WITHOUT TIME ZONE,
                DeliveryTime INT,
                OrderValue DECIMAL(8,2),
                DeliveryFee DECIMAL(8,2),
                TipAmount DECIMAL(8,2),
                OrderStatus VARCHAR(50)
            );
        """,
    )

    # Load data into dimension tables from S3
    load_dimCustomers = S3ToRedshiftOperator(
        task_id='load_dimCustomers',
        schema='food_delivery_db',
        table='dimCustomers',
        s3_bucket='food-delivery-stream',
        s3_key='dim/dimCustomers.csv',
        copy_options=['CSV', 'IGNOREHEADER 1', 'QUOTE as \'"\''],
        aws_conn_id='aws_default',
        redshift_conn_id='redshift',
    )

    load_dimRestaurants = S3ToRedshiftOperator(
        task_id='load_dimRestaurants',
        schema='food_delivery_db',
        table='dimRestaurants',
        s3_bucket='food-delivery-stream',
        s3_key='dim/dimRestaurants.csv',
        copy_options=['CSV', 'IGNOREHEADER 1', 'QUOTE as \'"\''],
        aws_conn_id='aws_default',
        redshift_conn_id='redshift',
    )

    load_dimDeliveryDrivers = S3ToRedshiftOperator(
        task_id='load_dimDeliveryDrivers',
        schema='food_delivery_db',
        table='dimDeliveryDrivers',
        s3_bucket='food-delivery-stream',
        s3_key='dim/dimDeliveryDrivers.csv',
        copy_options=['CSV', 'IGNOREHEADER 1', 'QUOTE as \'"\''],
        aws_conn_id='aws_default',
        redshift_conn_id='redshift',
    )

    trigger_spark_streaming_dag = TriggerDagRunOperator(
        task_id="trigger_spark_streaming_dag",
        trigger_dag_id="spark_submit_streaming_job_to_emr",
    )



# First, create the schema
create_schema >> [drop_dimCustomers, drop_dimRestaurants, drop_dimDeliveryDrivers, drop_factOrders]

# Once the existing tables are dropped, proceed to create new tables
drop_dimCustomers >> create_dimCustomers
drop_dimRestaurants >> create_dimRestaurants
drop_dimDeliveryDrivers >> create_dimDeliveryDrivers
drop_factOrders >> create_factOrders

# Once all the dimension tables have been created, then create the fact table
[create_dimCustomers, create_dimRestaurants, create_dimDeliveryDrivers] >> create_factOrders

# After each table is created, load the Dimension data
create_dimCustomers >> load_dimCustomers
create_dimRestaurants >> load_dimRestaurants
create_dimDeliveryDrivers >> load_dimDeliveryDrivers

# Once all the dimension tables have been loaded, then trigger the EMR spark streaming step
[load_dimCustomers, load_dimRestaurants, load_dimDeliveryDrivers] >> trigger_spark_streaming_dag