import os
from datetime import timedelta
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.amazon.aws.operators.redshift_data import RedshiftDataOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator



os.environ['AWS_DEFAULT_REGION'] = 'us-east-2'
cluster_identifier = 'redshift-cluster-2'
database = 'dev'
db_user = 'awsuser'

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
    create_schema = RedshiftDataOperator(
        task_id='create_schema',
        cluster_identifier=cluster_identifier,
        database=database,
        db_user=db_user,
        sql="CREATE SCHEMA IF NOT EXISTS food_delivery_db;",
        poll_interval=10,
        wait_for_completion=True
    )

    # Drop dimension and fact tables
    drop_dimCustomers = RedshiftDataOperator(
        task_id='drop_dimCustomers',
        cluster_identifier=cluster_identifier,
        database=database,
        db_user=db_user,
        sql="DROP TABLE IF EXISTS food_delivery_db.dimCustomers CASCADE;",
        poll_interval=10,
        wait_for_completion=True
    )

    drop_dimRestaurants = RedshiftDataOperator(
        task_id='drop_dimRestaurants',
        cluster_identifier=cluster_identifier,
        database=database,
        db_user=db_user,
        sql="DROP TABLE IF EXISTS food_delivery_db.dimRestaurants CASCADE;",
        poll_interval=10,
        wait_for_completion=True
    )

    drop_dimDeliveryDrivers = RedshiftDataOperator(
        task_id='drop_dimDeliveryDrivers',
        cluster_identifier=cluster_identifier,
        database=database,
        db_user=db_user,
        sql="DROP TABLE IF EXISTS food_delivery_db.dimDeliveryDrivers CASCADE;",
        poll_interval=10,
        wait_for_completion=True
    )

    drop_factOrders = RedshiftDataOperator(
        task_id='drop_factOrders',
        cluster_identifier=cluster_identifier,
        database=database,
        db_user=db_user,
        sql="DROP TABLE IF EXISTS food_delivery_db.factOrders CASCADE;",
        poll_interval=10,
        wait_for_completion=True
    )

    # Recreate dimension and fact tables
    create_dimCustomers = RedshiftDataOperator(
        task_id='create_dimCustomers',
        cluster_identifier=cluster_identifier,
        database=database,
        db_user=db_user,
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
        poll_interval=10,
        wait_for_completion=True
    )

    create_dimRestaurants = RedshiftDataOperator(
        task_id='create_dimRestaurants',
        cluster_identifier=cluster_identifier,
        database=database,
        db_user=db_user,
        sql="""
            CREATE TABLE food_delivery_db.dimRestaurants (
                RestaurantID INT PRIMARY KEY,
                RestaurantName VARCHAR(255),
                CuisineType VARCHAR(100),
                RestaurantAddress VARCHAR(500),
                RestaurantRating DECIMAL(3,1)
            );
        """,
        poll_interval=10,
        wait_for_completion=True
    )

    create_dimDeliveryDrivers = RedshiftDataOperator(
        task_id='create_dimDeliveryDrivers',
        cluster_identifier=cluster_identifier,
        database=database,
        db_user=db_user,
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
        poll_interval=10,
        wait_for_completion=True
    )

    create_factOrders = RedshiftDataOperator(
        task_id='create_factOrders',
        cluster_identifier=cluster_identifier,
        database=database,
        db_user=db_user,
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
        poll_interval=10,
        wait_for_completion=True
    )

    # Load dimension tables only
    load_dimCustomers = RedshiftDataOperator(
        task_id='load_dimCustomers',
        cluster_identifier=cluster_identifier,
        database=database,
        db_user=db_user,
        sql="""
            copy food_delivery_db.dimCustomers
            from 's3://food-delivery-stream-bucket/dims/dimCustomers.csv'
            iam_role 'arn:aws:iam::891377180984:role/service-role/AmazonRedshift-CommandsAccessRole-20240405T160936'
            CSV
            delimiter ','
            ignoreheader 1;
            """,
        poll_interval=10,
        wait_for_completion=True
    )

    load_dimRestaurants = RedshiftDataOperator(
        task_id='load_dimRestaurants',
        cluster_identifier=cluster_identifier,
        database=database,
        db_user=db_user,
        sql="""
            copy food_delivery_db.dimRestaurants
            from 's3://food-delivery-stream-bucket/dims/dimRestaurants.csv'
            iam_role 'arn:aws:iam::891377180984:role/service-role/AmazonRedshift-CommandsAccessRole-20240405T160936'
            CSV
            delimiter ','
            ignoreheader 1;
            """,
        poll_interval=10,
        wait_for_completion=True
    )

    load_dimDeliveryDrivers = RedshiftDataOperator(
        task_id='load_dimDeliveryDrivers',
        cluster_identifier=cluster_identifier,
        database=database,
        db_user=db_user,
        sql="""
            copy food_delivery_db.dimDeliveryDrivers
            from 's3://food-delivery-stream-bucket/dims/dimDeliveryDrivers.csv'
            iam_role 'arn:aws:iam::891377180984:role/service-role/AmazonRedshift-CommandsAccessRole-20240405T160936'
            CSV
            delimiter ','
            ignoreheader 1;
            """,
        poll_interval=10,
        wait_for_completion=True
    )

    # Run Pyspark EMR
    trigger_spark_streaming_dag = TriggerDagRunOperator(
        task_id="trigger_spark_streaming_dag",
        trigger_dag_id="spark_submit_streaming_job_to_emr"
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