from datetime import datetime
from airflow import DAG
from airflow.models import Variable
from airflow.providers.amazon.aws.operators.emr import EmrAddStepsOperator


cluster_id = 'j-2KL0AJB7P9N0W'
aws_conn = 'aws_default'

spark_packages = [
    "com.qubole.spark:spark-sql-kinesis_2.12:1.2.0_spark-3.0",
    "io.github.spark-redshift-community:spark-redshift_2.12:6.2.0-spark_3.5"
]
packages_list = ",".join(spark_packages)
jdbc_jar_s3_path = "s3://food-delivery-stream-bucket/jars/redshift-connector-jar/redshift-jdbc42-2.1.0.12.jar"

# Fetch Redshift credentials from Airflow Variables
redshift_user = Variable.get("redshift_user")
redshift_password = Variable.get("redshift_password")
aws_access_key = Variable.get("aws_access_key")
aws_secret_key = Variable.get("aws_secret_key")


dag = DAG(
    'spark_submit_streaming_job_to_emr',
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=['streaming'],
)

step_adder = EmrAddStepsOperator(
    task_id='add_step',
    job_flow_id=cluster_id,
    aws_conn_id=aws_conn,
    dag=dag,
    steps=[{
        'Name': 'Run PySpark Streaming Script',
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': [
                'spark-submit',
                '--deploy-mode',
                'cluster',
                '--num-executors', '3',
                '--executor-memory', '6G',
                '--executor-cores', '3',
                '--spark.dynamicAllocation.minExecutors', '1',
                '--spark.dynamicAllocation.maxExecutors', '3',
                '--packages', packages_list,
                '--jars', jdbc_jar_s3_path,
                's3://food-delivery-stream-bucket/pyspark_scripts/pyspark_streaming.py',
                '--redshift_user', redshift_user,
                '--redshift_password', redshift_password,
                '--aws_access_key', aws_access_key,
                '--aws_secret_key', aws_secret_key,
            ],
        }
    }]
)