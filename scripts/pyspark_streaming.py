from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DecimalType, TimestampType
import argparse


# Variables and Setup
parser = argparse.ArgumentParser(description='PySpark Streaming Job Args')
parser.add_argument('--redshift_user', required=True, help='Redshift Username')
parser.add_argument('--redshift_password', required=True, help='Redshift Password')
parser.add_argument('--aws_access_key', required=True, help='aws_access_key')
parser.add_argument('--aws_secret_key', required=True, help='aws_secret_key')
args = parser.parse_args()

appName = "KinesisToRedshift"
kinesisStreamName = "incoming-food-orders-data"
kinesisEndpointURL = "https://kinesis.us-east-2.amazonaws.com"
kinesisRegion = "us-east-2"
checkpointLocation = "s3://stream-checkpoint/kinesisToRedshift/"
redshiftJdbcUrl = f"jdbc:redshift://redshift-cluster-2.c4gvtbzfgnah.us-east-2.redshift.amazonaws.com:5439/dev"
redshiftTable = "food_delivery_db.factOrders"
redshiftTempDir = "s3://redshift-temp-data3/temp-data/streaming_temp/"

# Define the schema of the incoming JSON data from Kinesis
schema = StructType([
    StructField("OrderID", IntegerType(), True),
    StructField("CustomerID", IntegerType(), True),
    StructField("RestaurantID", IntegerType(), True),
    StructField("RiderID", IntegerType(), True),
    StructField("OrderDate", TimestampType(), True),
    StructField("DeliveryTime", IntegerType(), True),
    StructField("OrderValue", DecimalType(8, 2), True),
    StructField("DeliveryFee", DecimalType(8, 2), True),
    StructField("TipAmount", DecimalType(8, 2), True),
    StructField("OrderStatus", StringType(), True)
])


# Spark Streaming
spark = (SparkSession.builder
         .appName(appName)
         .getOrCreate())

df = (spark
      .readStream
      .format("kinesis")
      .option("streamName", kinesisStreamName)
      .option("endpointUrl", kinesisEndpointURL)
      .option("region", kinesisRegion)
      .option("startingPosition", "latest")
      .option("awsUseInstanceProfile", "false")
      .option("spark.dynamicAllocation.enabled", "false")
      .option("awsAccessKeyId", args.aws_access_key)
      .option("awsSecretKey", args.aws_secret_key)
      .load())

print("Consuming From Read Stream...")
parsed_df = df.selectExpr("CAST(data AS STRING)").select(from_json(col("data"), schema).alias("parsed_data")).select(
    "parsed_data.*")

# Perform stateful deduplication
deduped_df = parsed_df.withWatermark("OrderDate", "10 minutes").dropDuplicates(["OrderID"])


# Writing Data to Redshift - doesn't allow for record by record ingestion
def write_to_redshift(batch_df, batch_id):
    (batch_df.write
     .format("jdbc")
     .option("url", redshiftJdbcUrl)
     .option("user", args.redshift_user)
     .option("password", args.redshift_password)
     .option("dbtable", redshiftTable)
     .option("tempdir", redshiftTempDir)
     .option("driver", "com.amazon.redshift.jdbc.Driver")
     .mode("append")
     .save())


# Write the deduplicated data to Redshift
query = (deduped_df.writeStream
         .foreachBatch(write_to_redshift)
         .outputMode("append")
         .trigger(processingTime='5 seconds')
         .option("checkpointLocation", checkpointLocation)
         .start())

print("Current batch written in Redshift")

query.awaitTermination()
