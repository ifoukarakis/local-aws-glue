import sys
import boto3
import logging

from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.transforms import *
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.types import *
from pyspark.sql import Row
from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)

glueContext = GlueContext(SparkContext.getOrCreate())

S3_MOCK_ENDPOINT = "http://localstack:4566"
BUCKET = 'test-bucket'
# Values do not really matter
ACCESS_KEY = "mock"
SECRET_KEY = "mock"

spark = SparkSession.builder.getOrCreate()
hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
hadoop_conf.set("fs.s3a.path.style.access", "true")
hadoop_conf.set("fs.s3a.connection.ssl.enabled", "false")
hadoop_conf.set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
hadoop_conf.set("fs.s3a.access.key", ACCESS_KEY)
hadoop_conf.set("fs.s3a.secret.key", SECRET_KEY)
hadoop_conf.set("fs.s3a.session.token", "mock")
hadoop_conf.set("fs.s3a.endpoint", S3_MOCK_ENDPOINT)

# Create bucket

s3_conn = boto3.resource(
    "s3", region_name="eu-central-1",
    endpoint_url=S3_MOCK_ENDPOINT,
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY,
)

try:
    s3_conn.create_bucket(Bucket=BUCKET)
except:
    # I.e. bucket already exists
    log.info('Error creating bucket')

# Sample job

order_list = [
    ['1005', '623', 'YES', '1418901234', '75091'], \
    ['1006', '547', 'NO', '1418901256', '75034'], \
    ['1007', '823', 'YES', '1418901300', '75023'], \
    ['1008', '912', 'NO', '1418901400', '82091'], \
    ['1009', '321', 'YES', '1418902000', '90093'] \
    ]

# Define schema for the order_list
order_schema = StructType([
    StructField("order_id", StringType()),
    StructField("customer_id", StringType()),
    StructField("essential_item", StringType()),
    StructField("timestamp", StringType()),
    StructField("zipcode", StringType())
])

# Create a Spark Dataframe from the python list and the schema
df_orders = spark.createDataFrame(order_list, schema=order_schema)

dyf_orders = DynamicFrame.fromDF(df_orders, glueContext, "dyf")

# Input 
dyf_applyMapping = ApplyMapping.apply(frame=dyf_orders, mappings=[
    ("order_id", "String", "order_id", "Long"),
    ("customer_id", "String", "customer_id", "Long"),
    ("essential_item", "String", "essential_item", "String"),
    ("timestamp", "String", "timestamp", "Long"),
    ("zipcode", "String", "zip", "Long")
])


def next_day_air(rec):
    if rec["zip"] == 75034:
        rec["next_day_air"] = True
    return rec


mapped_dyF = Map.apply(frame=dyf_applyMapping, f=next_day_air)

glueContext.write_dynamic_frame.from_options(
    frame=mapped_dyF,
    connection_options={'path': f's3a://{BUCKET}/output'},
    connection_type='s3',
    format='json'
)
