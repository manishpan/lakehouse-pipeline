from utils.aws_read import get_raw_data
from utils.aws_write import write_to_silver
from config import config
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

spark = SparkSession.builder \
    .config(map = config.spark_config) \
    .getOrCreate()

df_raw = get_raw_data(spark, config.bucket_name, config.s3_raw_folder, config.filename)

## Defining schema to parse JSON fields.
device_schema = StructType([
    StructField("browser", StringType(), True),
    StructField("operatingSystem", StringType(), True),
    StructField("deviceCategory", StringType(), True)
])

totals_schema = StructType([
    StructField("visits", IntegerType(), True),
    StructField("hits", IntegerType(), True),
    StructField("pageviews", IntegerType(), True),
    StructField("bounces", IntegerType(), True),
    StructField("transactions", IntegerType(), True),
    StructField("transactionRevenue", IntegerType(), True),
])

df = df_raw \
    .withColumn("device_parsed", from_json(col("device"), device_schema)) \
    .withColumn("totals_parsed", from_json(col("totals"), totals_schema)) \
    .withColumn("visitStartTime_ts", to_timestamp(col("visitStartTime")))


## Validating data

df_validated = df \
        .withColumn("is_valid_fullVisitorId", col("fullVisitorId").isNotNull() & (length(col("fullVisitorId")) > 5)) \
        .withColumn("is_valid_visitId", col("visitId").cast('long') > 0) \
        .withColumn("is_valid_visitNumber", col("visitNumber").cast('int') >= 1) \
        .withColumn("is_valid_timestamp", col("visitStartTime_ts").isNotNull()) \
        .withColumn("is_valid_transactions", when(col("totals_parsed.transactions") > 0, col("totals_parsed.transactionRevenue") > 0).otherwise(True)) \
        .withColumn("is_valid_bounces", when(col("totals_parsed.bounces") == 1, col("totals_parsed.pageviews") == 1).otherwise(True)) \
        .withColumn("row_quality", 
                       when(
                            ~col("is_valid_fullVisitorId") |
                           ~col("is_valid_visitId") |
                           ~col("is_valid_visitNumber") |
                           ~col("is_valid_timestamp"), "FAIL")
                        .when(~col("is_valid_transactions") | ~col("is_valid_bounces"), "WARN")
                        .otherwise("CLEAN")
                   )

df_validated.groupBy("row_quality").count().show()

write_to_silver(df_validated, config.bucket_name, config.s3_silver_folder, config.s3_silver_filename)