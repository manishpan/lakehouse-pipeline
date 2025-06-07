import sys
sys.path.append("/home/manish/Documents/Spark-Tutorial/DE_Complete_Project_1/lakehouse-pipeline")
from config import config
from pyspark.sql.types import *
from pyspark.sql.functions import *
from utils.aws_read import read_from_s3
from utils.aws_write import write_to_s3


class SilverLayer:
    def __init__(self, spark):
        self.spark = spark
        self.bucket_name = config.bucket_name
        self.read_from = config.s3_bronze_folder
        self.read_filename = config.s3_bronze_filename
        self.write_to = config.s3_silver_folder
        self.write_filename = config.s3_silver_filename

        ## Defining device_schema and totals_schema for JSON parsing
        self.device_schema = StructType([
            StructField("browser", StringType(), True),
            StructField("operatingSystem", StringType(), True),
            StructField("deviceCategory", StringType(), True)
        ])
        self.totals_schema = StructType([
            StructField("visits", IntegerType(), True),
            StructField("hits", IntegerType(), True),
            StructField("pageviews", IntegerType(), True),
            StructField("bounces", IntegerType(), True),
            StructField("transactions", IntegerType(), True),
            StructField("transactionRevenue", IntegerType(), True),
        ])
    
    def read(self, format='parquet'):
        return read_from_s3(self.spark, self.bucket_name, self.read_from, self.read_filename, format='parquet')
    
    def validate(self, df_bronze):
        df = df_bronze \
            .withColumn("device_parsed", from_json(col("device"), self.device_schema)) \
            .withColumn("totals_parsed", from_json(col("totals"), self.totals_schema)) \
            .withColumn("visitStartTime_ts", to_timestamp(col("visitStartTime")))

        ## Validating data

        df_validated = df \
                .withColumn("is_valid_fullVisitorId", col("fullVisitorId").isNotNull() & (length(col("fullVisitorId")) > 5)) \
                .withColumn("is_valid_visitId", col("visitId").cast('long') > 0) \
                .withColumn("is_valid_visitNumber", col("visitNumber").cast('int') >= 1) \
                .withColumn("visitStartTime_ts", to_timestamp(col("visitStartTime"), "yyyyMMdd")) \
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
        
        return df_validated

    def write(self, df):
        write_to_s3(df, self.bucket_name, self.write_to, self.write_filename, partition_col="row_quality")

    def run(self):
        df_bronze = self.read()
        df_validated = self.validate(df_bronze)
        self.write(df_validated)