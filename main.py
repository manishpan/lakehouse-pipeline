import sys
sys.path.append("/home/manish/Documents/Spark-Tutorial/DE_Complete_Project_1/lakehouse-pipeline")
from utils.s3_client_object import S3ClientProvider
from upload.upload_to_s3 import uploadToS3
from config import config
from utils.spark_session import get_spark_session
from layers.bronze_layer import BronzeLayer
from layers.silver_layer import SilverLayer
from layers.gold_layer import GoldLayer

s3_client = S3ClientProvider().get_client()

s3_uploader = uploadToS3(s3_client)

message = s3_uploader.upload_to_s3(config.s3_raw_folder, config.bucket_name, f"{config.raw_data_path}")

spark = get_spark_session()

## Bronze layer
BronzeLayer(spark).run()

## Silver layer
SilverLayer(spark).run()

## Gold Layer
GoldLayer(spark).run()