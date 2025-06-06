from utils.s3_client_object import S3ClientProvider
from upload.upload_to_s3 import uploadToS3
from config import config
from utils.spark_session import get_spark_session
from bronze import BronzeLayer
from silver import SilverLayer
from gold import GoldLayer

s3_client = S3ClientProvider().get_client()

s3_uploader = uploadToS3(s3_client)

message = s3_uploader.upload_to_s3(config.s3_raw_folder, config.bucket_name, f"{config.raw_data_path}")

spark = get_spark_session()

## Bronze layer
bronze_layer = BronzeLayer(spark)
bronze_layer.run()

## Silver layer
silver_layer = SilverLayer(spark)
silver_layer.run()

## Gold Layer
gold_layer = GoldLayer(spark)
gold_layer.run()