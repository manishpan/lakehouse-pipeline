from utils.s3_client_object import S3ClientProvider
from upload.upload_to_s3 import uploadToS3
from config import config
from utils.spark_session import get_spark_session
from utils.aws_read import get_raw_data


s3_client = S3ClientProvider().get_client()

s3_uploader = uploadToS3(s3_client)

message = s3_uploader.upload_to_s3(config.s3_raw_folder, config.bucket_name, f"{config.raw_data_path}")

spark = get_spark_session()

df = get_raw_data(spark, config.bucket_name, config.s3_raw_folder, config.filename)
df.show()



## Bronze layer

