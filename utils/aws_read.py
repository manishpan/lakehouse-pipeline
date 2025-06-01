import boto3
import traceback
import sys
sys.path.append('/home/manish/Documents/Spark-Tutorial/DE_Complete_Project_1/lakehouse-pipeline/utils')
from logging_config import logger


def get_raw_data(spark, bucket_name, folder_path, filename):
    logger.info(f"************** Reading {filename} from {bucket_name}/{folder_path} ***************")
    try:
        path = f"s3a://{bucket_name}/{folder_path}/{filename}"
        df = spark.read.format('csv') \
                .option('header', 'true') \
                .option('inferSchema', 'false') \
                .load(path)
        logger.info(f" ✅ read {filename} successfully ")
    except Exception as e:
        logger.error(f"❌ Error while reading {filename}")
        raise RuntimeError(f"Unable to read {filename} from {bucket_name}/{folder_path}/{filename}") from e
    
    return df