import boto3
import traceback
import sys
sys.path.append('/home/manish/Documents/Spark-Tutorial/DE_Complete_Project_1/lakehouse-pipeline/utils')
from logging_config import logger


def read_from_s3(spark, bucket_name, folder_path, filename, format='csv'):
    logger.info(f"************** Reading {filename} from {bucket_name}/{folder_path} ***************")
    try:
        ## Checking if correct file format has been passed
        allowed_formats = {'csv', 'parquet', 'orc', 'json'}
        if format not in allowed_formats:
            raise ValueError(f"Unsupported format: {format}. Supported formats are {allowed_formats}")

        path = f"s3a://{bucket_name}/{folder_path}"
        ## filename can be empty, maybe we just want to read whole folder
        if filename:
            path = f"{path}/{filename}"

        ## Raw layers are assumed to be in CSV format
        if format == 'csv':
            df = spark.read.format('csv') \
                    .option('header', 'true') \
                    .option("quote", '"') \
                    .option("escape", '"') \
                    .option("multiLine", "false") \
                    .option('inferSchema', 'false') \
                    .load(path)
        ## If data is not in raw, then it should be from bronze, or silver
        else:
            df = spark.read.format(format) \
                .load(path)
        logger.info(f" ✅ {filename} loaded with {df.count()} rows and {len(df.columns)} columns ")
        return df
    except Exception as e:
        logger.error(f"❌ Error while reading {filename}")
        raise RuntimeError(f"Unable to read {filename} from {bucket_name}/{folder_path}/{filename}") from e