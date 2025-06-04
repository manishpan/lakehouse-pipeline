import boto3
import traceback
import sys
sys.path.append('/home/manish/Documents/Spark-Tutorial/DE_Complete_Project_1/lakehouse-pipeline/utils')
from logging_config import logger
from pyspark.sql.functions import *
from pyspark.sql.utils import AnalysisException
import os

def write_to_s3(df, bucket_name, folder_path, output_folder, format='parquet', mode='overwrite', partition_col=None, single_file = False):
    logger.info(f"************** Writing {output_folder} to {bucket_name}/{folder_path} ***************")
    try:
        path = f"s3a://{bucket_name}/{os.path.join(folder_path, output_folder)}"
        if single_file:
            df = df.coalesce(1)
            
        if partition_col:
            ## Checking existence of partition_col and adding partition_col column to partition for later use
            try:
                df = df.withColumn(partition_col, col(partition_col))
            except AnalysisException:
                raise ValueError(f"Partition column '{partition_col}' is invalid or not found in DataFrame")
            
            df_write = df.write.format(format).partitionBy(partition_col)
        else:
            df_write = df.write.format(format)
        
        df_write.mode(mode).save(path)

        logger.info(f" ✅ {output_folder} written successfully ")
    except Exception as e:
        logger.error(f"❌ Error while writing {output_folder}")
        raise RuntimeError(f"Unable to write {output_folder} from {bucket_name}/{folder_path}/{output_folder}") from e
