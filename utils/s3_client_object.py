import boto3
import boto3.session
import sys
sys.path.append("/home/manish/Documents/Spark-Tutorial/DE_Complete_Project_1/lakehouse-pipeline/utils")
from logging_config import logger

class S3ClientProvider:
    def __init__(self):
        logger.info(f"***************** 🧵 Establishing aws connection ***************")
        try:
            self.session = boto3.Session(profile_name='default')
        except Exception as e:
            logger.error(f"❌ Failed to establish AWS session.")
            raise RuntimeError("Unable to establish AWS session") from e
        
        logger.info(f" ✅ Connection successful ")
        self.s3_client = self.session.client('s3')
    
    def get_client(self):
        return self.s3_client