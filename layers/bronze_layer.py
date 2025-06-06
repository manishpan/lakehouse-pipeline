import sys
sys.path.append("/home/manish/Documents/Spark-Tutorial/DE_Complete_Project_1/lakehouse-pipeline")
from utils.aws_read import read_from_s3
from utils.aws_write import write_to_s3
from config import config

class BronzeLayer:
    def __init__(self, spark):
        self.spark = spark
        self.bucket_name = config.bucket_name
        self.read_from = config.s3_raw_folder
        self.read_filename = config.filename
        self.write_to = config.s3_bronze_folder
        self.write_filename = config.s3_bronze_filename
    
    def read(self):
        return read_from_s3(self.spark, self.bucket_name, self.read_from, self.read_filename)

    def write(self, df):
        write_to_s3(df, self.bucket_name, self.write_to, self.write_filename)

    def run(self):
        df = self.read()
        self.write(df)