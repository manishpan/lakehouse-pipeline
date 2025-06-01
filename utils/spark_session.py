from pyspark.sql import SparkSession
import sys
sys.path.append('/home/manish/Documents/Spark-Tutorial/DE_Complete_Project_1/lakehouse-pipeline/config')
from config import *

def get_spark_session():

    spark = SparkSession.builder \
    .config(map = config.spark_config) \
    .getOrCreate()

    return spark