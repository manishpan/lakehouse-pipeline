import sys
sys.path.append("/home/manish/Documents/Spark-Tutorial/DE_Complete_Project_1/lakehouse-pipeline")
from layers.bronze_layer import BronzeLayer
from layers.silver_layer import SilverLayer
from layers.gold_layer import GoldLayer
from utils.spark_session import get_spark_session

def run_bronze():
    spark = get_spark_session()
    BronzeLayer(spark).run()

def run_silver():
    spark = get_spark_session()
    SilverLayer(spark).run()

def run_gold():
    spark = get_spark_session()
    GoldLayer(spark).run()