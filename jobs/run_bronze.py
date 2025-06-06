import sys
sys.path.append("/home/manish/Documents/Spark-Tutorial/DE_Complete_Project_1/lakehouse-pipeline")
from utils.spark_session import get_spark_session
from bronze_layer import BronzeLayer
from utils.logging_config import logger


def main():
    spark = get_spark_session("Bronze Layer Execution")
    logger.info("*** Starting bronze layer ***")
    
    try:
        BronzeLayer(spark).run()
        logger.info("*** ✅ Bronze layer execution successful ***")
    except Exception as e:
        logger.error(f"❌ Error! Bronze layer execution failed: {e}", exc_info=True)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
        