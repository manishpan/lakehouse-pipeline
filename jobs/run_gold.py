import sys
sys.path.append("/home/manish/Documents/Spark-Tutorial/DE_Complete_Project_1/lakehouse-pipeline")
from utils.spark_session import get_spark_session
from gold_layer import GoldLayer
from utils.logging_config import logger


def main():
    spark = get_spark_session("Gold Layer Execution")
    logger.info("*** Starting gold layer ***")
    
    try:
        GoldLayer(spark).run()
        logger.info("*** ✅ Gold layer execution successful ***")
    except Exception as e:
        logger.error(f"❌ Error! gold layer execution failed: {e}", exc_info=True)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
        