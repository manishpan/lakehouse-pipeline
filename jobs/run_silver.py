import sys
sys.path.append("/home/manish/Documents/Spark-Tutorial/DE_Complete_Project_1/lakehouse-pipeline")
from utils.spark_session import get_spark_session
from silver_layer import SilverLayer
from utils.logging_config import logger


def main():
    spark = get_spark_session("Silver Layer Execution")
    logger.info("*** Starting silver layer ***")
    
    try:
        SilverLayer(spark).run()
        logger.info("*** ✅ Silver layer execution successful ***")
    except Exception as e:
        logger.error(f"❌ Error! Silver layer execution failed: {e}", exc_info=True)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
        