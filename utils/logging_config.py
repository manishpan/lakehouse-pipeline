import logging
import logging.config

logging.basicConfig(filename='app.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# import yaml

# with open(r'/home/manish/Documents/Spark-Tutorial/DE_Complete_Project_1/lakehouse-pipeline/config/settings.yaml', 'r') as file:
#     config = yaml.safe_load(file)
#     logging.config.dictConfig(config['logging'])

# logger = logging.getLogger(__name__)
# logger.info("Logging with YAML config")