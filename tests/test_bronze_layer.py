import sys
sys.path.append("/home/manish/Documents/Spark-Tutorial/DE_Complete_Project_1/lakehouse-pipeline")
import pytest
from unittest.mock import patch, MagicMock
from layers.bronze_layer import BronzeLayer
from pyspark.sql import Row

@pytest.fixture
def sample_df(spark):
    data = [Row(id=1, name="Alice"), Row(id=2, name="Bob")]
    return spark.createDataFrame(data)

@patch("layers.bronze_layer.read_from_s3")
def test_read_calls_read_from_s3(mock_read_from_s3, spark):
    bronze = BronzeLayer(spark)
    bronze.read()
    mock_read_from_s3.assert_called_once_with(
        spark,
        bronze.bucket_name,
        bronze.read_from, 
        bronze.read_filename
    )

@patch("layers.bronze_layer.write_to_s3")
def test_write_calls_write_to_s3(mock_write_to_s3, spark, sample_df):
    bronze = BronzeLayer(spark)
    bronze.write(sample_df)
    mock_write_to_s3.assert_called_once_with(
        sample_df, 
        bronze.bucket_name, 
        bronze.write_to, 
        bronze.write_filename
    )

@patch("layers.bronze_layer.read_from_s3")
@patch("layers.bronze_layer.write_to_s3")
def test_run_calls_read_and_write(mock_write_to_s3, mock_read_from_s3, spark, sample_df):
    mock_read_from_s3.return_value = sample_df
    bronze = BronzeLayer(spark)
    bronze.run()
    mock_read_from_s3.assert_called_once()
    mock_write_to_s3.assert_called_once_with(
        sample_df, 
        bronze.bucket_name, 
        bronze.write_to, 
        bronze.write_filename
    )
