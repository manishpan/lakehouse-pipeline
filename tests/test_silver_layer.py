import sys
sys.path.append("/home/manish/Documents/Spark-Tutorial/DE_Complete_Project_1/lakehouse-pipeline")
import pytest
from layers.silver_layer import SilverLayer
from pyspark.sql import Row

@pytest.fixture
def silver_layer(spark):
    return SilverLayer(spark)

@pytest.fixture
def sample_input_df(spark):
    data = [
        # CLEAN: All valid
        Row(
            fullVisitorId="111111",
            visitId="12345",
            visitNumber="2",
            visitStartTime="20170801",
            device='{"browser": "Chrome", "operatingSystem": "Windows", "deviceCategory": "desktop"}',
            totals='{"visits":1, "hits":3, "pageviews":1, "bounces":0, "transactions":1, "transactionRevenue":1000}'
        ),
        # FAIL: Invalid visitId
        Row(
            fullVisitorId="222222",
            visitId="0",
            visitNumber="1",
            visitStartTime="20170801",
            device='{"browser": "Safari", "operatingSystem": "Mac", "deviceCategory": "mobile"}',
            totals='{"visits":1, "hits":3, "pageviews":2, "bounces":0, "transactions":0, "transactionRevenue":0}'
        ),
        # WARN: bounces = 1 but pageviews ≠ 1
        Row(
            fullVisitorId="333333",
            visitId="12346",
            visitNumber="1",
            visitStartTime="20170801",
            device='{"browser": "Firefox", "operatingSystem": "Linux", "deviceCategory": "desktop"}',
            totals='{"visits":1, "hits":3, "pageviews":4, "bounces":1, "transactions":0, "transactionRevenue":0}'
        )
    ]
    return spark.createDataFrame(data)

def test_validate_assign_correct_row_quality(silver_layer, sample_input_df):
    df_validated = silver_layer.validate(sample_input_df)

    results = df_validated.select("fullVisitorId", "row_quality").collect()
    output = {row["fullVisitorId"]: row["row_quality"] for row in results}
    
    assert output["111111"] == "CLEAN"
    assert output["222222"] == "FAIL"
    assert output["333333"] == "WARN"

def test_validate_adds_required_columns(silver_layer, sample_input_df):
    df_validated = silver_layer.validate(sample_input_df)

    expected_columns = [
        "device_parsed",
        "totals_parsed",
        "visitStartTime_ts",
        "is_valid_fullVisitorId",
        "is_valid_visitId",
        "is_valid_visitNumber",
        "is_valid_transactions",
        "is_valid_bounces",
        "row_quality"
    ]

    for col in expected_columns:
        assert col in df_validated.columns