import pytest
from pyspark.sql import Row
from unittest.mock import patch, MagicMock
from layers.gold_layer import GoldLayer


@pytest.fixture
def sample_df(spark):
    return spark.createDataFrame([
        Row(
            fullVisitorId="1001", visitId=1, visitStartTime_ts="2024-01-01 10:00:00", visitNumber=1,
            row_quality="CLEAN",
            totals_parsed={"pageviews": 5, "bounces": 0, "transactions": 1, "transactionRevenue": 100.0},
            device_parsed={"deviceCategory": "desktop", "browser": "Chrome", "operatingSystem": "Windows"}
        ),
        Row(
            fullVisitorId="1002", visitId=2, visitStartTime_ts="2024-01-01 12:00:00", visitNumber=1,
            row_quality="CLEAN",
            totals_parsed={"pageviews": 3, "bounces": 1, "transactions": 0, "transactionRevenue": 0.0},
            device_parsed={"deviceCategory": "mobile", "browser": "Safari", "operatingSystem": "iOS"}
        )
    ])

# Patch S3 read and write functions
@patch("layers.gold_layer.read_from_s3")
@patch("layers.gold_layer.write_to_s3")
def test_gold_layer_run_calls_all_steps(mock_write, mock_read, spark, sample_df):
    mock_read.return_value = sample_df
    gold = GoldLayer(spark)

    gold.run()

    # read_from_s3 should be called once
    mock_read.assert_called_once()

    # write_to_s3 should be called 4 times (once per output table)
    assert mock_write.call_count == 4

    # Check each write call includes the correct output folder
    write_calls = [call.args[3] for call in mock_write.call_args_list]
    assert set(write_calls) == {"daily_kpis", "visitor_summary", "device_performance", "session_facts"}

def test_build_daily_kpis(spark, sample_df):
    gold = GoldLayer(spark)
    df_kpis = gold.build_daily_kpis(sample_df)

    result = df_kpis.collect()[0]
    assert result.total_sessions == 2
    assert result.total_users == 2
    assert result.total_pageviews == 8
    assert round(result.bounce_rate, 4) == 0.5
    assert result.transactions == 1
    assert result.revenue == 100.0


def test_build_visitor_summary(spark, sample_df):
    gold = GoldLayer(spark)
    df_summary = gold.build_visitor_summary(sample_df)

    summary_ids = [row.fullVisitorId for row in df_summary.collect()]
    assert set(summary_ids) == {"1001", "1002"}


def test_build_device_performance(spark, sample_df):
    gold = GoldLayer(spark)
    df_device = gold.build_device_performance(sample_df)

    categories = {(row.device_category, row.browser) for row in df_device.collect()}
    assert categories == {("desktop", "Chrome"), ("mobile", "Safari")}


def test_build_session_facts(spark, sample_df):
    gold = GoldLayer(spark)
    df_sessions = gold.build_session_facts(sample_df)

    assert df_sessions.count() == 2
    assert "deviceCategory" in df_sessions.columns
    assert "browser" in df_sessions.columns
    assert "transactionRevenue" in df_sessions.columns