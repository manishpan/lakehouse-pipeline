from utils.aws_read import read_from_s3
from utils.aws_write import write_to_s3
from config import config
from pyspark.sql.types import *
from pyspark.sql.functions import *


class GoldLayer:
    def __init__(self, spark):
        self.spark = spark
        self.bucket_name = config.bucket_name
        self.read_from = config.s3_silver_folder
        self.read_filename = config.s3_silver_filename
        self.write_to = config.s3_gold_folder
    
    def read(self):
        return read_from_s3(self.spark, self.bucket_name, self.read_from, self.read_filename, format='parquet')
    
    def write(self, df, output_folder):
        write_to_s3(df, self.bucket_name, self.write_to, output_folder)
        
    def build_daily_kpis(self, df):
        df_kpis = df.filter(col("row_quality") == "CLEAN") \
            .withColumn("date", to_date(col("visitStartTime_ts"))) \
            .groupBy("date") \
            .agg(
                countDistinct("visitId").alias("total_sessions"),
                countDistinct("fullVisitorId").alias("total_users"),
                sum("totals_parsed.pageviews").alias("total_pageviews"),
                round((sum(when(col("totals_parsed.bounces") == 1, 1).otherwise(0)) / count("visitId")), 4).alias("bounce_rate"),
                sum("totals_parsed.transactions").alias("transactions"),
                sum("totals_parsed.transactionRevenue").alias("revenue")
            )
        
        return df_kpis

    def build_visitor_summary(self, df):
        df_summary = df.filter(col("row_quality") == "CLEAN") \
            .groupBy("fullVisitorId") \
            .agg(
                to_date(min("visitStartTime_ts")).alias("first_seen_date"),
                count("visitId").alias("totals_visits"),
                sum("totals_parsed.transactions").alias("total_transactions"),
                sum("totals_parsed.transactionRevenue").alias("total_revenue")
            )
        
        return df_summary

    def build_device_performance(self, df):
        df_device = df.filter(col("row_quality") == "CLEAN") \
                    .groupBy(
                        col("device_parsed.deviceCategory").alias("device_category"),
                        col("device_parsed.browser").alias("browser")
                    ) \
                    .agg(
                        count("visitId").alias("total_sessions"),
                        countDistinct("fullVisitorId").alias("total_users"),
                        round(avg("totals_parsed.pageviews"), 2).alias("avg_pageviews"),
                        round((sum(when(col("totals_parsed.bounces") == 1, 1).otherwise(0)) / count("visitId")), 4).alias("bounce_rate"),
                        sum("totals_parsed.transactions").alias("transactions"),
                        sum("totals_parsed.transactionRevenue").alias("revenue")
                    )

        return df_device

    def build_session_facts(self, df):
        df_session_facts = df.select(
                        col("fullVisitorId"),
                        col("visitId"),
                        col("visitStartTime_ts"),
                        col("visitNumber"),
                        col("device_parsed.deviceCategory").alias("deviceCategory"),
                        col("device_parsed.browser").alias("browser"),
                        col("device_parsed.operatingSystem").alias("operatingSystem"),
                        col("totals_parsed.pageviews").alias("pageviews"),
                        col("totals_parsed.bounces").alias("bounces"),
                        col("totals_parsed.transactions").alias("transactions"),
                        col("totals_parsed.transactionRevenue").alias("transactionRevenue"),
                        col("row_quality")
        )

        return df_session_facts

    def run(self):
        df = self.read()

        kpis = self.build_daily_kpis(df)
        self.write(kpis, "daily_kpis")

        visitor_summary = self.build_visitor_summary(df)
        self.write(visitor_summary, "visitor_summary")

        device_performance = self.build_device_performance(df)
        self.write(device_performance, "device_performance")

        sessions = self.build_session_facts(df)
        self.write(sessions, "session_facts")