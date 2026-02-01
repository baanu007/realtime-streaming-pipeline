"""
Real-Time Order Processor
Spark Structured Streaming job for processing orders from Kafka
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_timestamp, current_timestamp,
    window, sum, count, avg, expr, when, lit
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DoubleType, TimestampType, BooleanType
)


# Order event schema
ORDER_SCHEMA = StructType([
    StructField("order_id", StringType(), False),
    StructField("customer_id", StringType(), False),
    StructField("product_id", StringType(), False),
    StructField("quantity", IntegerType(), False),
    StructField("unit_price", DoubleType(), False),
    StructField("discount", DoubleType(), True),
    StructField("order_status", StringType(), False),
    StructField("payment_method", StringType(), True),
    StructField("shipping_address", StringType(), True),
    StructField("event_time", StringType(), False),
    StructField("event_type", StringType(), False)
])


class OrderProcessor:
    """Processes order events from Kafka in real-time"""
    
    def __init__(self, config: dict):
        self.config = config
        self.spark = self._create_spark_session()
        
    def _create_spark_session(self) -> SparkSession:
        """Initialize Spark session with Kafka support"""
        return (
            SparkSession.builder
            .appName("OrderProcessor")
            .config("spark.sql.streaming.checkpointLocation", 
                    self.config.get("checkpoint_location", "/tmp/checkpoints"))
            .config("spark.sql.shuffle.partitions", "8")
            .config("spark.streaming.kafka.maxRatePerPartition", "1000")
            .config("spark.sql.streaming.stateStore.stateSchemaCheck", "false")
            .getOrCreate()
        )
    
    def read_kafka_stream(self):
        """Read orders from Kafka topic"""
        return (
            self.spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", 
                    self.config.get("bootstrap_servers", "localhost:9092"))
            .option("subscribe", self.config.get("orders_topic", "orders"))
            .option("startingOffsets", "latest")
            .option("failOnDataLoss", "false")
            .load()
        )
    
    def parse_orders(self, kafka_df):
        """Parse JSON orders from Kafka messages"""
        return (
            kafka_df
            .select(
                col("key").cast("string").alias("message_key"),
                from_json(col("value").cast("string"), ORDER_SCHEMA).alias("order"),
                col("timestamp").alias("kafka_timestamp"),
                col("partition"),
                col("offset")
            )
            .select(
                "message_key",
                "order.*",
                "kafka_timestamp",
                "partition",
                "offset"
            )
            .withColumn("event_time", to_timestamp(col("event_time")))
            .withColumn("processing_time", current_timestamp())
        )
    
    def enrich_orders(self, orders_df):
        """Enrich orders with calculated fields"""
        return (
            orders_df
            .withColumn("gross_amount", col("quantity") * col("unit_price"))
            .withColumn("discount_amount", 
                        when(col("discount").isNotNull(), 
                             col("gross_amount") * col("discount"))
                        .otherwise(0))
            .withColumn("net_amount", 
                        col("gross_amount") - col("discount_amount"))
            .withColumn("is_high_value", col("net_amount") > 500)
            .withColumn("processing_lag_ms",
                        (col("processing_time").cast("long") - 
                         col("event_time").cast("long")) * 1000)
        )
    
    def aggregate_orders(self, enriched_df):
        """Windowed aggregations for real-time metrics"""
        return (
            enriched_df
            .withWatermark("event_time", "1 minute")
            .groupBy(
                window("event_time", "5 minutes", "1 minute"),
                "order_status"
            )
            .agg(
                count("*").alias("order_count"),
                sum("net_amount").alias("total_revenue"),
                avg("net_amount").alias("avg_order_value"),
                sum(when(col("is_high_value"), 1).otherwise(0))
                    .alias("high_value_count"),
                avg("processing_lag_ms").alias("avg_latency_ms")
            )
        )
    
    def write_to_console(self, df, query_name: str):
        """Write stream to console (for debugging)"""
        return (
            df.writeStream
            .queryName(query_name)
            .outputMode("update")
            .format("console")
            .option("truncate", False)
            .trigger(processingTime="10 seconds")
            .start()
        )
    
    def write_to_snowflake(self, df, table_name: str):
        """Write stream to Snowflake"""
        snowflake_options = {
            "sfURL": self.config["snowflake"]["url"],
            "sfUser": self.config["snowflake"]["user"],
            "sfPassword": self.config["snowflake"]["password"],
            "sfDatabase": self.config["snowflake"]["database"],
            "sfSchema": self.config["snowflake"]["schema"],
            "sfWarehouse": self.config["snowflake"]["warehouse"],
            "dbtable": table_name
        }
        
        def write_batch(batch_df, batch_id):
            if batch_df.count() > 0:
                (batch_df.write
                 .format("snowflake")
                 .options(**snowflake_options)
                 .mode("append")
                 .save())
        
        return (
            df.writeStream
            .foreachBatch(write_batch)
            .outputMode("update")
            .trigger(processingTime="30 seconds")
            .start()
        )
    
    def run(self):
        """Main execution method"""
        print("Starting Order Processor...")
        
        # Read from Kafka
        raw_stream = self.read_kafka_stream()
        
        # Parse and transform
        orders = self.parse_orders(raw_stream)
        enriched = self.enrich_orders(orders)
        aggregated = self.aggregate_orders(enriched)
        
        # Write outputs
        detail_query = self.write_to_console(enriched, "order_details")
        agg_query = self.write_to_console(aggregated, "order_aggregates")
        
        # Wait for termination
        self.spark.streams.awaitAnyTermination()


def main():
    config = {
        "bootstrap_servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        "orders_topic": os.getenv("KAFKA_ORDERS_TOPIC", "orders"),
        "checkpoint_location": os.getenv("CHECKPOINT_LOCATION", "/tmp/checkpoints/orders"),
        "snowflake": {
            "url": os.getenv("SNOWFLAKE_URL"),
            "user": os.getenv("SNOWFLAKE_USER"),
            "password": os.getenv("SNOWFLAKE_PASSWORD"),
            "database": os.getenv("SNOWFLAKE_DATABASE", "STREAMING"),
            "schema": os.getenv("SNOWFLAKE_SCHEMA", "REALTIME"),
            "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE", "STREAMING_WH")
        }
    }
    
    processor = OrderProcessor(config)
    processor.run()


if __name__ == "__main__":
    main()
