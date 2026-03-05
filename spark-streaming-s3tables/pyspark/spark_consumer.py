#!/usr/bin/env python3
"""
PySpark Streaming Consumer: Endpoint Security Logs → S3 Tables (Iceberg)

Reads endpoint security events from MSK (Kafka) and writes them to an
Iceberg table in S3 Tables via the Glue Iceberg REST catalog.

Usage (submitted via EMR Serverless):
    --kafka-bootstrap-servers  MSK bootstrap servers
    --kafka-topic              Kafka topic (default: endpoint_logs)
    --table-bucket-name        S3 Table bucket name
    --namespace                S3 Tables namespace (default: endpoint_security)
    --table-name               Table name (default: endpoint_events)
    --checkpoint-location      S3 path for streaming checkpoints
    --region                   AWS region (default: us-east-1)
"""

import argparse
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_timestamp, current_timestamp,
    year, month, day, hour,
)
from pyspark.sql.types import (
    StructType, StructField, StringType, BooleanType,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

CATALOG = "s3tablescatalog"


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--kafka-bootstrap-servers", required=True)
    p.add_argument("--kafka-topic", default="endpoint_logs")
    p.add_argument("--table-bucket-name", required=True)
    p.add_argument("--namespace", default="endpoint_security")
    p.add_argument("--table-name", default="endpoint_events")
    p.add_argument("--checkpoint-location", required=True)
    p.add_argument("--region", default="us-east-1")
    return p.parse_args()


def event_schema() -> StructType:
    """Schema matching the 23-field endpoint security events from FakeDataGenerator."""
    return StructType([
        StructField("event_id", StringType(), False),
        StructField("customer_id", StringType(), False),
        StructField("tenant_id", StringType(), False),
        StructField("device_id", StringType(), False),
        StructField("device_name", StringType(), True),
        StructField("device_type", StringType(), True),
        StructField("event_type", StringType(), False),
        StructField("event_category", StringType(), True),
        StructField("severity", StringType(), False),
        StructField("timestamp", StringType(), False),
        StructField("user", StringType(), True),
        StructField("process_name", StringType(), True),
        StructField("file_path", StringType(), True),
        StructField("action", StringType(), True),
        StructField("result", StringType(), True),
        StructField("ip_address", StringType(), True),
        StructField("os", StringType(), True),
        StructField("os_version", StringType(), True),
        StructField("threat_detected", BooleanType(), True),
        StructField("threat_type", StringType(), True),
        # Enrichment fields added by EndpointLogsDataGenerator
        StructField("ingestion_timestamp", StringType(), True),
        StructField("source", StringType(), True),
        StructField("version", StringType(), True),
    ])


def main():
    args = parse_args()

    fq_table = f"{CATALOG}.{args.namespace}.{args.table_name}"

    logger.info("=" * 60)
    logger.info("Spark Streaming → S3 Tables (Iceberg)")
    logger.info("=" * 60)
    logger.info(f"Kafka:      {args.kafka_bootstrap_servers}")
    logger.info(f"Topic:      {args.kafka_topic}")
    logger.info(f"Table:      {fq_table}")
    logger.info(f"Checkpoint: {args.checkpoint_location}")

    spark = SparkSession.builder \
        .appName("EndpointSecurityStreaming") \
        .getOrCreate()

    # Create namespace if not exists
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.{args.namespace}")
    logger.info(f"Namespace {CATALOG}.{args.namespace} ready")

    # Create table if not exists
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {fq_table} (
            event_id STRING,
            customer_id STRING,
            tenant_id STRING,
            device_id STRING,
            device_name STRING,
            device_type STRING,
            event_type STRING,
            event_category STRING,
            severity STRING,
            timestamp STRING,
            user STRING,
            process_name STRING,
            file_path STRING,
            action STRING,
            result STRING,
            ip_address STRING,
            os STRING,
            os_version STRING,
            threat_detected BOOLEAN,
            threat_type STRING,
            ingestion_timestamp STRING,
            source STRING,
            version STRING,
            kafka_timestamp TIMESTAMP,
            kafka_partition INT,
            kafka_offset BIGINT,
            processing_timestamp TIMESTAMP,
            event_timestamp TIMESTAMP,
            event_year INT,
            event_month INT,
            event_day INT,
            event_hour INT
        )
        USING iceberg
        PARTITIONED BY (event_year, event_month, event_day)
        LOCATION 's3://dummy-location/{args.namespace}/{args.table_name}'
    """)
    logger.info(f"Table {fq_table} ready")

    # Read from Kafka (MSK with IAM auth)
    raw_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", args.kafka_bootstrap_servers) \
        .option("subscribe", args.kafka_topic) \
        .option("startingOffsets", "earliest") \
        .option("failOnDataLoss", "false") \
        .option("kafka.security.protocol", "SASL_SSL") \
        .option("kafka.sasl.mechanism", "AWS_MSK_IAM") \
        .option("kafka.sasl.jaas.config",
                "software.amazon.msk.auth.iam.IAMLoginModule required;") \
        .option("kafka.sasl.client.callback.handler.class",
                "software.amazon.msk.auth.iam.IAMClientCallbackHandler") \
        .load()

    schema = event_schema()

    # Parse JSON, flatten, add partitioning columns
    parsed = raw_df.select(
        from_json(col("value").cast("string"), schema).alias("data"),
        col("timestamp").alias("kafka_timestamp"),
        col("partition").alias("kafka_partition"),
        col("offset").alias("kafka_offset"),
    )

    transformed = parsed.select(
        col("data.*"),
        col("kafka_timestamp"),
        col("kafka_partition"),
        col("kafka_offset"),
        current_timestamp().alias("processing_timestamp"),
    ).withColumn(
        "event_timestamp",
        to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss'Z'"),
    ).withColumn("event_year", year(col("event_timestamp"))) \
     .withColumn("event_month", month(col("event_timestamp"))) \
     .withColumn("event_day", day(col("event_timestamp"))) \
     .withColumn("event_hour", hour(col("event_timestamp")))

    # Write to Iceberg table
    query = transformed.writeStream \
        .format("iceberg") \
        .outputMode("append") \
        .option("checkpointLocation", args.checkpoint_location) \
        .option("path", fq_table) \
        .option("fanout-enabled", "true") \
        .trigger(processingTime="30 seconds") \
        .start()

    logger.info("Streaming query started. Waiting for termination...")
    query.awaitTermination()


if __name__ == "__main__":
    main()
