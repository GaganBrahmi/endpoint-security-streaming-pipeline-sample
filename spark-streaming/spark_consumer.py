#!/usr/bin/env python3
"""
PySpark Streaming Consumer: Endpoint Security Logs to Iceberg

This PySpark streaming job reads endpoint security events from Kafka
and writes them to an Iceberg table on S3 for analytics.

Architecture:
- Source: Kafka topic "endpoint_logs"
- Processing: PySpark Structured Streaming
- Sink: Iceberg table on S3 with Glue Catalog
- Format: Iceberg (optimized for analytics)
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_timestamp, current_timestamp,
    year, month, day, hour
)
from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType,
    BooleanType, IntegerType
)
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration - Update these for your environment
KAFKA_BOOTSTRAP_SERVERS = "b-2.democluster.c3otci.c12.kafka.us-east-1.amazonaws.com:9092,b-1.democluster.c3otci.c12.kafka.us-east-1.amazonaws.com:9092"
KAFKA_TOPIC = "endpoint_logs"
WAREHOUSE_LOCATION = "s3://gbrahmi-demo/data-gen/data/raw/data-generator"
CATALOG_NAME = "glue_catalog"
DATABASE_NAME = "data_generator"
TABLE_NAME = "endpoint_data"


def create_spark_session(app_name: str = "EndpointLogsConsumer") -> SparkSession:
    """
    Create Spark session with Iceberg and Glue Catalog configuration.

    Args:
        app_name: Name of the Spark application

    Returns:
        Configured SparkSession
    """
    spark = SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config(f"spark.sql.catalog.{CATALOG_NAME}", "org.apache.iceberg.spark.SparkCatalog") \
        .config(f"spark.sql.catalog.{CATALOG_NAME}.warehouse", WAREHOUSE_LOCATION) \
        .config(f"spark.sql.catalog.{CATALOG_NAME}.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog") \
        .config(f"spark.sql.catalog.{CATALOG_NAME}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
        .getOrCreate()

    logger.info(f"Spark session created: {app_name}")
    return spark


def define_event_schema() -> StructType:
    """
    Define schema for endpoint security events.

    Returns:
        StructType schema matching Lambda producer events
    """
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
        StructField("ingestion_timestamp", StringType(), True),
        StructField("source", StringType(), True),
        StructField("version", StringType(), True)
    ])


def read_from_kafka(spark: SparkSession, kafka_bootstrap_servers: str,
                    kafka_topic: str) -> "DataFrame":
    """
    Read streaming data from Kafka topic.

    Args:
        spark: SparkSession
        kafka_bootstrap_servers: Kafka broker addresses
        kafka_topic: Kafka topic name

    Returns:
        Streaming DataFrame
    """
    logger.info(f"Reading from Kafka: {kafka_bootstrap_servers}, topic: {kafka_topic}")

    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("subscribe", kafka_topic) \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .load()

    return df


def transform_events(df: "DataFrame", event_schema: StructType) -> "DataFrame":
    """
    Transform raw Kafka messages into structured endpoint security events.

    Args:
        df: Raw Kafka DataFrame
        event_schema: Schema for parsing JSON events

    Returns:
        Transformed DataFrame with parsed and enriched fields
    """
    logger.info("Transforming events...")

    # Parse JSON from Kafka value
    parsed_df = df.select(
        from_json(col("value").cast("string"), event_schema).alias("data"),
        col("timestamp").alias("kafka_timestamp"),
        col("partition").alias("kafka_partition"),
        col("offset").alias("kafka_offset")
    )

    # Flatten structure and add processing metadata
    transformed_df = parsed_df.select(
        col("data.*"),
        col("kafka_timestamp"),
        col("kafka_partition"),
        col("kafka_offset"),
        current_timestamp().alias("processing_timestamp")
    )

    # Convert timestamp string to timestamp type
    transformed_df = transformed_df.withColumn(
        "event_timestamp",
        to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss'Z'")
    )

    # Add partitioning columns for efficient querying
    transformed_df = transformed_df \
        .withColumn("event_year", year(col("event_timestamp"))) \
        .withColumn("event_month", month(col("event_timestamp"))) \
        .withColumn("event_day", day(col("event_timestamp"))) \
        .withColumn("event_hour", hour(col("event_timestamp")))

    return transformed_df


def create_iceberg_table_if_not_exists(spark: SparkSession, table_name: str, warehouse_path: str):
    """
    Create Iceberg table if it doesn't exist.

    Args:
        spark: SparkSession
        table_name: Fully qualified table name (catalog.database.table)
        warehouse_path: S3 warehouse path
    """
    # Parse table name
    parts = table_name.split('.')
    if len(parts) != 3:
        raise ValueError(f"Table name must be in format catalog.database.table, got: {table_name}")

    catalog, database, table = parts

    logger.info(f"Checking if table {table_name} exists...")

    try:
        # Check if database exists, create if not
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {catalog}.{database}")
        logger.info(f"Database {catalog}.{database} ready")

        # Check if table exists
        tables = spark.sql(f"SHOW TABLES IN {catalog}.{database}").collect()
        table_exists = any(row.tableName == table for row in tables)

        if not table_exists:
            logger.info(f"Creating Iceberg table {table_name}...")

            # Create table with schema
            spark.sql(f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
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
                LOCATION '{warehouse_path}/{database}.db/{table}'
            """)

            logger.info(f"✅ Created Iceberg table {table_name}")
        else:
            logger.info(f"✅ Table {table_name} already exists")

    except Exception as e:
        logger.error(f"Error creating table: {e}")
        raise


def write_to_iceberg(df: "DataFrame", table_name: str, checkpoint_location: str):
    """
    Write streaming data to Iceberg table with Glue Catalog.

    Args:
        df: Transformed DataFrame
        table_name: Fully qualified Iceberg table name (catalog.database.table)
        checkpoint_location: S3 path for streaming checkpoints
    """
    logger.info(f"Writing to Iceberg table: {table_name}")

    query = df.writeStream \
        .format("iceberg") \
        .outputMode("append") \
        .option("checkpointLocation", checkpoint_location) \
        .option("path", table_name) \
        .option("fanout-enabled", "true") \
        .partitionBy("event_year", "event_month", "event_day") \
        .trigger(processingTime="30 seconds") \
        .start()

    logger.info(f"Streaming query started. Checkpoint: {checkpoint_location}")
    return query


def main():
    """
    Main streaming pipeline execution.
    """
    # Build derived configuration
    ICEBERG_TABLE = f"{CATALOG_NAME}.{DATABASE_NAME}.{TABLE_NAME}"
    CHECKPOINT_LOCATION = f"{WAREHOUSE_LOCATION}/checkpoints/{DATABASE_NAME}/{TABLE_NAME}"

    logger.info("=" * 60)
    logger.info("Endpoint Security Logs Streaming Pipeline")
    logger.info("=" * 60)
    logger.info(f"Kafka: {KAFKA_BOOTSTRAP_SERVERS}")
    logger.info(f"Topic: {KAFKA_TOPIC}")
    logger.info(f"Iceberg Table: {ICEBERG_TABLE}")
    logger.info(f"Checkpoint: {CHECKPOINT_LOCATION}")

    try:
        # Create Spark session
        spark = create_spark_session()

        # Create Iceberg table if it doesn't exist
        create_iceberg_table_if_not_exists(spark, ICEBERG_TABLE, WAREHOUSE_LOCATION)

        # Define event schema
        event_schema = define_event_schema()

        # Read from Kafka
        raw_df = read_from_kafka(spark, KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC)

        # Transform events
        transformed_df = transform_events(raw_df, event_schema)

        # Write to Iceberg
        query = write_to_iceberg(transformed_df, ICEBERG_TABLE, CHECKPOINT_LOCATION)

        logger.info("Streaming pipeline running. Press Ctrl+C to stop...")
        query.awaitTermination()

    except KeyboardInterrupt:
        logger.info("Streaming pipeline stopped by user")
    except Exception as e:
        logger.error(f"Pipeline error: {e}", exc_info=True)
        raise
    finally:
        if 'spark' in locals():
            spark.stop()
            logger.info("Spark session stopped")


if __name__ == "__main__":
    main()