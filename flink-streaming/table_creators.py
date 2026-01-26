"""
Table creation module for PyFlink aggregation pipeline.

This module contains functions to create Kafka sources, Iceberg catalogs,
databases, and tables.
"""

import logging
from pyflink.table import TableEnvironment

logger = logging.getLogger(__name__)


def create_kafka_source_table(
    table_env: TableEnvironment,
    kafka_bootstrap_servers: str,
    kafka_topic: str,
    kafka_group_id: str
):
    """
    Create Kafka source table for consuming endpoint security events.
    
    Args:
        table_env: PyFlink TableEnvironment
        kafka_bootstrap_servers: Kafka broker addresses
        kafka_topic: Kafka topic name
        kafka_group_id: Kafka consumer group ID
    """
    logger.info(f"Creating Kafka source table for topic: {kafka_topic}")
    
    kafka_ddl = f"""
        CREATE TABLE kafka_source (
            event_id STRING,
            customer_id STRING,
            tenant_id STRING,
            device_id STRING,
            device_name STRING,
            device_type STRING,
            event_type STRING,
            event_category STRING,
            severity STRING,
            `timestamp` STRING,
            `user` STRING,
            process_name STRING,
            file_path STRING,
            `action` STRING,
            `result` STRING,
            ip_address STRING,
            os STRING,
            os_version STRING,
            threat_detected BOOLEAN,
            threat_type STRING,
            ingestion_timestamp STRING,
            `source` STRING,
            `version` STRING,
            event_time AS TO_TIMESTAMP(`timestamp`, 'yyyy-MM-dd''T''HH:mm:ss''Z'''),
            WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
        ) WITH (
            'connector' = 'kafka',
            'topic' = '{kafka_topic}',
            'properties.bootstrap.servers' = '{kafka_bootstrap_servers}',
            'properties.group.id' = '{kafka_group_id}',
            'scan.startup.mode' = 'latest-offset',
            'format' = 'json',
            'json.fail-on-missing-field' = 'false',
            'json.ignore-parse-errors' = 'true'
        )
    """
    
    table_env.execute_sql(kafka_ddl)
    logger.info("✅ Kafka source table created")


def create_iceberg_catalog(
    table_env: TableEnvironment,
    catalog_name: str,
    warehouse_location: str,
    aws_region: str
):
    """
    Create Iceberg catalog using AWS Glue Data Catalog.
    
    Args:
        table_env: PyFlink TableEnvironment
        catalog_name: Name for the Iceberg catalog
        warehouse_location: S3 warehouse location
        aws_region: AWS region
    """
    logger.info(f"Creating Iceberg catalog: {catalog_name}")
    
    # Using working configuration from sample code
    catalog_ddl = f"""
        CREATE CATALOG {catalog_name} WITH (
            'type' = 'iceberg',
            'property-version' = '1',
            'catalog-impl' = 'org.apache.iceberg.aws.glue.GlueCatalog',
            'io-impl' = 'org.apache.iceberg.aws.s3.S3FileIO',
            'warehouse' = '{warehouse_location}',
            'aws.region' = '{aws_region}'
        )
    """
    
    table_env.execute_sql(catalog_ddl)
    logger.info(f"✅ Iceberg catalog '{catalog_name}' created")
    
    # Use the catalog
    table_env.use_catalog(catalog_name)
    logger.info(f"Using catalog: {catalog_name}")


def create_iceberg_database(table_env: TableEnvironment, catalog_name: str, database_name: str):
    """
    Create Iceberg database if it doesn't exist.
    
    Args:
        table_env: PyFlink TableEnvironment
        catalog_name: Catalog name
        database_name: Database name to create
    """
    logger.info(f"Creating database: {database_name}")
    
    database_ddl = f"CREATE DATABASE IF NOT EXISTS {database_name}"
    table_env.execute_sql(database_ddl)
    
    # Use the database
    table_env.use_database(database_name)
    logger.info(f"✅ Using database: {database_name}")


def create_raw_events_table(
    table_env: TableEnvironment,
    catalog_name: str,
    database_name: str,
    table_name: str
):
    """
    Create Iceberg table for storing raw endpoint security events.
    
    Args:
        table_env: PyFlink TableEnvironment
        catalog_name: Catalog name
        database_name: Database name
        table_name: Table name for raw events
    """
    logger.info(f"Creating raw events table: {table_name}")
    
    iceberg_ddl = f"""
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
            `timestamp` STRING,
            `user` STRING,
            process_name STRING,
            file_path STRING,
            `action` STRING,
            `result` STRING,
            ip_address STRING,
            os STRING,
            os_version STRING,
            threat_detected BOOLEAN,
            threat_type STRING,
            ingestion_timestamp STRING,
            `source` STRING,
            `version` STRING,
            event_time TIMESTAMP(3),
            processing_time TIMESTAMP(3),
            event_year INT,
            event_month INT,
            event_day INT,
            event_hour INT
        ) PARTITIONED BY (event_year, event_month, event_day)
        WITH (
            'format-version' = '2',
            'write.upsert.enabled' = 'false'
        )
    """
    
    table_env.execute_sql(iceberg_ddl)
    logger.info(f"✅ Raw events table '{table_name}' created")


def create_aggregation_table(
    table_env: TableEnvironment,
    table_name: str,
    window_type: str
):
    """
    Create Iceberg table for storing aggregated threat metrics.
    
    Args:
        table_env: PyFlink TableEnvironment
        table_name: Table name for aggregations
        window_type: Type of window ('tumbling', 'sliding', 'session')
    """
    logger.info(f"Creating {window_type} aggregation table: {table_name}")
    
    # Base columns for all aggregation types
    base_columns = """
        customer_id STRING,
        device_id STRING,
        device_name STRING,
        window_start TIMESTAMP(3),
        window_end TIMESTAMP(3),
        total_events BIGINT,
        threat_count BIGINT,
        threat_percentage DOUBLE,
        high_severity_count BIGINT,
        critical_severity_count BIGINT,
        medium_severity_count BIGINT,
        low_severity_count BIGINT,
        unique_threat_types BIGINT,
        blocked_count BIGINT,
        allowed_count BIGINT,
        failed_login_count BIGINT,
        malware_count BIGINT,
        data_exfiltration_count BIGINT
    """
    
    # Add window-specific columns
    if window_type == 'tumbling':
        specific_columns = ",\n        window_duration_minutes INT"
    elif window_type == 'sliding':
        specific_columns = ",\n        window_duration_minutes INT,\n        slide_interval_minutes INT"
    else:  # session
        specific_columns = ",\n        session_duration_seconds BIGINT,\n        event_burst_detected BOOLEAN"
    
    iceberg_ddl = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            {base_columns}{specific_columns},
            PRIMARY KEY (customer_id, device_id, window_start) NOT ENFORCED
        ) PARTITIONED BY (customer_id)
        WITH (
            'format-version' = '2',
            'write.upsert.enabled' = 'true'
        )
    """
    
    table_env.execute_sql(iceberg_ddl)
    logger.info(f"✅ {window_type.capitalize()} aggregation table '{table_name}' created")
