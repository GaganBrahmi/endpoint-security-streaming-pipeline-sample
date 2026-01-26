"""
Configuration module for PyFlink aggregation pipeline.

This module defines the configuration dataclass used across all aggregation components.
"""

from dataclasses import dataclass


@dataclass
class AggregationConfig:
    """
    Configuration for PyFlink aggregation pipeline.
    
    Attributes:
        kafka_bootstrap_servers: Comma-separated Kafka broker addresses
        kafka_topic: Kafka topic name to consume from
        kafka_group_id: Kafka consumer group ID
        warehouse_location: S3 warehouse location for Iceberg tables
        catalog_name: Iceberg catalog name (typically 'glue_catalog')
        database_name: Database name within the catalog
        table_name: Base table name for raw events
        aws_region: AWS region for Glue Catalog
    """
    kafka_bootstrap_servers: str
    kafka_topic: str
    kafka_group_id: str
    warehouse_location: str
    catalog_name: str
    database_name: str
    table_name: str
    aws_region: str
    
    def get_full_table_name(self, table_name: str) -> str:
        """
        Get fully qualified table name.
        
        Args:
            table_name: Table name without catalog/database prefix
            
        Returns:
            Fully qualified table name (catalog.database.table)
        """
        return f"{self.catalog_name}.{self.database_name}.{table_name}"
