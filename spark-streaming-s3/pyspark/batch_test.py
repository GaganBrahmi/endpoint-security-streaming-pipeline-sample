"""
Batch test: Create a simple Iceberg table on standard S3 and write sample data.

Uses GlueCatalog with a standard S3 warehouse location. No S3 Tables,
no REST catalog, no Lake Formation, no LOCATION workaround needed.

All catalog configs are set in Python .config() calls so this script is
self-contained and doesn't depend on sparkSubmitParameters.
"""
import argparse
import logging
import time
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType
from datetime import datetime, timedelta

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

CATALOG = "glue_catalog"


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--warehouse-location", required=True,
                   help="S3 warehouse path, e.g. s3://bucket/iceberg/")
    p.add_argument("--database", default="endpoint_security")
    p.add_argument("--table-name", default="batch_test")
    p.add_argument("--region", default="us-east-1")
    return p.parse_args()


def main():
    args = parse_args()

    # Append timestamp to table name to guarantee a fresh table every run
    ts = str(int(time.time()))
    unique_table = f"{args.table_name}_{ts}"
    fq_table = f"{CATALOG}.{args.database}.{unique_table}"

    logger.info("=" * 60)
    logger.info("Batch Test: Write to Standard S3 (Iceberg via GlueCatalog)")
    logger.info("=" * 60)
    logger.info(f"Table: {fq_table} (unique per run)")
    logger.info(f"Warehouse: {args.warehouse_location}")

    spark = SparkSession.builder \
        .appName("BatchTestS3Iceberg") \
        .config("spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config(f"spark.sql.catalog.{CATALOG}",
                "org.apache.iceberg.spark.SparkCatalog") \
        .config(f"spark.sql.catalog.{CATALOG}.catalog-impl",
                "org.apache.iceberg.aws.glue.GlueCatalog") \
        .config(f"spark.sql.catalog.{CATALOG}.warehouse",
                args.warehouse_location) \
        .config(f"spark.sql.catalog.{CATALOG}.client.region",
                args.region) \
        .getOrCreate()

    # Log active catalog config for debugging
    for key in [
        f"spark.sql.catalog.{CATALOG}",
        f"spark.sql.catalog.{CATALOG}.catalog-impl",
        f"spark.sql.catalog.{CATALOG}.warehouse",
        f"spark.sql.catalog.{CATALOG}.client.region",
    ]:
        val = spark.conf.get(key, "<not set>")
        logger.info(f"  {key} = {val}")

    # Create database
    logger.info(f"Creating database {CATALOG}.{args.database}...")
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {CATALOG}.{args.database}")
    logger.info("Database ready")

    # Create table — no LOCATION workaround needed with standard GlueCatalog + warehouse
    logger.info(f"Creating table {fq_table}...")
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {fq_table} (
            id INT,
            name STRING,
            email STRING,
            created_date DATE
        )
        USING iceberg
    """)
    logger.info("Table created")

    # Generate sample data
    base_date = datetime.now().date()
    data = [
        (1, "Alice", "alice@example.com", base_date - timedelta(days=3)),
        (2, "Bob", "bob@example.com", base_date - timedelta(days=2)),
        (3, "Carol", "carol@example.com", base_date - timedelta(days=1)),
        (4, "Dave", "dave@example.com", base_date),
        (5, "Eve", "eve@example.com", base_date),
    ]
    schema = StructType([
        StructField("id", IntegerType(), False),
        StructField("name", StringType(), False),
        StructField("email", StringType(), False),
        StructField("created_date", DateType(), False),
    ])
    df = spark.createDataFrame(data, schema)
    logger.info(f"Sample data: {df.count()} rows")
    df.show()

    # Insert data
    logger.info("Inserting data...")
    df.createOrReplaceTempView("temp_data")
    spark.sql(f"INSERT INTO {fq_table} SELECT * FROM temp_data")
    logger.info("Data inserted")

    # Verify
    result = spark.table(fq_table)
    logger.info(f"Verification: {result.count()} rows in table")
    result.show()

    spark.stop()
    logger.info("Batch test completed successfully!")


if __name__ == "__main__":
    main()
