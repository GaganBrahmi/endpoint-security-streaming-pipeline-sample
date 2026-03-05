"""
Batch test: Create a simple Iceberg table on S3 Tables and write sample data.

Supports two catalog approaches via --catalog-mode:
  rest  — RESTCatalog
  glue  — GlueCatalog + glue.id

All catalog configs are set in Python .config() calls so this script is
self-contained and doesn't depend on sparkSubmitParameters.
"""
import argparse
import logging
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType
from datetime import datetime, timedelta

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

CATALOG = "my_rest_catalog"


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--table-bucket-name", required=True)
    p.add_argument("--namespace", default="endpoint_security")
    p.add_argument("--table-name", default="batch_test")
    p.add_argument("--region", default="us-east-1")
    p.add_argument("--catalog-mode", choices=["rest", "glue"], default="rest",
                   help="rest=RESTCatalog, glue=GlueCatalog+glue.id (needs warehouse)")
    return p.parse_args()


def main():
    args = parse_args()

    # Append timestamp to table name to guarantee a fresh table every run
    import time
    ts = str(int(time.time()))
    unique_table = f"{args.table_name}_{ts}"
    fq_table = f"{CATALOG}.{args.namespace}.{unique_table}"

    import boto3
    account = boto3.client("sts").get_caller_identity()["Account"]

    logger.info("=" * 60)
    logger.info("Batch Test: Write to S3 Tables (Iceberg)")
    logger.info("=" * 60)
    logger.info(f"Table: {fq_table} (unique per run)")
    logger.info(f"Catalog mode: {args.catalog_mode}")

    builder = SparkSession.builder \
        .appName("BatchTestS3Tables") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config(f"spark.sql.catalog.{CATALOG}", "org.apache.iceberg.spark.SparkCatalog")

    if args.catalog_mode == "rest":
        warehouse = f"{account}:s3tablescatalog/{args.table_bucket_name}"
        logger.info(f"Warehouse: {warehouse}")
        builder = builder \
            .config(f"spark.sql.catalog.{CATALOG}.type", "rest") \
            .config(f"spark.sql.catalog.{CATALOG}.uri", f"https://glue.{args.region}.amazonaws.com/iceberg") \
            .config("spark.sql.defaultCatalog", CATALOG) \
            .config(f"spark.sql.catalog.{CATALOG}.warehouse", warehouse) \
            .config(f"spark.sql.catalog.{CATALOG}.rest.sigv4-enabled", "true") \
            .config(f"spark.sql.catalog.{CATALOG}.rest.signing-name", "glue") \
            .config(f"spark.sql.catalog.{CATALOG}.rest.signing-region", args.region)
    else:
        glue_id = f"{account}:s3tablescatalog/{args.table_bucket_name}"
        logger.info(f"Glue ID: {glue_id}")
        builder = builder \
            .config(f"spark.sql.catalog.{CATALOG}.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog") \
            .config(f"spark.sql.catalog.{CATALOG}.glue.id", glue_id) \
            .config(f"spark.sql.catalog.{CATALOG}.client.region", args.region)

    spark = builder.getOrCreate()

    # Log active catalog config for debugging
    for key in [
        f"spark.sql.catalog.{CATALOG}",
        f"spark.sql.catalog.{CATALOG}.type",
        f"spark.sql.catalog.{CATALOG}.catalog-impl",
        f"spark.sql.catalog.{CATALOG}.warehouse",
        f"spark.sql.catalog.{CATALOG}.uri",
        f"spark.sql.catalog.{CATALOG}.rest.sigv4-enabled",
        f"spark.sql.catalog.{CATALOG}.rest.signing-name",
        f"spark.sql.catalog.{CATALOG}.glue.id",
        f"spark.sql.catalog.{CATALOG}.client.region",
        f"spark.sql.defaultCatalog",
    ]:
        val = spark.conf.get(key, "<not set>")
        logger.info(f"  {key} = {val}")

    # Create namespace
    logger.info(f"Creating namespace {CATALOG}.{args.namespace}...")
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.{args.namespace}")
    logger.info("Namespace ready")

    # Create table
    logger.info(f"Creating table {fq_table}...")
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {fq_table} (
            id INT,
            name STRING,
            email STRING,
            created_date DATE
        )
        USING iceberg
        LOCATION 's3://dummy-bucket/iceberg-warehouse/'
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
