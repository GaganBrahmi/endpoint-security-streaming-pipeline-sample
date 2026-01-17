# Spark Streaming Consumer

PySpark Structured Streaming job that reads endpoint security events from Kafka and writes them to an Iceberg table on S3 for analytics.

## ⚠️ Security Notice

**This code uses unauthenticated/plaintext Kafka connections for demonstration purposes.**

For production use, add Kafka security configuration to `spark_consumer.py`:

```python
# Add to Kafka read options
.option("kafka.security.protocol", "SASL_SSL") \
.option("kafka.sasl.mechanism", "SCRAM-SHA-512") \
.option("kafka.sasl.jaas.config", 
    'org.apache.kafka.common.security.scram.ScramLoginModule required '
    'username="your-username" password="your-password";')
```

See the main [README.md](../README.md) for complete security configuration examples.

## Overview

This consumer uses PySpark Structured Streaming to:
- Read streaming data from Kafka topic
- Parse and validate JSON events
- Add processing metadata and partitioning columns
- Write to Iceberg table with time-based partitioning
- Integrate with AWS Glue Catalog for metadata management

## Architecture

![Endpoint Security Spark Streaming Pipeline Architecture](../images/endpoint-security-streaming-pipeline-architecture.png)

## Prerequisites

1. **Ingestion Setup**: Complete the ingestion setup in [../common/README.md](../common/README.md)
2. **Kafka Topic**: Ensure `endpoint_logs` topic exists and has data
3. **AWS Services**:
   - S3 bucket for data storage
   - AWS Glue Data Catalog database
   - EMR cluster or local Spark environment

## Configuration

### Update Configuration in `spark_consumer.py`

```python
# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS = "your-kafka-broker:9092"
KAFKA_TOPIC = "endpoint_logs"

# Iceberg Configuration
WAREHOUSE_LOCATION = "s3://your-bucket/warehouse"
CATALOG_NAME = "glue_catalog"
DATABASE_NAME = "your_database"
TABLE_NAME = "endpoint_data"

# Derived values (automatically computed)
# ICEBERG_TABLE = f"{CATALOG_NAME}.{DATABASE_NAME}.{TABLE_NAME}"
# CHECKPOINT_LOCATION = f"{WAREHOUSE_LOCATION}/checkpoints/{DATABASE_NAME}/{TABLE_NAME}"
```

The configuration is modular - update the base values and the table name and checkpoint location are automatically derived.

## Deployment

### Run on EMR

Since all Spark configurations are defined in `spark_consumer.py`, you only need to provide the required packages:

```bash
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --packages org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.4.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0 \
  spark_consumer.py
```

**Alternative (with explicit configs):** Override configurations via command line:

```bash
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --packages org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.4.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0 \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.glue_catalog=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.glue_catalog.warehouse=s3://your-bucket/warehouse \
  --conf spark.sql.catalog.glue_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog \
  spark_consumer.py
```

**Note:** Command-line `--conf` options will override configurations in the script.

### Run Locally (for testing)

```bash
python spark_consumer.py
```

## Features

### Automatic Table Creation

The Spark consumer automatically creates the Iceberg table if it doesn't exist!

When you run `spark_consumer.py`, it will:
1. Check if the database exists (creates if needed)
2. Check if the table exists (creates if needed)
3. Start streaming data

### Partitioning Strategy

- Partitioned by: `event_year`, `event_month`, `event_day`
- Enables efficient time-range queries
- Optimizes storage and query performance

### Processing Features

- Parses JSON events from Kafka
- Adds Kafka metadata (timestamp, partition, offset)
- Adds processing timestamp
- Extracts date components for partitioning
- Handles schema validation
- Checkpoint-based fault tolerance

## Usage

### End-to-End Pipeline

1. **Start Kafka** (if running locally):
   ```bash
   # Start Zookeeper
   zookeeper-server-start.sh config/zookeeper.properties
   
   # Start Kafka
   kafka-server-start.sh config/server.properties
   
   # Create topic
   kafka-topics.sh --create --topic endpoint_logs --bootstrap-server localhost:9092
   ```

2. **Start PySpark Consumer:**
   ```bash
   python spark_consumer.py
   ```

3. **Send Events** (from common directory):
   ```bash
   # Deploy and trigger data generator
   cd ../common
   ./deploy_data_generator.sh
   
   aws lambda invoke \
     --function-name endpoint-logs-data-generator \
     response.json
   ```

4. **Monitor Data Flow:**
   ```bash
   # Check Kafka topic
   kafka-console-consumer.sh \
     --bootstrap-server localhost:9092 \
     --topic endpoint_logs \
     --from-beginning
   
   # Check Spark consumer logs
   # Check Iceberg table
   ```

## Querying Data

### Using PySpark

```python
from pyspark.sql import SparkSession

# Configuration - match with spark_consumer.py
CATALOG_NAME = "glue_catalog"
DATABASE_NAME = "your_database"
TABLE_NAME = "endpoint_data"

spark = SparkSession.builder \
    .config(f"spark.sql.catalog.{CATALOG_NAME}", "org.apache.iceberg.spark.SparkCatalog") \
    .getOrCreate()

# Query recent high-severity events
spark.sql(f"""
    SELECT 
        event_timestamp,
        customer_id,
        device_name,
        event_type,
        severity,
        threat_type,
        result
    FROM {CATALOG_NAME}.{DATABASE_NAME}.{TABLE_NAME}
    WHERE severity = 'HIGH'
      AND event_year = 2026
      AND event_month = 1
    ORDER BY event_timestamp DESC
    LIMIT 10
""").show()
```

### Analytics Queries

**Note:** Replace `glue_catalog.your_database.endpoint_data` with your actual catalog, database, and table names.

#### 1. Threat Detection Summary
```sql
SELECT 
    DATE(event_timestamp) as event_date,
    COUNT(*) as total_events,
    SUM(CASE WHEN threat_detected THEN 1 ELSE 0 END) as threats_detected,
    COUNT(DISTINCT device_id) as unique_devices,
    COUNT(DISTINCT customer_id) as unique_customers
FROM glue_catalog.your_database.endpoint_data
WHERE event_year = 2026 AND event_month = 1
GROUP BY DATE(event_timestamp)
ORDER BY event_date DESC;
```

#### 2. Top Threat Types
```sql
SELECT 
    threat_type,
    COUNT(*) as occurrence_count,
    COUNT(DISTINCT device_id) as affected_devices,
    COUNT(DISTINCT customer_id) as affected_customers
FROM glue_catalog.your_database.endpoint_data
WHERE threat_detected = true
  AND event_year = 2026
GROUP BY threat_type
ORDER BY occurrence_count DESC
LIMIT 10;
```

#### 3. Device Security Posture
```sql
SELECT 
    device_id,
    device_name,
    device_type,
    COUNT(*) as total_events,
    SUM(CASE WHEN severity = 'HIGH' THEN 1 ELSE 0 END) as high_severity_events,
    SUM(CASE WHEN threat_detected THEN 1 ELSE 0 END) as threats_detected,
    MAX(event_timestamp) as last_event_time
FROM glue_catalog.your_database.endpoint_data
WHERE event_year = 2026 AND event_month = 1
GROUP BY device_id, device_name, device_type
HAVING SUM(CASE WHEN threat_detected THEN 1 ELSE 0 END) > 0
ORDER BY threats_detected DESC
LIMIT 20;
```

#### 4. Blocked Actions Analysis
```sql
SELECT 
    action,
    event_type,
    COUNT(*) as blocked_count,
    COUNT(DISTINCT device_id) as devices_affected
FROM glue_catalog.your_database.endpoint_data
WHERE result = 'blocked'
  AND event_year = 2026
GROUP BY action, event_type
ORDER BY blocked_count DESC;
```

## Benefits

### Real-Time Processing
- Events processed within seconds of generation
- Immediate threat detection and response
- Continuous monitoring of endpoint security

### Scalable Architecture
- PySpark handles high-throughput streaming
- Iceberg optimizes storage and query performance
- Partitioning minimizes query costs

### Analytics-Ready
- Data immediately available in Glue Catalog
- Query with Spark, Trino, or Athena
- Time-travel capabilities with Iceberg
- Optimized for time-range queries

## Monitoring

### Spark Metrics
- Streaming query progress
- Processing rate
- Batch duration
- Input/output rows

### CloudWatch Logs
- Spark driver/executor logs
- Error tracking
- Performance metrics

## Troubleshooting

### Kafka Connection Failed
- Verify bootstrap servers address
- Check security group rules
- Ensure Kafka topic exists

### Iceberg Write Errors
- Verify S3 bucket permissions
- Check Glue Catalog database exists
- Ensure table schema matches data

### Checkpoint Errors
- Verify S3 checkpoint location permissions
- Clear old checkpoints if schema changed
- Ensure unique checkpoint path per job

### Performance Issues
- Tune Kafka consumer settings
- Adjust Spark batch intervals
- Optimize Iceberg compaction
- Review partition strategy

## Next Steps

1. **Add Data Quality Checks**: Validate event schema, check for duplicates
2. **Implement Alerting**: High-severity threat alerts, pipeline failure notifications
3. **Optimize Performance**: Tune Kafka consumer, adjust Spark batch intervals
4. **Enhance Security**: Encrypt data at rest and in transit, implement IAM role-based access

## Comparison with Flink

| Feature | Spark Streaming | Flink Streaming |
|---------|----------------|-----------------|
| **Latency** | Seconds (micro-batch) | Milliseconds (true streaming) |
| **Processing Model** | Micro-batch | Event-by-event |
| **Deployment** | EMR, Databricks | Kinesis Analytics, EMR |
| **Language** | Python, Scala, Java | Java, Python (PyFlink) |
| **State Management** | Checkpoints | Savepoints |
| **Maturity** | Very mature | Mature |
| **Ease of Use** | High (Python API) | Medium |

**When to use Spark Streaming:**
- Batch-oriented workloads with streaming requirements
- Python-first development
- Existing Spark infrastructure
- Complex analytics and ML integration

**When to use Flink:**
- Ultra-low latency requirements (< 1 second)
- True event-by-event processing
- Complex event processing (CEP)
- Stateful stream processing

See [../flink-streaming/README.md](../flink-streaming/README.md) for Flink implementation.
