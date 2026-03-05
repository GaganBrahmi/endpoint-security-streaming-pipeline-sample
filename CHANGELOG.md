# Endpoint Security Streaming Pipeline - Changelog

## Version 2.0 - Multi-Engine Streaming Pipeline with Deployment Automation

### Overview
Complete evolution from a single Lambda data generator into a full multi-engine streaming pipeline supporting three stream processing engines (Flink, Spark on S3, Spark on S3 Tables), all sharing a common MSK Serverless + Lambda ingestion layer. Includes one-command deployment, comprehensive documentation, and real-time windowed aggregations.

### Major Features

#### 1. Common Infrastructure Layer (MSK + Lambda)
- **MSK Serverless Cluster**: Shared Kafka cluster with IAM authentication (port 9098), requires 2+ subnets in different AZs
- **CDK Stacks**: Separate `MSKStack` and `LambdaDataGenStack` with context-driven configuration
- **Lambda Data Generator**: Generates fake endpoint security events (23-field schema) and publishes to Kafka topic `endpoint_logs`
- **Auto-Generated IDs**: Customer IDs (`cust_00001` format) and tenant IDs (`tenant_a` format) are automatically generated via `NUM_CUSTOMERS` and `NUM_TENANTS` environment variables, replacing the manual `CUSTOMER_IDS`/`TENANT_IDS` from v1.0
- **MSK Data Generator**: MSK-specific variant using `aws-msk-iam-sasl-signer` for IAM auth
- **Lambda Layer**: Bundled `kafka-python-ng` + `aws-msk-iam-sasl-signer` + `boto3`
- **Modular Scripts**: `deploy_msk.sh`, `deploy_lambda.sh`, `generate_data.sh`, `cleanup_msk.sh`, `cleanup_lambda.sh`

#### 2. Flink Streaming (Amazon Managed Service for Apache Flink)
- **PyFlink 1.20 Application**: Event-by-event processing with sub-second latency on Amazon MSF
- **7 Iceberg Tables**: Raw events + 6 aggregation tables via multiple windowing strategies
- **Windowing Strategies**:
  - Tumbling windows (5, 15, 30, 60 minutes) for compliance reporting
  - Sliding windows (5-min window, 1-min slide) for real-time alerting
  - Cumulate windows (5-min step, 60-min max) replacing Flink 1.20's unsupported SESSION TVF
- **Modular Code**: `flink_consumer_with_aggregations.py`, `aggregation_config.py`, `table_creators.py`, `aggregation_jobs.py`
- **Maven Build**: Fat-jar + assembly ZIP packaging with Kafka connector, Iceberg runtime, MSK IAM auth
- **Two-Phase CDK Deployment**: Phase 1 (infra) → Maven build + S3 upload → Phase 2 (MSF application)
- **Operational Scripts**: `deploy.sh`, `build.sh`, `start_app.sh`, `update_app.sh`, `cleanup.sh`
- **PROCTIME() Windowing**: Uses processing time since event-time watermarks don't advance reliably on MSF
- **TVF Window Syntax**: Flink 1.20 `TABLE(TUMBLE(...))`, `TABLE(HOP(...))`, `TABLE(CUMULATE(...))`
- **Non-Shaded Kafka Connector**: Avoids conflicts between `flink-sql-connector-kafka` and `aws-msk-iam-auth`

#### 3. Spark Streaming — Standard S3 (Iceberg)
- **PySpark Structured Streaming**: Micro-batch processing on EMR Serverless (emr-7.12.0, Spark 3.5)
- **Standard S3 + Glue Catalog**: Iceberg tables on a standard S3 bucket, no Lake Formation needed
- **Kafka IAM Auth**: MSK Serverless connectivity via port 9098
- **30-Second Micro-Batch Trigger**: Append mode with fanout enabled
- **Kafka Metadata Enrichment**: `kafka_timestamp`, `kafka_partition`, `kafka_offset`
- **Time-Based Partitioning**: `event_year`, `event_month`, `event_day`
- **Batch Test Script**: `batch_test.py` for connectivity verification
- **CDK Stack**: S3 warehouse bucket, EMR Serverless app, IAM role, security group

#### 4. Spark Streaming — S3 Tables (Iceberg)
- **S3 Table Bucket**: Managed Iceberg storage with automatic compaction
- **Lake Formation Integration**: Catalog-level permissions on S3 Tables nested catalog (granted via AWS CLI since CDK/CloudFormation can't do this)
- **REST + GlueCatalog Modes**: Batch test supports both catalog modes
- **Same Streaming Architecture**: Kafka source, JSON parsing, Iceberg sink, 30-second micro-batch
- **CDK Stack**: S3 Table Bucket with resource policy, EMR Serverless app, IAM role, SG, LF permissions

#### 5. One-Command Deployment and Cleanup
- **Root-Level Deploy Scripts**: `deploy_flink.sh`, `deploy_spark_s3.sh`, `deploy_spark_s3tables.sh`
- **Auto-Discovery**: Detects existing MSK/Lambda deployments and skips if already active
- **Root-Level Cleanup Scripts**: `cleanup_flink.sh`, `cleanup_spark_s3.sh`, `cleanup_spark_s3tables.sh`
- **Teardown Order**: Consumer → Lambda → MSK
- **Environment Files**: `.env.msk`, `.env.lambda`, `.env` per consumer for cross-component configuration

#### 6. Status Dashboard
- **`status.sh`**: Color-coded dashboard showing all component statuses
- **Checks**: MSK cluster, Lambda function, Flink MSF app, EMR Serverless apps, databases, warehouse locations
- **CloudFormation Stack Status**: Queries stack state for each component

#### 7. Comprehensive Documentation
- **Root README.md**: Full project documentation with architecture diagram, directory structure, quick start, event schema, and stream processing comparison table
- **common/README.md**: MSK Serverless + Lambda data generator documentation
- **flink-streaming/README.md**: Flink application documentation with two-phase CDK deployment, tables, scripts, configuration, and key technical decisions
- **flink-streaming/USE_CASE.md**: Detailed endpoint security use case with data model, windowing strategies, aggregation metrics, SQL query examples, and security operations workflows
- **flink-streaming/WINDOWING_GUIDE.md**: In-depth guide to tumbling, sliding, and session windowing strategies with SQL examples, performance considerations, and troubleshooting
- **spark-streaming-s3/README.md**: Spark S3 consumer documentation
- **spark-streaming-s3tables/README.md**: Spark S3 Tables consumer documentation with Lake Formation notes
#### 8. MSK IAM Authentication
- **IAM-Based Auth Everywhere**: All components (Lambda, Flink, Spark) connect to MSK Serverless using IAM authentication on port 9098 — no plaintext Kafka credentials
- **MSK IAM SASL Signer**: Lambda uses `aws-msk-iam-sasl-signer` (bundled in Lambda Layer) to generate short-lived auth tokens
- **Kafka Cluster IAM Policies**: Each consumer role gets scoped `kafka-cluster:*` permissions on the `streaming-demo-msk` cluster, topics, and consumer groups
- **Self-Referencing Security Group**: MSK security group allows inbound 9098 from itself; all consumers attach this SG so they're automatically authorized
- **Flink MSK IAM Auth**: Uses `aws-msk-iam-auth` JAR (bundled via Maven) for native Kafka connector IAM authentication
- **Spark MSK IAM Auth**: EMR Serverless Spark 3.5 (emr-7.12.0) has built-in MSK IAM auth support via `kafka.security.protocol=SASL_SSL`

#### 9. EMR Serverless
- **Spark 3.5 on emr-7.12.0**: Both Spark consumers run on EMR Serverless with auto-start and auto-stop (30-min idle timeout)
- **Maximum Capacity**: 8 vCPU, 32 GB memory per application
- **VPC Networking**: EMR ENIs placed in user-specified subnets with MSK security group attached for Kafka connectivity
- **Two Applications**: `spark-streaming-s3` (standard S3) and `spark-streaming-s3tables` (S3 Table Bucket) — independent apps with separate IAM roles
- **Job Submission Scripts**: `submit_job.sh` for streaming jobs, `submit_batch_test.sh` for connectivity verification
- **S3 Tables Variant**: Adds S3 Table Bucket with resource policy, Lake Formation grants (via AWS CLI post-deploy), and Glue Iceberg REST catalog integration

#### 10. Infrastructure as Code (CDK) with IAM Roles and Policies
All infrastructure is defined in AWS CDK (Python). Each component has a dedicated CDK stack with least-privilege IAM roles:

- **MSKStack** (`common/cdk/msk_stack.py`):
  - MSK Serverless cluster with IAM client authentication
  - Security group with self-referencing rule on port 9098

- **LambdaDataGenStack** (`common/cdk/lambda_stack.py`):
  - Lambda execution role with `AWSLambdaVPCAccessExecutionRole` managed policy
  - MSK IAM policy: `kafka-cluster:Connect`, `DescribeCluster`, `DescribeTopic`, `CreateTopic`, `WriteData`, `ReadData`, `DescribeGroup`, `AlterGroup`, `AlterTopic`
  - Lambda function in VPC with both Lambda SG and MSK SG attached

- **FlinkStreamingStack** (`flink-streaming/cdk/stack.py`):
  - MSF execution role assumed by `kinesisanalytics.amazonaws.com`
  - S3 policies: read on artifact bucket, read/write on warehouse bucket
  - Glue Catalog policies: full CRUD on Iceberg database, tables, partitions (scoped to target database + Flink's `default_catalog`/`default_database`)
  - MSK IAM policy: `kafka-cluster:Connect`, `DescribeCluster`, `DescribeTopic`, `ReadData`, `DescribeGroup`, `AlterGroup`
  - VPC ENI management: `ec2:CreateNetworkInterface`, `DeleteNetworkInterface`, `Describe*`
  - CloudWatch: `logs:PutLogEvents`, `cloudwatch:PutMetricData`

- **SparkStreamingS3Stack** (`spark-streaming-s3/cdk/stack.py`):
  - EMR execution role assumed by `emr-serverless.amazonaws.com`
  - S3 policies: read/write on warehouse bucket, scripts bucket, checkpoints bucket
  - Glue Catalog policies: full CRUD on database and tables (scoped to namespace)
  - MSK IAM policy: same consumer permissions as Flink

- **SparkStreamingStack** (`spark-streaming-s3tables/cdk/stack.py`):
  - EMR execution role assumed by `emr-serverless.amazonaws.com`
  - S3 Tables policies: `GetTable`, `CreateTable`, `DeleteTable`, `PutTableData`, `GetTableData`, `UpdateTableMetadataLocation`, `CreateNamespace`, `GetNamespace`, `ListTables`, `ListNamespaces`
  - S3 Table Bucket resource policy: grants `s3tables:*` actions to account root
  - Glue Catalog policies: scoped to `s3tablescatalog` and nested paths
  - Lake Formation policies: `GetDataAccess`, `GrantPermissions`, `RevokePermissions`, `ListPermissions`
  - MSK IAM policy: same consumer permissions as other stacks

### Architecture

```
Endpoint Devices → Lambda Data Generator → MSK Serverless (Kafka) → Stream Processor → Iceberg → Analytics
                                                                          │
                                                          ┌───────────────┼───────────────┐
                                                          │               │               │
                                                     Flink (MSF)    Spark (S3)    Spark (S3 Tables)
                                                          │               │               │
                                                     Glue Catalog    Glue Catalog    S3 Tables Catalog
```

### Database Names

| Consumer | Database/Namespace |
|---|---|
| Flink (MSF) | `endpoint_security_flink` |
| Spark (Standard S3) | `endpoint_security_spark_s3` |
| Spark (S3 Tables) | `endpoint_security_spark_s3tables` |

### Stream Processing Comparison

| Feature | Flink (MSF) | Spark (Standard S3) | Spark (S3 Tables) |
|---|---|---|---|
| Processing Model | Event-by-event | Micro-batch | Micro-batch |
| Latency | Milliseconds | Seconds | Seconds |
| Deployment | Amazon MSF | EMR Serverless | EMR Serverless |
| Storage | Standard S3 | Standard S3 | S3 Table Bucket |
| Catalog | Glue Catalog | Glue Catalog | S3 Tables + Lake Formation |
| Lake Formation | Not needed | Not needed | Required |
| Windowing | Tumbling, Sliding, Cumulate | Micro-batch triggers | Micro-batch triggers |
| Aggregations | Built-in (6 tables) | Custom | Custom |
| Tables Created | 7 (raw + 6 agg) | 1 (raw) | 1 (raw) |

### Project Structure

```
endpoint-security-streaming-pipeline/
├── common/                        # Shared: MSK Serverless + Lambda data generator
│   ├── cdk/                       # CDK stacks (MSKStack, LambdaDataGenStack)
│   ├── lambda_function/           # Lambda code (data generator + MSK producer)
│   ├── lambda_layer/              # kafka-python-ng + aws-msk-iam-sasl-signer + boto3
│   └── scripts/                   # deploy_msk.sh, deploy_lambda.sh, generate_data.sh, cleanup_*.sh
├── flink-streaming/               # Flink on Amazon MSF
│   ├── cdk/                       # CDK stack (two-phase: infra → MSF app)
│   ├── scripts/                   # deploy, build, start_app, update_app, cleanup, generate_data
│   ├── assembly/                  # Maven assembly descriptor (ZIP packaging)
│   ├── *.py                       # PyFlink application code (4 modules)
│   └── pom.xml                    # Maven build (fat-jar + ZIP)
├── spark-streaming-s3/            # Spark on EMR Serverless → Standard S3 (Iceberg)
│   ├── cdk/                       # CDK stack (EMR Serverless + S3 + Glue)
│   ├── pyspark/                   # spark_consumer.py + batch_test.py
│   └── scripts/                   # deploy, submit_job, submit_batch_test, cleanup, generate_data
├── spark-streaming-s3tables/      # Spark on EMR Serverless → S3 Tables (Iceberg)
│   ├── cdk/                       # CDK stack (EMR Serverless + S3 Table Bucket + LF)
│   ├── pyspark/                   # spark_consumer.py + batch_test.py
│   └── scripts/                   # deploy, submit_job, submit_batch_test, cleanup, generate_data
├── deploy_flink.sh                # One-command deploy: MSK + Lambda + Flink
├── deploy_spark_s3.sh             # One-command deploy: MSK + Lambda + Spark (S3)
├── deploy_spark_s3tables.sh       # One-command deploy: MSK + Lambda + Spark (S3 Tables)
├── cleanup_flink.sh               # One-command teardown: Flink → Lambda → MSK
├── cleanup_spark_s3.sh            # One-command teardown: Spark S3 → Lambda → MSK
├── cleanup_spark_s3tables.sh      # One-command teardown: Spark S3 Tables → Lambda → MSK
├── status.sh                      # Dashboard: check all component statuses
└── CHANGELOG.md
```

---

## Version 1.0 - Initial Release

### Features
- Lambda function for generating fake endpoint security events
- Kafka integration for event publishing
- Configurable event generation via environment variables
- EventBridge scheduling support
- Manual customer and tenant ID specification

### Environment Variables (v1.0)
- `KAFKA_BOOTSTRAP_SERVERS`: Kafka broker addresses
- `KAFKA_TOPIC`: Kafka topic name
- `NUM_EVENTS`: Number of events per execution
- `CUSTOMER_IDS`: Comma-separated customer IDs
- `TENANT_IDS`: Comma-separated tenant IDs
