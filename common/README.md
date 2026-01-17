# Common Ingestion Components

This directory contains shared components for ingesting endpoint security events into Kafka. These components are used by all streaming processing engines (Spark, Flink, etc.).

## ⚠️ Security Notice

**This code uses unauthenticated/plaintext Kafka connections for demonstration purposes.**

For production use, implement proper Kafka security:
- **Authentication**: SASL/SCRAM, SASL/PLAIN, or IAM (for Amazon MSK)
- **Encryption**: SSL/TLS for data in transit
- **Authorization**: Kafka ACLs for topic-level access control

See the main [README.md](../README.md) for production security configuration examples.

## Components

### 1. Lambda Producer (`lambda_producer.py`)
AWS Lambda function that receives **real** endpoint security events and publishes them to Kafka.

**Use Case:** Production deployment where real endpoint agents send events

### 2. Lambda Data Generator (`lambda_data_generator.py`)
AWS Lambda function that **generates fake** endpoint security events for testing and demos.

**Use Case:** Testing, demos, load testing, and development without real endpoint agents

### 3. Deployment Scripts
- `deploy_lambda.sh` - Deploy the real event producer
- `deploy_data_generator.sh` - Deploy the fake event generator

### 4. Sample Data
- `sample_events.json` - Example endpoint security events for testing

## Prerequisites

### AWS Services
- S3 bucket for data storage
- AWS Glue Data Catalog database
- Kafka cluster (MSK, self-managed, or local)
- Topic: `endpoint_logs` created

### Dependencies
```bash
pip install -r requirements.txt
```

## IAM Role Setup

Before deploying Lambda functions, create an IAM execution role with appropriate permissions.

### Option 1: Lambda WITHOUT VPC (Public Kafka or Local Testing)

```bash
# Create the role
aws iam create-role \
  --role-name lambda-endpoint-logs-role \
  --assume-role-policy-document '{
    "Version": "2012-10-17",
    "Statement": [{
      "Effect": "Allow",
      "Principal": {"Service": "lambda.amazonaws.com"},
      "Action": "sts:AssumeRole"
    }]
  }'

# Attach basic execution policy (for CloudWatch Logs)
aws iam attach-role-policy \
  --role-name lambda-endpoint-logs-role \
  --policy-arn arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole

# Get the role ARN
aws iam get-role \
  --role-name lambda-endpoint-logs-role \
  --query 'Role.Arn' \
  --output text
```

### Option 2: Lambda WITH VPC (For MSK in VPC)

```bash
# Create the role
aws iam create-role \
  --role-name lambda-msk-execution-role \
  --assume-role-policy-document '{
    "Version": "2012-10-17",
    "Statement": [{
      "Effect": "Allow",
      "Principal": {"Service": "lambda.amazonaws.com"},
      "Action": "sts:AssumeRole"
    }]
  }'

# Attach basic execution policy
aws iam attach-role-policy \
  --role-name lambda-msk-execution-role \
  --policy-arn arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole

# Attach VPC access policy
aws iam attach-role-policy \
  --role-name lambda-msk-execution-role \
  --policy-arn arn:aws:iam::aws:policy/service-role/AWSLambdaVPCAccessExecutionRole

# Get the role ARN
aws iam get-role \
  --role-name lambda-msk-execution-role \
  --query 'Role.Arn' \
  --output text
```

## Configuration

### Lambda Producer Setup (Real Events)

**Edit Configuration in `deploy_lambda.sh`:**
```bash
ROLE_ARN="arn:aws:iam::YOUR_ACCOUNT_ID:role/YOUR_LAMBDA_ROLE"
KAFKA_BOOTSTRAP_SERVERS="your-msk-broker:9092"
KAFKA_TOPIC="endpoint_logs"
```

**VPC Configuration (for MSK in VPC):**
```bash
VPC_ENABLED="true"
VPC_SUBNET_IDS="subnet-xxxxx,subnet-yyyyy"  # Private subnets
VPC_SECURITY_GROUP_IDS="sg-xxxxx"  # Lambda security group
```

**Deploy:**
```bash
./deploy_lambda.sh
```

**Test Locally:**
```bash
python lambda_producer.py
```

### Lambda Data Generator Setup (Fake Events)

**Edit Configuration in `deploy_data_generator.sh`:**
```bash
ROLE_ARN="arn:aws:iam::YOUR_ACCOUNT_ID:role/YOUR_LAMBDA_ROLE"
KAFKA_BOOTSTRAP_SERVERS="your-msk-broker:9092"
KAFKA_TOPIC="endpoint_logs"
NUM_EVENTS=100
NUM_CUSTOMERS=5
NUM_TENANTS=3
```

**VPC Configuration (for MSK in VPC):**
```bash
VPC_ENABLED="true"
VPC_SUBNET_IDS="subnet-xxxxx,subnet-yyyyy"
VPC_SECURITY_GROUP_IDS="sg-xxxxx"
```

**Deploy:**
```bash
./deploy_data_generator.sh
```

**Test Locally:**
```bash
python lambda_data_generator.py
```

## Event Schema

```json
{
  "customer_id": "cust_12345",
  "tenant_id": "tenant_abc",
  "device_id": "device_xyz",
  "device_name": "LAPTOP-001",
  "device_type": "laptop",
  "event_type": "file_access",
  "event_category": "security",
  "severity": "HIGH",
  "timestamp": "2024-01-16T10:30:00Z",
  "user": "john.doe@company.com",
  "process_name": "chrome.exe",
  "file_path": "/etc/passwd",
  "action": "read",
  "result": "blocked",
  "ip_address": "192.168.1.100",
  "os": "Windows 10",
  "os_version": "10.0.19045",
  "threat_detected": true,
  "threat_type": "unauthorized_access"
}
```

## Next Steps

After setting up ingestion, choose a stream processing engine:

- **Spark Streaming**: See [../spark-streaming/README.md](../spark-streaming/README.md)
- **Flink Streaming**: See [../flink-streaming/README.md](../flink-streaming/README.md)

## Monitoring

### CloudWatch Metrics
- Lambda invocations and errors
- Kafka producer metrics

### Logging
- Lambda logs in CloudWatch
- Check for Kafka connection errors

## Troubleshooting

### Kafka Connection Failed
- Verify bootstrap servers address
- Check security group rules (if using VPC)
- Ensure Kafka topic exists

### Lambda Timeout
- Increase Lambda timeout setting
- Optimize batch size
- Check Kafka broker connectivity
