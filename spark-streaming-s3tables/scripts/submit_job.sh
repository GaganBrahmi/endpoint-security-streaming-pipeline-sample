#!/bin/bash
set -e

echo "=== Submitting Spark Streaming Job to EMR Serverless ==="
echo ""

export AWS_PROFILE=${AWS_PROFILE:-default}

# Load environment
if [ ! -f .env ]; then
    echo "ERROR: .env not found. Run ./scripts/deploy.sh first."
    exit 1
fi
source .env

if [ -z "$KAFKA_BOOTSTRAP_SERVERS" ] || [ "$KAFKA_BOOTSTRAP_SERVERS" = "PENDING" ]; then
    echo "ERROR: KAFKA_BOOTSTRAP_SERVERS not available."
    echo "  Check MSK status and update common/.env.msk, then redeploy."
    exit 1
fi

TABLE_NAME=${TABLE_NAME:-endpoint_events}

# Deterministic bucket names
HASH=$(echo -n "$BUCKET_NAME" | md5 2>/dev/null || echo -n "$BUCKET_NAME" | md5sum | cut -c1-8)
HASH=${HASH:0:8}
CHECKPOINT_BUCKET="spark-streaming-checkpoints-${AWS_REGION}-${HASH}"
CHECKPOINT_LOCATION="s3://${CHECKPOINT_BUCKET}/checkpoints/${NAMESPACE}/${KAFKA_TOPIC}"
SCRIPTS_BUCKET="spark-streaming-scripts-${AWS_REGION}-${HASH}"

echo "Configuration:"
echo "  Table Bucket:  $BUCKET_NAME"
echo "  Namespace:     $NAMESPACE"
echo "  Table Name:    $TABLE_NAME"
echo "  EMR App ID:    $EMR_APP_ID"
echo "  Kafka Brokers: $KAFKA_BOOTSTRAP_SERVERS"
echo "  Kafka Topic:   $KAFKA_TOPIC"
echo "  Checkpoints:   $CHECKPOINT_LOCATION"
echo ""

# Create buckets
aws s3 mb s3://$SCRIPTS_BUCKET --region $AWS_REGION 2>/dev/null || true
aws s3 mb s3://$CHECKPOINT_BUCKET --region $AWS_REGION 2>/dev/null || true

# Upload PySpark script
echo "Uploading spark_consumer.py..."
aws s3 cp pyspark/spark_consumer.py s3://$SCRIPTS_BUCKET/scripts/spark_consumer.py

# Warehouse location for Glue Iceberg REST catalog
ACCOUNT=$(echo $BUCKET_ARN | cut -d: -f5)
WAREHOUSE="${ACCOUNT}:s3tablescatalog/${BUCKET_NAME}"

echo ""
echo "Submitting streaming job..."
JOB_RUN_ID=$(aws emr-serverless start-job-run \
    --application-id $EMR_APP_ID \
    --execution-role-arn $EMR_ROLE_ARN \
    --name "spark-streaming-${KAFKA_TOPIC}" \
    --mode 'STREAMING' \
    --retry-policy '{"maxFailedAttemptsPerHour": 1}' \
    --job-driver '{
        "sparkSubmit": {
            "entryPoint": "s3://'"$SCRIPTS_BUCKET"'/scripts/spark_consumer.py",
            "entryPointArguments": [
                "--kafka-bootstrap-servers", "'"$KAFKA_BOOTSTRAP_SERVERS"'",
                "--kafka-topic", "'"$KAFKA_TOPIC"'",
                "--table-bucket-name", "'"$BUCKET_NAME"'",
                "--namespace", "'"$NAMESPACE"'",
                "--table-name", "'"$TABLE_NAME"'",
                "--checkpoint-location", "'"$CHECKPOINT_LOCATION"'",
                "--region", "'"$AWS_REGION"'"
            ],
            "sparkSubmitParameters": "--conf spark.executor.cores=2 --conf spark.executor.memory=4g --conf spark.executor.instances=2 --conf spark.driver.cores=2 --conf spark.driver.memory=4g --conf spark.dynamicAllocation.enabled=false --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions --conf spark.sql.catalog.s3tablescatalog=org.apache.iceberg.spark.SparkCatalog --conf spark.sql.catalog.s3tablescatalog.catalog-impl=org.apache.iceberg.rest.RESTCatalog --conf spark.sql.catalog.s3tablescatalog.uri=https://glue.'"$AWS_REGION"'.amazonaws.com/iceberg --conf spark.sql.catalog.s3tablescatalog.warehouse='"$WAREHOUSE"' --conf spark.sql.catalog.s3tablescatalog.rest.sigv4-enabled=true --conf spark.sql.catalog.s3tablescatalog.rest.signing-name=glue --conf spark.sql.catalog.s3tablescatalog.rest.signing-region='"$AWS_REGION"' --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4,software.amazon.msk:aws-msk-iam-auth:2.2.0"
        }
    }' \
    --query 'jobRunId' \
    --output text)

echo ""
echo "✅ Streaming job submitted!"
echo "  Job Run ID: $JOB_RUN_ID"
echo ""

# Poll job status until RUNNING or terminal state
echo "Waiting for job to start... (status will update automatically as it changes on the EMR cluster)"
PREV_STATUS=""
while true; do
    STATUS=$(aws emr-serverless get-job-run \
        --application-id "$EMR_APP_ID" \
        --job-run-id "$JOB_RUN_ID" \
        --query 'jobRun.state' \
        --output text 2>/dev/null)

    if [ "$STATUS" != "$PREV_STATUS" ]; then
        echo "  Status: $STATUS"
        PREV_STATUS="$STATUS"
    fi

    case $STATUS in
        RUNNING)
            echo ""
            echo "✅ Job is RUNNING. Waiting 10 seconds before exiting..."
            sleep 10
            echo ""
            echo "Verify tables:"
            echo "  aws s3tables list-tables --table-bucket-arn $BUCKET_ARN --namespace $NAMESPACE"
            echo ""
            echo "Monitor:"
            echo "  aws emr-serverless get-job-run --application-id $EMR_APP_ID --job-run-id $JOB_RUN_ID"
            echo ""
            echo "Cancel:"
            echo "  aws emr-serverless cancel-job-run --application-id $EMR_APP_ID --job-run-id $JOB_RUN_ID"
            break
            ;;
        FAILED|CANCELLED|SUCCESS)
            echo ""
            echo "❌ Job reached terminal state: $STATUS"
            echo "  Check logs:"
            echo "  aws emr-serverless get-job-run --application-id $EMR_APP_ID --job-run-id $JOB_RUN_ID"
            break
            ;;
    esac

    sleep 5
done

# Save job details
cat >> .env << EOF
JOB_RUN_ID=$JOB_RUN_ID
SCRIPTS_BUCKET=$SCRIPTS_BUCKET
CHECKPOINT_BUCKET=$CHECKPOINT_BUCKET
EOF

echo ""
echo "Job details appended to .env"
