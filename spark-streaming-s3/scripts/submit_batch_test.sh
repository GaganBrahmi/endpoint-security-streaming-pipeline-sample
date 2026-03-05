#!/bin/bash
set -e

echo "=== Submitting Batch Test Job to EMR Serverless (Standard S3 Iceberg) ==="
echo ""

export AWS_PROFILE=${AWS_PROFILE:-default}

# Load environment from deploy
if [ ! -f .env ]; then
    echo "ERROR: .env not found. Run ./scripts/deploy.sh first."
    exit 1
fi
source .env

# Deterministic bucket name for scripts
HASH=$(echo -n "$WAREHOUSE_BUCKET" | md5 2>/dev/null || echo -n "$WAREHOUSE_BUCKET" | md5sum | cut -c1-8)
HASH=${HASH:0:8}
SCRIPTS_BUCKET="spark-streaming-s3-scripts-${AWS_REGION}-${HASH}"

echo "Configuration:"
echo "  Warehouse:     $WAREHOUSE_LOCATION"
echo "  Database:      $DATABASE"
echo "  EMR App ID:    $EMR_APP_ID"
echo ""

# Create scripts bucket if needed
aws s3 mb s3://$SCRIPTS_BUCKET --region $AWS_REGION 2>/dev/null || true

# Upload batch test script
echo "Uploading batch_test.py..."
aws s3 cp pyspark/batch_test.py s3://$SCRIPTS_BUCKET/scripts/batch_test.py

echo ""
echo "Submitting batch test job..."
JOB_RUN_ID=$(aws emr-serverless start-job-run \
    --application-id $EMR_APP_ID \
    --execution-role-arn $EMR_ROLE_ARN \
    --name "batch-test-s3-iceberg" \
    --job-driver '{
        "sparkSubmit": {
            "entryPoint": "s3://'"$SCRIPTS_BUCKET"'/scripts/batch_test.py",
            "entryPointArguments": [
                "--warehouse-location", "'"$WAREHOUSE_LOCATION"'",
                "--database", "'"$DATABASE"'",
                "--table-name", "batch_test",
                "--region", "'"$AWS_REGION"'"
            ],
            "sparkSubmitParameters": "--conf spark.executor.cores=1 --conf spark.executor.memory=2g --conf spark.executor.instances=1 --conf spark.driver.cores=1 --conf spark.driver.memory=2g --conf spark.dynamicAllocation.enabled=false"
        }
    }' \
    --query 'jobRunId' \
    --output text)

echo ""
echo "✅ Batch test job submitted!"
echo "  Job Run ID: $JOB_RUN_ID"
echo ""

# Poll until complete
echo "Waiting for job to complete..."
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
        SUCCESS)
            echo ""
            echo "✅ Batch test PASSED!"
            echo ""
            echo "Verify table in Glue Catalog:"
            echo "  aws glue get-tables --database-name $DATABASE"
            echo ""
            echo "Check data on S3:"
            echo "  aws s3 ls s3://$WAREHOUSE_BUCKET/iceberg/$DATABASE/ --recursive"
            break
            ;;
        FAILED|CANCELLED)
            echo ""
            echo "❌ Batch test FAILED: $STATUS"
            echo "  Check logs:"
            echo "  aws emr-serverless get-job-run --application-id $EMR_APP_ID --job-run-id $JOB_RUN_ID"
            exit 1
            ;;
    esac

    sleep 5
done
