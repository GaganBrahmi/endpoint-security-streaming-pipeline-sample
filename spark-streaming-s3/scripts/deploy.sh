#!/bin/bash
set -e

echo "=== Deploying Spark Streaming (EMR Serverless + Standard S3 Iceberg) ==="
echo ""

export AWS_PROFILE=${AWS_PROFILE:-default}
export AWS_REGION=${AWS_REGION:-us-east-1}
AWS_ACCOUNT=$(aws sts get-caller-identity --query Account --output text)

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
SPARK_STREAMING_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
COMMON_DIR="$(cd "$SPARK_STREAMING_DIR/../common" && pwd)"

# Load MSK outputs (required)
if [ ! -f "$COMMON_DIR/.env.msk" ]; then
    echo "ERROR: MSK not deployed. Run common/scripts/deploy_msk.sh first."
    exit 1
fi
source "$COMMON_DIR/.env.msk"

NAMESPACE=${NAMESPACE:-endpoint_security_spark_s3}
SUBNET_CONTEXT=""
if [ -n "$SUBNET_IDS" ]; then
    SUBNET_CONTEXT="-c subnet_ids=$SUBNET_IDS"
fi

CDK_CONTEXT="-c vpc_id=$VPC_ID -c namespace=$NAMESPACE -c msk_security_group_id=$MSK_SECURITY_GROUP_ID $SUBNET_CONTEXT"

cd "$SPARK_STREAMING_DIR/cdk"

# Install CDK Python dependencies
pip install -r requirements.txt --quiet

echo "Deploying Spark Streaming S3 stack..."
echo "  (Standard S3 bucket for Iceberg warehouse, Glue Catalog for metadata)"
echo ""
cdk deploy --require-approval never $CDK_CONTEXT

# Extract outputs
STACK=SparkStreamingS3Stack
WAREHOUSE_BUCKET=$(aws cloudformation describe-stacks --stack-name $STACK \
    --query "Stacks[0].Outputs[?OutputKey=='WarehouseBucketName'].OutputValue" --output text)
WAREHOUSE_LOCATION=$(aws cloudformation describe-stacks --stack-name $STACK \
    --query "Stacks[0].Outputs[?OutputKey=='WarehouseLocation'].OutputValue" --output text)
DATABASE_NAME=$(aws cloudformation describe-stacks --stack-name $STACK \
    --query "Stacks[0].Outputs[?OutputKey=='DatabaseName'].OutputValue" --output text)
EMR_APP_ID=$(aws cloudformation describe-stacks --stack-name $STACK \
    --query "Stacks[0].Outputs[?OutputKey=='EMRApplicationId'].OutputValue" --output text)
EMR_ROLE_ARN=$(aws cloudformation describe-stacks --stack-name $STACK \
    --query "Stacks[0].Outputs[?OutputKey=='EMRRoleArn'].OutputValue" --output text)

cd "$SPARK_STREAMING_DIR"

# Load Kafka topic from Lambda env if available
KAFKA_TOPIC="endpoint_logs"
if [ -f "$COMMON_DIR/.env.lambda" ]; then
    source "$COMMON_DIR/.env.lambda"
fi

cat > .env << EOF
WAREHOUSE_BUCKET=$WAREHOUSE_BUCKET
WAREHOUSE_LOCATION=$WAREHOUSE_LOCATION
DATABASE=$DATABASE_NAME
KAFKA_TOPIC=$KAFKA_TOPIC
KAFKA_BOOTSTRAP_SERVERS=$KAFKA_BOOTSTRAP_SERVERS
EMR_APP_ID=$EMR_APP_ID
EMR_ROLE_ARN=$EMR_ROLE_ARN
AWS_REGION=$AWS_REGION
VPC_ID=$VPC_ID
MSK_SECURITY_GROUP_ID=$MSK_SECURITY_GROUP_ID
SUBNET_IDS=$SUBNET_IDS
EOF

echo ""
echo "Stack outputs saved to .env"
echo ""
echo "  Warehouse Bucket:  $WAREHOUSE_BUCKET"
echo "  Warehouse Location: $WAREHOUSE_LOCATION"
echo "  Database:          $DATABASE_NAME"
echo "  EMR App ID:        $EMR_APP_ID"
echo ""
echo "No Lake Formation grants needed — standard Glue Catalog + S3."
echo ""
echo "=== Deployment Complete ==="
echo ""
echo "Next steps:"
echo "  1. Generate data:  ../common/scripts/generate_data.sh"
echo "  2. Batch test:     ./scripts/submit_batch_test.sh"
echo "  3. Submit stream:  ./scripts/submit_job.sh"
echo ""
