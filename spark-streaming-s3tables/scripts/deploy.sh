#!/bin/bash
set -e

echo "=== Deploying Spark Streaming (EMR Serverless + S3 Tables) ==="
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

NAMESPACE=${NAMESPACE:-endpoint_security_spark_s3tables}
SUBNET_CONTEXT=""
if [ -n "$SUBNET_IDS" ]; then
    SUBNET_CONTEXT="-c subnet_ids=$SUBNET_IDS"
fi

CDK_CONTEXT="-c vpc_id=$VPC_ID -c namespace=$NAMESPACE -c msk_security_group_id=$MSK_SECURITY_GROUP_ID $SUBNET_CONTEXT"

cd "$SPARK_STREAMING_DIR/cdk"

# Install CDK Python dependencies
pip install -r requirements.txt --quiet

echo "Deploying Spark Streaming stack..."
echo "  (S3 Table bucket will be created by CDK with resource policy)"
echo ""
cdk deploy --require-approval never $CDK_CONTEXT

# Extract outputs
STACK=SparkStreamingStack
BUCKET_NAME=$(aws cloudformation describe-stacks --stack-name $STACK \
    --query "Stacks[0].Outputs[?OutputKey=='TableBucketName'].OutputValue" --output text)
BUCKET_ARN=$(aws cloudformation describe-stacks --stack-name $STACK \
    --query "Stacks[0].Outputs[?OutputKey=='TableBucketArn'].OutputValue" --output text)
NAMESPACE_NAME=$(aws cloudformation describe-stacks --stack-name $STACK \
    --query "Stacks[0].Outputs[?OutputKey=='NamespaceName'].OutputValue" --output text)
EMR_APP_ID=$(aws cloudformation describe-stacks --stack-name $STACK \
    --query "Stacks[0].Outputs[?OutputKey=='EMRApplicationId'].OutputValue" --output text)
EMR_ROLE_ARN=$(aws cloudformation describe-stacks --stack-name $STACK \
    --query "Stacks[0].Outputs[?OutputKey=='EMRRoleArn'].OutputValue" --output text)
S3_TABLES_CATALOG_ID=$(aws cloudformation describe-stacks --stack-name $STACK \
    --query "Stacks[0].Outputs[?OutputKey=='S3TablesCatalogId'].OutputValue" --output text)

cd "$SPARK_STREAMING_DIR"

# Load Kafka topic from Lambda env if available
KAFKA_TOPIC="endpoint_logs"
if [ -f "$COMMON_DIR/.env.lambda" ]; then
    source "$COMMON_DIR/.env.lambda"
fi

cat > .env << EOF
BUCKET_NAME=$BUCKET_NAME
BUCKET_ARN=$BUCKET_ARN
NAMESPACE=$NAMESPACE_NAME
KAFKA_TOPIC=$KAFKA_TOPIC
KAFKA_BOOTSTRAP_SERVERS=$KAFKA_BOOTSTRAP_SERVERS
EMR_APP_ID=$EMR_APP_ID
EMR_ROLE_ARN=$EMR_ROLE_ARN
S3_TABLES_CATALOG_ID=$S3_TABLES_CATALOG_ID
AWS_REGION=$AWS_REGION
EOF

echo ""
echo "Stack outputs saved to .env"
echo ""
echo "  Table Bucket:      $BUCKET_NAME"
echo "  Table Bucket ARN:  $BUCKET_ARN"
echo "  Namespace:         $NAMESPACE_NAME"
echo "  EMR App ID:        $EMR_APP_ID"
echo "  Catalog ID:        $S3_TABLES_CATALOG_ID"
echo ""

# Grant Lake Formation permissions (matching working s3TableBucketDemo)
echo "Granting Lake Formation permissions on S3 Tables catalog..."
echo "  Catalog:   $S3_TABLES_CATALOG_ID"
echo "  Principal: $EMR_ROLE_ARN"
echo "  Permissions: ALTER, CREATE_DATABASE, DESCRIBE, DROP"
echo ""

aws lakeformation grant-permissions \
    --cli-input-json '{
        "Principal": {
            "DataLakePrincipalIdentifier": "'"$EMR_ROLE_ARN"'"
        },
        "Resource": {
            "Catalog": {
                "Id": "'"$S3_TABLES_CATALOG_ID"'"
            }
        },
        "Permissions": ["ALTER", "CREATE_DATABASE", "DESCRIBE", "DROP"],
        "PermissionsWithGrantOption": []
    }' && echo "✅ Lake Formation permissions granted" || {
    echo "⚠️  LF grant failed. Grant manually:"
    echo "   Principal: $EMR_ROLE_ARN"
    echo "   Catalog:   $S3_TABLES_CATALOG_ID"
    echo "   Permissions: ALTER, CREATE_DATABASE, DESCRIBE, DROP"
}

echo ""
echo "=== Deployment Complete ==="
echo ""
echo "Next steps:"
echo "  1. Generate data:  ../common/scripts/generate_data.sh"
echo "  2. Batch test:     ./scripts/submit_batch_test.sh"
echo "  3. Submit stream:  ./scripts/submit_job.sh"
echo ""
