#!/bin/bash
set -e

echo "=== Deploying Flink Streaming (Amazon MSF + Iceberg) ==="
echo ""
echo "Two-phase CDK deployment:"
echo "  Phase 1: Create S3 buckets, IAM role, security group, log group"
echo "  Phase 2: Upload ZIP → create MSF application"
echo ""

export AWS_PROFILE=${AWS_PROFILE:-default}
export AWS_REGION=${AWS_REGION:-us-east-1}

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
FLINK_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
COMMON_DIR="$(cd "$FLINK_DIR/../common" && pwd)"

# Load MSK outputs (required)
if [ ! -f "$COMMON_DIR/.env.msk" ]; then
    echo "ERROR: MSK not deployed. Run common/scripts/deploy_msk.sh first."
    exit 1
fi
source "$COMMON_DIR/.env.msk"

if [ -z "$KAFKA_BOOTSTRAP_SERVERS" ] || [ "$KAFKA_BOOTSTRAP_SERVERS" = "PENDING" ]; then
    echo "ERROR: KAFKA_BOOTSTRAP_SERVERS not available in .env.msk"
    echo "  MSK may still be provisioning. Check status and retry."
    exit 1
fi

KAFKA_TOPIC=${KAFKA_TOPIC:-endpoint_logs}
DATABASE_NAME=${DATABASE_NAME:-endpoint_security_flink}
TABLE_NAME=${TABLE_NAME:-endpoint_data_flink}

SUBNET_CONTEXT=""
if [ -n "$SUBNET_IDS" ]; then
    SUBNET_CONTEXT="-c subnet_ids=$SUBNET_IDS"
fi

CDK_BASE_CONTEXT="-c vpc_id=$VPC_ID -c msk_security_group_id=$MSK_SECURITY_GROUP_ID -c kafka_bootstrap_servers=$KAFKA_BOOTSTRAP_SERVERS -c kafka_topic=$KAFKA_TOPIC -c database_name=$DATABASE_NAME -c table_name=$TABLE_NAME $SUBNET_CONTEXT"

cd "$FLINK_DIR/cdk"
pip install -r requirements.txt --quiet

# ── Phase 1: Infrastructure (no MSF app yet) ──
echo "Phase 1: Deploying infrastructure (S3 buckets, IAM, SG, logs)..."
echo ""
cdk deploy --require-approval never $CDK_BASE_CONTEXT

STACK=FlinkStreamingStack
get_output() {
    aws cloudformation describe-stacks --stack-name $STACK \
        --query "Stacks[0].Outputs[?OutputKey=='$1'].OutputValue" --output text
}

ARTIFACT_BUCKET=$(get_output ArtifactBucketName)
WAREHOUSE_BUCKET=$(get_output WarehouseBucketName)
APP_S3_KEY=$(get_output AppS3Key)
WAREHOUSE_PATH=$(get_output WarehousePath)
FLINK_ROLE_ARN=$(get_output FlinkRoleArn)
LOG_GROUP=$(get_output LogGroupName)

echo ""
echo "  ✅ Phase 1 complete"
echo "  Artifact bucket:  $ARTIFACT_BUCKET"
echo "  Warehouse bucket: $WAREHOUSE_BUCKET"
echo ""

# ── Build & Upload ──
echo "Building application..."
cd "$FLINK_DIR"
"$SCRIPT_DIR/build.sh"

echo "Uploading ZIP to S3..."
aws s3 cp "$FLINK_DIR/flink-application.zip" "s3://${ARTIFACT_BUCKET}/${APP_S3_KEY}"
echo "  ✅ Uploaded to s3://${ARTIFACT_BUCKET}/${APP_S3_KEY}"
echo ""

# ── Phase 2: Create MSF application (ZIP now exists in S3) ──
echo "Phase 2: Creating MSF application..."
echo ""
cd "$FLINK_DIR/cdk"
cdk deploy --require-approval never $CDK_BASE_CONTEXT -c deploy_app=true

APP_NAME=$(get_output ApplicationName)

cd "$FLINK_DIR"

# Load Kafka topic from Lambda env if available
if [ -f "$COMMON_DIR/.env.lambda" ]; then
    source "$COMMON_DIR/.env.lambda"
fi

cat > .env << EOF
ARTIFACT_BUCKET=$ARTIFACT_BUCKET
WAREHOUSE_BUCKET=$WAREHOUSE_BUCKET
APP_S3_KEY=$APP_S3_KEY
WAREHOUSE_PATH=$WAREHOUSE_PATH
DATABASE=$DATABASE_NAME
TABLE_NAME=$TABLE_NAME
KAFKA_TOPIC=$KAFKA_TOPIC
KAFKA_BOOTSTRAP_SERVERS=$KAFKA_BOOTSTRAP_SERVERS
APP_NAME=$APP_NAME
FLINK_ROLE_ARN=$FLINK_ROLE_ARN
LOG_GROUP=$LOG_GROUP
AWS_REGION=$AWS_REGION
VPC_ID=$VPC_ID
MSK_SECURITY_GROUP_ID=$MSK_SECURITY_GROUP_ID
SUBNET_IDS=$SUBNET_IDS
EOF

echo ""
echo "Stack outputs saved to .env"
echo ""
echo "  Artifact Bucket:  $ARTIFACT_BUCKET"
echo "  Warehouse Path:   $WAREHOUSE_PATH"
echo "  Application Name: $APP_NAME"
echo "  Log Group:        $LOG_GROUP"
echo ""
echo "=== Deployment Complete ==="
echo ""
echo "Next steps:"
echo "  1. Generate data:  ./scripts/generate_data.sh"
echo "  2. Start app:      ./scripts/start_app.sh"
echo "  3. View logs:      aws logs tail $LOG_GROUP --follow --region $AWS_REGION"
echo ""
