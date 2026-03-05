#!/bin/bash
set -e

echo "=== Deploying Lambda Data Generator ==="
echo ""

export AWS_PROFILE=${AWS_PROFILE:-default}
export AWS_REGION=${AWS_REGION:-us-east-1}
AWS_ACCOUNT=$(aws sts get-caller-identity --query Account --output text)

COMMON_DIR="$(dirname "$0")/.."

# Load MSK outputs
if [ ! -f "$COMMON_DIR/.env.msk" ]; then
    echo "ERROR: .env.msk not found. Deploy MSK first: ./scripts/deploy_msk.sh"
    exit 1
fi
source "$COMMON_DIR/.env.msk"

if [ -z "$MSK_SECURITY_GROUP_ID" ]; then
    echo "ERROR: MSK_SECURITY_GROUP_ID not found in .env.msk"
    exit 1
fi

KAFKA_TOPIC=${KAFKA_TOPIC:-endpoint_logs}

SUBNET_CONTEXT=""
if [ -n "$SUBNET_IDS" ]; then
    SUBNET_CONTEXT="-c subnet_ids=$SUBNET_IDS"
fi

CDK_CONTEXT="-c vpc_id=$VPC_ID -c msk_security_group_id=$MSK_SECURITY_GROUP_ID -c kafka_topic=$KAFKA_TOPIC -c deploy_msk=false -c deploy_lambda=true $SUBNET_CONTEXT"

# Build Lambda layer
echo "Building Lambda layer..."
LAYER_DIR="$COMMON_DIR/lambda_layer/python"
rm -rf "$COMMON_DIR/lambda_layer"
mkdir -p "$LAYER_DIR"
pip install kafka-python-ng aws-msk-iam-sasl-signer-python -t "$LAYER_DIR" --quiet
echo "✅ Lambda layer built"
echo ""

cd "$COMMON_DIR/cdk"

# Install CDK Python dependencies
pip install -r requirements.txt --quiet

echo "Deploying Lambda stack..."
cdk deploy LambdaDataGenStack --require-approval never $CDK_CONTEXT

# Extract outputs
STACK=LambdaDataGenStack
LAMBDA_FUNCTION=$(aws cloudformation describe-stacks --stack-name $STACK \
    --query "Stacks[0].Outputs[?OutputKey=='LambdaFunctionName'].OutputValue" --output text)
KAFKA_TOPIC_OUT=$(aws cloudformation describe-stacks --stack-name $STACK \
    --query "Stacks[0].Outputs[?OutputKey=='KafkaTopic'].OutputValue" --output text)

cd "$COMMON_DIR"

# Update Lambda with bootstrap servers if available
if [ "$KAFKA_BOOTSTRAP_SERVERS" != "PENDING" ] && [ -n "$KAFKA_BOOTSTRAP_SERVERS" ]; then
    echo "Updating Lambda environment with MSK bootstrap servers..."
    aws lambda update-function-configuration \
        --function-name "$LAMBDA_FUNCTION" \
        --environment "Variables={KAFKA_BOOTSTRAP_SERVERS=$KAFKA_BOOTSTRAP_SERVERS,KAFKA_TOPIC=$KAFKA_TOPIC_OUT,NUM_EVENTS=100,NUM_CUSTOMERS=5,NUM_TENANTS=3}" \
        --query 'FunctionName' --output text > /dev/null
    echo "✅ Lambda environment updated"
else
    echo "⚠️  Bootstrap servers not available yet. Update Lambda manually after MSK is ready:"
    echo "   aws lambda update-function-configuration --function-name $LAMBDA_FUNCTION \\"
    echo "     --environment 'Variables={KAFKA_BOOTSTRAP_SERVERS=<servers>,KAFKA_TOPIC=$KAFKA_TOPIC_OUT,...}'"
fi

# Save to shared env file
cat > .env.lambda << EOF
LAMBDA_FUNCTION=$LAMBDA_FUNCTION
KAFKA_TOPIC=$KAFKA_TOPIC_OUT
EOF

echo ""
echo "✅ Lambda deployed. Outputs saved to .env.lambda"
echo ""
echo "  Function:    $LAMBDA_FUNCTION"
echo "  Kafka Topic: $KAFKA_TOPIC_OUT"
echo ""
