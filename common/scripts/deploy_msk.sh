#!/bin/bash
set -e

echo "=== Deploying MSK Serverless Cluster ==="
echo ""

export AWS_PROFILE=${AWS_PROFILE:-default}
export AWS_REGION=${AWS_REGION:-us-east-1}
AWS_ACCOUNT=$(aws sts get-caller-identity --query Account --output text)

if [ -z "$VPC_ID" ]; then
    echo "ERROR: VPC_ID is not set. export VPC_ID=vpc-xxxxxxxx"
    exit 1
fi

SUBNET_CONTEXT=""
if [ -n "$SUBNET_IDS" ]; then
    # Validate at least 2 subnets for MSK Serverless
    SUBNET_COUNT=$(echo "$SUBNET_IDS" | tr ',' '\n' | wc -l | tr -d ' ')
    if [ "$SUBNET_COUNT" -lt 2 ]; then
        echo "ERROR: MSK Serverless requires at least 2 subnets in different AZs."
        echo "  You provided $SUBNET_COUNT: $SUBNET_IDS"
        echo "  export SUBNET_IDS=subnet-aaa,subnet-bbb"
        exit 1
    fi
    SUBNET_CONTEXT="-c subnet_ids=$SUBNET_IDS"
fi

CDK_CONTEXT="-c vpc_id=$VPC_ID -c deploy_msk=true -c deploy_lambda=false $SUBNET_CONTEXT"

cd "$(dirname "$0")/../cdk"

# Install CDK Python dependencies
pip install -r requirements.txt --quiet

echo "Bootstrapping CDK..."
cdk bootstrap aws://$AWS_ACCOUNT/$AWS_REGION $CDK_CONTEXT

echo ""
echo "Deploying MSK stack..."
cdk deploy MSKStack --require-approval never $CDK_CONTEXT

# Extract outputs
STACK=MSKStack
MSK_CLUSTER_ARN=$(aws cloudformation describe-stacks --stack-name $STACK \
    --query "Stacks[0].Outputs[?OutputKey=='MSKClusterArn'].OutputValue" --output text)
MSK_SG_ID=$(aws cloudformation describe-stacks --stack-name $STACK \
    --query "Stacks[0].Outputs[?OutputKey=='MSKSecurityGroupId'].OutputValue" --output text)
SUBNET_IDS_OUT=$(aws cloudformation describe-stacks --stack-name $STACK \
    --query "Stacks[0].Outputs[?OutputKey=='SubnetIds'].OutputValue" --output text)

cd "$(dirname "$0")/.."

# Get bootstrap servers
echo ""
echo "Retrieving MSK bootstrap servers..."
KAFKA_BOOTSTRAP_SERVERS=$(aws kafka get-bootstrap-brokers \
    --cluster-arn "$MSK_CLUSTER_ARN" \
    --query 'BootstrapBrokerStringSaslIam' \
    --output text 2>/dev/null || echo "PENDING")

if [ "$KAFKA_BOOTSTRAP_SERVERS" = "None" ] || [ -z "$KAFKA_BOOTSTRAP_SERVERS" ]; then
    KAFKA_BOOTSTRAP_SERVERS="PENDING"
    echo "⚠️  Bootstrap servers not yet available (cluster still provisioning)."
    echo "   Retrieve later: aws kafka get-bootstrap-brokers --cluster-arn $MSK_CLUSTER_ARN"
else
    echo "✅ Bootstrap servers: $KAFKA_BOOTSTRAP_SERVERS"
fi

# Save to shared env file
cat > .env.msk << EOF
MSK_CLUSTER_ARN=$MSK_CLUSTER_ARN
MSK_SECURITY_GROUP_ID=$MSK_SG_ID
KAFKA_BOOTSTRAP_SERVERS=$KAFKA_BOOTSTRAP_SERVERS
VPC_ID=$VPC_ID
SUBNET_IDS=$SUBNET_IDS_OUT
AWS_REGION=$AWS_REGION
EOF

echo ""
echo "✅ MSK deployed. Outputs saved to .env.msk"
echo ""
echo "  Cluster ARN:       $MSK_CLUSTER_ARN"
echo "  Security Group:    $MSK_SG_ID"
echo "  Bootstrap Servers: $KAFKA_BOOTSTRAP_SERVERS"
echo ""
