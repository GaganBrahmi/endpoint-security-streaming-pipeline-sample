#!/bin/bash
# Deployment script for Lambda Producer function

set -e

# Configuration
FUNCTION_NAME="endpoint-logs-producer"
RUNTIME="python3.11"
HANDLER="lambda_producer.lambda_handler"
ROLE_ARN="arn:aws:iam::YOUR_ACCOUNT_ID:role/lambda-execution-role"  # Update this
KAFKA_BOOTSTRAP_SERVERS="your-msk-broker:9092"  # Update with MSK bootstrap servers
KAFKA_TOPIC="endpoint_logs"

# VPC Configuration (required for MSK in VPC)
VPC_ENABLED="true"  # Set to "false" to disable VPC
VPC_SUBNET_IDS="subnet-xxxxx,subnet-yyyyy"  # Update with your private subnet IDs (comma-separated)
VPC_SECURITY_GROUP_IDS="sg-xxxxx"  # Update with your security group ID(s) (comma-separated)

echo "=========================================="
echo "Lambda Producer Deployment Script"
echo "=========================================="
echo "Function: $FUNCTION_NAME"
echo "Runtime: $RUNTIME"
echo "VPC Enabled: $VPC_ENABLED"
if [ "$VPC_ENABLED" = "true" ]; then
    echo "VPC Subnets: $VPC_SUBNET_IDS"
    echo "Security Groups: $VPC_SECURITY_GROUP_IDS"
fi
echo ""

# Create temporary directory for packaging
PACKAGE_DIR="lambda_package"
ZIP_FILE="lambda_function.zip"

echo "1. Creating package directory..."
rm -rf $PACKAGE_DIR
mkdir -p $PACKAGE_DIR

echo "2. Installing dependencies..."
pip install kafka-python -t $PACKAGE_DIR/

echo "3. Copying Lambda function code..."
cp lambda_producer.py $PACKAGE_DIR/

echo "4. Creating deployment package..."
cd $PACKAGE_DIR
zip -r ../$ZIP_FILE . > /dev/null
cd ..

echo "5. Checking if function exists..."
if aws lambda get-function --function-name $FUNCTION_NAME 2>/dev/null; then
    echo "   Function exists. Updating code..."
    aws lambda update-function-code \
        --function-name $FUNCTION_NAME \
        --zip-file fileb://$ZIP_FILE
    
    echo "   Updating environment variables..."
    aws lambda update-function-configuration \
        --function-name $FUNCTION_NAME \
        --environment Variables="{KAFKA_BOOTSTRAP_SERVERS=\"$KAFKA_BOOTSTRAP_SERVERS\",KAFKA_TOPIC=$KAFKA_TOPIC}"
    
    # Update VPC configuration if enabled
    if [ "$VPC_ENABLED" = "true" ]; then
        echo "   Updating VPC configuration..."
        aws lambda update-function-configuration \
            --function-name $FUNCTION_NAME \
            --vpc-config SubnetIds=$VPC_SUBNET_IDS,SecurityGroupIds=$VPC_SECURITY_GROUP_IDS
    fi
else
    echo "   Function does not exist. Creating..."
    
    # Add VPC configuration if enabled
    if [ "$VPC_ENABLED" = "true" ]; then
        aws lambda create-function \
            --function-name $FUNCTION_NAME \
            --runtime $RUNTIME \
            --handler $HANDLER \
            --zip-file fileb://$ZIP_FILE \
            --role $ROLE_ARN \
            --timeout 60 \
            --memory-size 512 \
            --environment Variables="{KAFKA_BOOTSTRAP_SERVERS=\"$KAFKA_BOOTSTRAP_SERVERS\",KAFKA_TOPIC=$KAFKA_TOPIC}" \
            --vpc-config SubnetIds=$VPC_SUBNET_IDS,SecurityGroupIds=$VPC_SECURITY_GROUP_IDS
    else
        aws lambda create-function \
            --function-name $FUNCTION_NAME \
            --runtime $RUNTIME \
            --handler $HANDLER \
            --zip-file fileb://$ZIP_FILE \
            --role $ROLE_ARN \
            --timeout 60 \
            --memory-size 512 \
            --environment Variables="{KAFKA_BOOTSTRAP_SERVERS=\"$KAFKA_BOOTSTRAP_SERVERS\",KAFKA_TOPIC=$KAFKA_TOPIC}"
    fi
fi

echo "6. Cleaning up..."
rm -rf $PACKAGE_DIR
rm -f $ZIP_FILE

echo ""
echo "=========================================="
echo "✅ Deployment Complete!"
echo "=========================================="
echo ""
echo "Configuration:"
echo "  Function: $FUNCTION_NAME"
echo "  Kafka topic: $KAFKA_TOPIC"
echo "  VPC Enabled: $VPC_ENABLED"
if [ "$VPC_ENABLED" = "true" ]; then
    echo "  VPC Subnets: $VPC_SUBNET_IDS"
    echo "  Security Groups: $VPC_SECURITY_GROUP_IDS"
fi
echo ""
echo "Test the function:"
echo "  aws lambda invoke \\"
echo "    --function-name $FUNCTION_NAME \\"
echo "    --payload file://sample_events.json \\"
echo "    response.json"
echo ""
echo "View logs:"
echo "  aws logs tail /aws/lambda/$FUNCTION_NAME --follow"
echo ""
if [ "$VPC_ENABLED" = "true" ]; then
    echo "⚠️  VPC Configuration Notes:"
    echo "  - Lambda execution role must have AWSLambdaVPCAccessExecutionRole policy"
    echo "  - Security group must allow outbound traffic to MSK on port 9092"
    echo "  - Subnets must have NAT Gateway for internet access (if needed)"
    echo "  - Cold start times may be longer (10-15 seconds) due to ENI creation"
fi
