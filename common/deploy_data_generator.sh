#!/bin/bash
# Deployment script for Lambda Data Generator function

set -e

# Configuration
FUNCTION_NAME="endpoint-logs-data-generator"
RUNTIME="python3.11"
HANDLER="lambda_data_generator.lambda_handler"
# ROLE_ARN="arn:aws:iam::YOUR_ACCOUNT_ID:role/lambda-execution-role"  # Update this
ROLE_ARN="arn:aws:iam::328901611032:role/lambda_eni_access"  # Update this
# KAFKA_BOOTSTRAP_SERVERS="your-msk-broker:9092"  # Update with MSK bootstrap servers
KAFKA_BOOTSTRAP_SERVERS="b-2.democluster.c3otci.c12.kafka.us-east-1.amazonaws.com:9092,b-1.democluster.c3otci.c12.kafka.us-east-1.amazonaws.com:9092"  # Update with MSK bootstrap servers
KAFKA_TOPIC="endpoint_logs"
NUM_EVENTS="100"  # Number of events to generate per execution
NUM_CUSTOMERS="5"  # Number of unique customers
NUM_TENANTS="3"  # Number of unique tenants

# VPC Configuration (required for MSK in VPC)
VPC_ENABLED="true"  # Set to "false" to disable VPC
# VPC_SUBNET_IDS="subnet-xxxxx,subnet-yyyyy"  # Update with your private subnet IDs (comma-separated)
# VPC_SECURITY_GROUP_IDS="sg-xxxxx"  # Update with your security group ID(s) (comma-separated)
VPC_SUBNET_IDS="subnet-01519cdbe933f4646,subnet-0ef7ab6a7bdc20a76"  # Update with your private subnet IDs (comma-separated)
VPC_SECURITY_GROUP_IDS="sg-0e6adec4102285acd"  # Update with your security group ID(s) (comma-separated)

echo "=========================================="
echo "Lambda Data Generator Deployment Script"
echo "=========================================="
echo "Function: $FUNCTION_NAME"
echo "Runtime: $RUNTIME"
echo "Events per execution: $NUM_EVENTS"
echo "VPC Enabled: $VPC_ENABLED"
if [ "$VPC_ENABLED" = "true" ]; then
    echo "VPC Subnets: $VPC_SUBNET_IDS"
    echo "Security Groups: $VPC_SECURITY_GROUP_IDS"
fi
echo ""

# Create temporary directory for packaging
PACKAGE_DIR="lambda_package_generator"
ZIP_FILE="lambda_data_generator.zip"

echo "1. Creating package directory..."
rm -rf $PACKAGE_DIR
mkdir -p $PACKAGE_DIR

echo "2. Installing dependencies..."
pip install kafka-python -t $PACKAGE_DIR/

echo "3. Copying Lambda function code..."
cp lambda_data_generator.py $PACKAGE_DIR/

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
        --environment Variables="{KAFKA_BOOTSTRAP_SERVERS=\"$KAFKA_BOOTSTRAP_SERVERS\",KAFKA_TOPIC=$KAFKA_TOPIC,NUM_EVENTS=$NUM_EVENTS,NUM_CUSTOMERS=$NUM_CUSTOMERS,NUM_TENANTS=$NUM_TENANTS}"
    
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
            --timeout 300 \
            --memory-size 512 \
            --environment Variables="{KAFKA_BOOTSTRAP_SERVERS=\"$KAFKA_BOOTSTRAP_SERVERS\",KAFKA_TOPIC=$KAFKA_TOPIC,NUM_EVENTS=$NUM_EVENTS,NUM_CUSTOMERS=$NUM_CUSTOMERS,NUM_TENANTS=$NUM_TENANTS}" \
            --vpc-config SubnetIds=$VPC_SUBNET_IDS,SecurityGroupIds=$VPC_SECURITY_GROUP_IDS
    else
        aws lambda create-function \
            --function-name $FUNCTION_NAME \
            --runtime $RUNTIME \
            --handler $HANDLER \
            --zip-file fileb://$ZIP_FILE \
            --role $ROLE_ARN \
            --timeout 300 \
            --memory-size 512 \
            --environment Variables="{KAFKA_BOOTSTRAP_SERVERS=\"$KAFKA_BOOTSTRAP_SERVERS\",KAFKA_TOPIC=$KAFKA_TOPIC,NUM_EVENTS=$NUM_EVENTS,NUM_CUSTOMERS=$NUM_CUSTOMERS,NUM_TENANTS=$NUM_TENANTS}"
    fi
fi

# echo "6. Setting up EventBridge schedule (optional)..."
# echo "   Creating EventBridge rule to trigger every 5 minutes..."
# 
# # Create EventBridge rule
# RULE_NAME="endpoint-logs-generator-schedule"
# aws events put-rule \
#     --name $RULE_NAME \
#     --schedule-expression "rate(5 minutes)" \
#     --state ENABLED \
#     --description "Trigger endpoint logs data generator every 5 minutes" \
#     2>/dev/null || echo "   Rule already exists or failed to create"
# 
# # Add Lambda permission for EventBridge
# aws lambda add-permission \
#     --function-name $FUNCTION_NAME \
#     --statement-id AllowEventBridgeInvoke \
#     --action lambda:InvokeFunction \
#     --principal events.amazonaws.com \
#     --source-arn "arn:aws:events:us-east-1:YOUR_ACCOUNT_ID:rule/$RULE_NAME" \
#     2>/dev/null || echo "   Permission already exists"
# 
# # Add Lambda as target
# aws events put-targets \
#     --rule $RULE_NAME \
#     --targets "Id"="1","Arn"="arn:aws:lambda:us-east-1:YOUR_ACCOUNT_ID:function/$FUNCTION_NAME" \
#     2>/dev/null || echo "   Target already exists"

echo "7. Cleaning up..."
rm -rf $PACKAGE_DIR
rm -f $ZIP_FILE

echo ""
echo "=========================================="
echo "✅ Deployment Complete!"
echo "=========================================="
echo ""
echo "Configuration:"
echo "  Function: $FUNCTION_NAME"
echo "  Events per execution: $NUM_EVENTS"
echo "  Customers: $NUM_CUSTOMERS"
echo "  Tenants: $NUM_TENANTS"
echo "  Kafka topic: $KAFKA_TOPIC"
echo "  VPC Enabled: $VPC_ENABLED"
if [ "$VPC_ENABLED" = "true" ]; then
    echo "  VPC Subnets: $VPC_SUBNET_IDS"
    echo "  Security Groups: $VPC_SECURITY_GROUP_IDS"
fi
echo "  Schedule: Every 5 minutes (via EventBridge)"
echo ""
echo "Test the function manually:"
echo "  aws lambda invoke \\"
echo "    --function-name $FUNCTION_NAME \\"
echo "    response.json"
echo ""
echo "View logs:"
echo "  aws logs tail /aws/lambda/$FUNCTION_NAME --follow"
echo ""
echo "Disable scheduled execution:"
echo "  aws events disable-rule --name $RULE_NAME"
echo ""
echo "Enable scheduled execution:"
echo "  aws events enable-rule --name $RULE_NAME"
echo ""
if [ "$VPC_ENABLED" = "true" ]; then
    echo "⚠️  VPC Configuration Notes:"
    echo "  - Lambda execution role must have AWSLambdaVPCAccessExecutionRole policy"
    echo "  - Security group must allow outbound traffic to MSK on port 9092"
    echo "  - Subnets must have NAT Gateway for internet access (if needed)"
    echo "  - Cold start times may be longer (10-15 seconds) due to ENI creation"
fi
