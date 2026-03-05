#!/bin/bash
set -e

echo "=== Cleaning up Lambda Data Generator ==="
echo ""

export AWS_PROFILE=${AWS_PROFILE:-default}
export AWS_REGION=${AWS_REGION:-us-east-1}
COMMON_DIR="$(dirname "$0")/.."

STACK_NAME="LambdaDataGenStack"

STACK_STATUS=$(aws cloudformation describe-stacks \
    --stack-name $STACK_NAME \
    --query 'Stacks[0].StackStatus' \
    --output text 2>/dev/null || echo "NOT_FOUND")

if [ "$STACK_STATUS" = "NOT_FOUND" ]; then
    echo "Stack not found. Nothing to clean up."
    rm -f "$COMMON_DIR/.env.lambda"
    exit 0
fi

echo "Stack status: $STACK_STATUS"

case $STACK_STATUS in
    ROLLBACK_FAILED|DELETE_FAILED)
        echo "Force deleting stack..."
        aws cloudformation delete-stack --stack-name $STACK_NAME --deletion-mode FORCE_DELETE_STACK 2>/dev/null || true
        aws cloudformation wait stack-delete-complete --stack-name $STACK_NAME 2>/dev/null || true
        ;;
    *_IN_PROGRESS)
        echo "Stack operation in progress. Wait and retry."
        exit 1
        ;;
    *)
        echo "Deleting Lambda stack..."
        cd "$COMMON_DIR/cdk"
        cdk destroy LambdaDataGenStack --force \
            -c vpc_id=${VPC_ID:-dummy} \
            -c msk_security_group_id=${MSK_SECURITY_GROUP_ID:-dummy} \
            -c deploy_msk=false -c deploy_lambda=true 2>/dev/null || {
            cd "$COMMON_DIR"
            aws cloudformation delete-stack --stack-name $STACK_NAME
        }
        cd "$COMMON_DIR" 2>/dev/null || true
        aws cloudformation wait stack-delete-complete --stack-name $STACK_NAME 2>/dev/null || true
        ;;
esac

# Clean up build artifacts
rm -rf "$COMMON_DIR/lambda_layer"
rm -f "$COMMON_DIR/.env.lambda"

echo "✅ Lambda stack cleaned up"
echo ""
