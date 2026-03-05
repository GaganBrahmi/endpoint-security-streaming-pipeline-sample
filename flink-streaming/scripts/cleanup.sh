#!/bin/bash
set -e

echo "=== Cleaning up Flink Streaming Stack ==="
echo ""

export AWS_PROFILE=${AWS_PROFILE:-default}
export AWS_REGION=${AWS_REGION:-us-east-1}

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
FLINK_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

STACK_NAME="FlinkStreamingStack"

if [ -f "$FLINK_DIR/.env" ]; then
    source "$FLINK_DIR/.env"

    # Stop MSF application if running (must stop before CDK can delete it)
    if [ -n "$APP_NAME" ]; then
        STATUS=$(aws kinesisanalyticsv2 describe-application \
            --application-name "$APP_NAME" \
            --region "$AWS_REGION" \
            --query 'ApplicationDetail.ApplicationStatus' \
            --output text 2>/dev/null || echo "NOT_FOUND")

        if [ "$STATUS" = "RUNNING" ] || [ "$STATUS" = "STARTING" ] || [ "$STATUS" = "AUTOSCALING" ]; then
            echo "Stopping Flink application $APP_NAME (status: $STATUS)..."
            aws kinesisanalyticsv2 stop-application \
                --application-name "$APP_NAME" \
                --region "$AWS_REGION" 2>/dev/null || {
                echo "  ⚠️  Graceful stop failed — using force stop..."
                aws kinesisanalyticsv2 stop-application \
                    --application-name "$APP_NAME" \
                    --region "$AWS_REGION" \
                    --force 2>/dev/null || true
            }

            echo "  Waiting for application to stop..."
            for i in $(seq 1 30); do
                STATUS=$(aws kinesisanalyticsv2 describe-application \
                    --application-name "$APP_NAME" \
                    --region "$AWS_REGION" \
                    --query 'ApplicationDetail.ApplicationStatus' \
                    --output text 2>/dev/null || echo "UNKNOWN")
                if [ "$STATUS" = "READY" ] || [ "$STATUS" = "NOT_FOUND" ]; then
                    echo "  ✅ Application stopped"
                    break
                fi
                sleep 10
            done
            echo ""
        else
            echo "Flink application status: $STATUS (no stop needed)"
        fi
    fi

    # Drop Glue database tables
    if [ -n "$DATABASE" ]; then
        echo "Cleaning up Glue database: $DATABASE..."
        TABLES=$(aws glue get-tables \
            --database-name "$DATABASE" \
            --query 'TableList[].Name' \
            --output text 2>/dev/null || echo "")

        for TBL in $TABLES; do
            aws glue delete-table \
                --database-name "$DATABASE" \
                --name "$TBL" 2>/dev/null \
                && echo "  ✅ Deleted table: $DATABASE.$TBL" \
                || echo "  ⚠️  Failed to delete table: $DATABASE.$TBL"
        done

        aws glue delete-database \
            --name "$DATABASE" 2>/dev/null \
            && echo "  ✅ Deleted database: $DATABASE" \
            || echo "  ⚠️  Failed to delete database: $DATABASE"
        echo ""
    fi
fi

# Delete CDK stack (includes MSF app, S3 bucket with auto-delete, IAM, SG, logs)
STACK_STATUS=$(aws cloudformation describe-stacks \
    --stack-name $STACK_NAME \
    --query 'Stacks[0].StackStatus' \
    --output text 2>/dev/null || echo "NOT_FOUND")

if [ "$STACK_STATUS" = "NOT_FOUND" ]; then
    echo "Stack not found."
else
    echo "Stack status: $STACK_STATUS"

    case $STACK_STATUS in
        ROLLBACK_COMPLETE|ROLLBACK_FAILED|DELETE_FAILED)
            echo "Force deleting stack..."
            aws cloudformation delete-stack --stack-name $STACK_NAME --deletion-mode FORCE_DELETE_STACK 2>/dev/null || true
            aws cloudformation wait stack-delete-complete --stack-name $STACK_NAME 2>/dev/null || true
            ;;
        *_IN_PROGRESS)
            echo "Stack operation in progress. Wait and retry."
            exit 1
            ;;
        *)
            echo "Deleting CDK stack (S3 bucket has auto-delete enabled)..."
            cd "$FLINK_DIR/cdk"
            # Use deploy_app=true so CDK sees the full stack for clean destroy
            cdk destroy --force \
                -c vpc_id=${VPC_ID:-dummy} \
                -c msk_security_group_id=${MSK_SECURITY_GROUP_ID:-dummy} \
                -c kafka_bootstrap_servers=${KAFKA_BOOTSTRAP_SERVERS:-dummy} \
                -c deploy_app=true 2>/dev/null || {
                cd "$FLINK_DIR"
                aws cloudformation delete-stack --stack-name $STACK_NAME
            }
            cd "$FLINK_DIR" 2>/dev/null || true
            aws cloudformation wait stack-delete-complete --stack-name $STACK_NAME 2>/dev/null || true
            ;;
    esac
    echo "  ✅ Stack deleted"
fi

rm -f "$FLINK_DIR/.env"
rm -f "$FLINK_DIR/flink-application.zip"
rm -rf "$FLINK_DIR/target/" "$FLINK_DIR/lib/" "$FLINK_DIR/build/"

echo ""
echo "✅ Flink Streaming stack cleaned up"
echo "   MSK and Lambda are managed separately via common/scripts/"
echo ""
echo "To redeploy:"
echo "  ./scripts/deploy.sh"
echo ""
