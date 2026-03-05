#!/bin/bash
set -e

echo "=== Cleaning up Spark Streaming S3 Stack ==="
echo ""

export AWS_PROFILE=${AWS_PROFILE:-default}
export AWS_REGION=${AWS_REGION:-us-east-1}

STACK_NAME="SparkStreamingS3Stack"

if [ -f .env ]; then
    source .env

    # Cancel ALL running/pending EMR jobs before stopping the application
    if [ -n "$EMR_APP_ID" ]; then
        echo "Checking for active EMR job runs..."
        ACTIVE_JOBS=$(aws emr-serverless list-job-runs \
            --application-id "$EMR_APP_ID" \
            --states RUNNING PENDING SUBMITTED SCHEDULED QUEUED \
            --query 'jobRuns[].id' \
            --output text 2>/dev/null || echo "")

        if [ -n "$ACTIVE_JOBS" ] && [ "$ACTIVE_JOBS" != "None" ]; then
            for JOB_ID in $ACTIVE_JOBS; do
                echo "  Cancelling job $JOB_ID..."
                aws emr-serverless cancel-job-run \
                    --application-id "$EMR_APP_ID" \
                    --job-run-id "$JOB_ID" 2>/dev/null \
                    && echo "  ✅ Cancelled $JOB_ID" \
                    || echo "  ⚠️  Failed to cancel $JOB_ID (may already be stopped)"
            done

            echo "  Waiting for jobs to finish cancelling..."
            for i in $(seq 1 20); do
                STILL_ACTIVE=$(aws emr-serverless list-job-runs \
                    --application-id "$EMR_APP_ID" \
                    --states RUNNING PENDING SUBMITTED SCHEDULED QUEUED CANCELLING \
                    --query 'jobRuns[].id' \
                    --output text 2>/dev/null || echo "")
                if [ -z "$STILL_ACTIVE" ] || [ "$STILL_ACTIVE" = "None" ]; then
                    echo "  ✅ All jobs stopped"
                    break
                fi
                sleep 5
            done
            echo ""
        else
            echo "  No active jobs found"
        fi
    fi

    # Stop EMR Serverless application if running
    if [ -n "$EMR_APP_ID" ]; then
        APP_STATE=$(aws emr-serverless get-application \
            --application-id "$EMR_APP_ID" \
            --query 'application.state' \
            --output text 2>/dev/null || echo "UNKNOWN")

        if [ "$APP_STATE" = "STARTED" ] || [ "$APP_STATE" = "STARTING" ]; then
            echo "Stopping EMR Serverless application $EMR_APP_ID (state: $APP_STATE)..."
            aws emr-serverless stop-application \
                --application-id "$EMR_APP_ID" 2>/dev/null \
                && echo "  ✅ Stop requested. Waiting for application to stop..." \
                || echo "  ⚠️  Stop request failed"

            for i in $(seq 1 30); do
                APP_STATE=$(aws emr-serverless get-application \
                    --application-id "$EMR_APP_ID" \
                    --query 'application.state' \
                    --output text 2>/dev/null || echo "UNKNOWN")
                if [ "$APP_STATE" = "STOPPED" ] || [ "$APP_STATE" = "TERMINATED" ]; then
                    echo "  ✅ Application stopped"
                    break
                fi
                sleep 10
            done
            echo ""
        else
            echo "EMR application state: $APP_STATE (no stop needed)"
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

# Delete stack
STACK_STATUS=$(aws cloudformation describe-stacks \
    --stack-name $STACK_NAME \
    --query 'Stacks[0].StackStatus' \
    --output text 2>/dev/null || echo "NOT_FOUND")

if [ "$STACK_STATUS" = "NOT_FOUND" ]; then
    echo "Stack not found."
    rm -f .env
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
        echo "Deleting stack (S3 bucket has auto-delete enabled)..."
        cd cdk
        cdk destroy --force \
            -c vpc_id=${VPC_ID:-dummy} \
            -c msk_security_group_id=${MSK_SECURITY_GROUP_ID:-dummy} 2>/dev/null || {
            cd ..
            aws cloudformation delete-stack --stack-name $STACK_NAME
        }
        cd .. 2>/dev/null || true
        aws cloudformation wait stack-delete-complete --stack-name $STACK_NAME 2>/dev/null || true
        ;;
esac

# Clean up S3 buckets
if [ -f .env ]; then
    source .env
    [ -n "$SCRIPTS_BUCKET" ] && aws s3 rb s3://$SCRIPTS_BUCKET --force 2>/dev/null || true
    [ -n "$CHECKPOINT_BUCKET" ] && aws s3 rb s3://$CHECKPOINT_BUCKET --force 2>/dev/null || true
fi

rm -f .env
echo ""
echo "✅ Spark Streaming S3 stack cleaned up"
echo "   MSK and Lambda are managed separately via common/scripts/"
echo ""
