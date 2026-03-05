#!/bin/bash
set -e

echo "=== Starting Flink Streaming Application ==="
echo ""

export AWS_PROFILE=${AWS_PROFILE:-default}

if [ ! -f .env ]; then
    echo "ERROR: .env not found. Run ./scripts/deploy.sh first."
    exit 1
fi
source .env

echo "Application: $APP_NAME"
echo "Region:      $AWS_REGION"
echo ""

# Check current status
STATUS=$(aws kinesisanalyticsv2 describe-application \
    --application-name "$APP_NAME" \
    --region "$AWS_REGION" \
    --query 'ApplicationDetail.ApplicationStatus' \
    --output text 2>/dev/null || echo "NOT_FOUND")

if [ "$STATUS" = "RUNNING" ]; then
    echo "Application is already RUNNING."
    exit 0
fi

if [ "$STATUS" = "STARTING" ]; then
    echo "Application is already STARTING. Wait for it to reach RUNNING."
    exit 0
fi

if [ "$STATUS" != "READY" ]; then
    echo "Application status: $STATUS (expected READY to start)"
    echo "  If UPDATING, wait and retry."
    exit 1
fi

# Parse flags
SKIP_RESTORE=false
for arg in "$@"; do
    case $arg in
        --skip-restore) SKIP_RESTORE=true ;;
    esac
done

if [ "$SKIP_RESTORE" = true ]; then
    echo "Starting application (skipping snapshot restore)..."
    aws kinesisanalyticsv2 start-application \
        --application-name "$APP_NAME" \
        --region "$AWS_REGION" \
        --run-configuration '{"ApplicationRestoreConfiguration":{"ApplicationRestoreType":"SKIP_RESTORE_FROM_SNAPSHOT"}}'
else
    echo "Starting application..."
    aws kinesisanalyticsv2 start-application \
        --application-name "$APP_NAME" \
        --region "$AWS_REGION" \
        --run-configuration '{}'
fi

echo ""
echo "Waiting for application to start..."
PREV_STATUS=""
ATTEMPT=1
for i in $(seq 1 60); do
    STATUS=$(aws kinesisanalyticsv2 describe-application \
        --application-name "$APP_NAME" \
        --region "$AWS_REGION" \
        --query 'ApplicationDetail.ApplicationStatus' \
        --output text 2>/dev/null)

    if [ "$STATUS" != "$PREV_STATUS" ]; then
        echo "  Status: $STATUS"
        PREV_STATUS="$STATUS"
    fi

    if [ "$STATUS" = "RUNNING" ]; then
        echo ""
        echo "✅ Application is RUNNING"
        echo ""
        echo "Next steps:"
        echo "  1. Generate data:  ./scripts/generate_data.sh"
        echo "  2. View logs:      aws logs tail $LOG_GROUP --follow --region $AWS_REGION"
        echo ""
        echo "Stop:"
        echo "  aws kinesisanalyticsv2 stop-application --application-name $APP_NAME --region $AWS_REGION"
        exit 0
    fi

    if [ "$STATUS" = "READY" ] || [ "$STATUS" = "STOPPING" ]; then
        if [ "$ATTEMPT" -eq 1 ] && [ "$SKIP_RESTORE" = false ]; then
            echo ""
            echo "⚠️  Start failed — retrying with --skip-restore (snapshot may be incompatible)..."
            ATTEMPT=2
            SKIP_RESTORE=true
            aws kinesisanalyticsv2 start-application \
                --application-name "$APP_NAME" \
                --region "$AWS_REGION" \
                --run-configuration '{"ApplicationRestoreConfiguration":{"ApplicationRestoreType":"SKIP_RESTORE_FROM_SNAPSHOT"}}'
            PREV_STATUS=""
            continue
        fi
        echo ""
        echo "❌ Application failed to start (status: $STATUS)"
        echo "  Check logs: aws logs tail $LOG_GROUP --follow --region $AWS_REGION"
        exit 1
    fi

    sleep 10
done

echo ""
echo "⚠️  Timed out waiting for RUNNING state. Current status: $STATUS"
echo "  Check: aws kinesisanalyticsv2 describe-application --application-name $APP_NAME --region $AWS_REGION"
