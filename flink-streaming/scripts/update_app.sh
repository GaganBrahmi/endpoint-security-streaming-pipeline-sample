#!/bin/bash
set -e

echo "=== Updating Flink Application Code ==="
echo ""

export AWS_PROFILE=${AWS_PROFILE:-default}

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
FLINK_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

if [ ! -f "$FLINK_DIR/.env" ]; then
    echo "ERROR: .env not found. Run ./scripts/deploy.sh first."
    exit 1
fi
source "$FLINK_DIR/.env"

# Rebuild
echo "Step 1: Rebuilding application..."
"$SCRIPT_DIR/build.sh"

cd "$FLINK_DIR"

# Upload new ZIP
echo "Step 2: Uploading new ZIP to S3..."
aws s3 cp flink-application.zip "s3://${ARTIFACT_BUCKET}/${APP_S3_KEY}"
echo "  ✅ Uploaded"

# Stop if running
STATUS=$(aws kinesisanalyticsv2 describe-application \
    --application-name "$APP_NAME" \
    --region "$AWS_REGION" \
    --query 'ApplicationDetail.ApplicationStatus' \
    --output text 2>/dev/null)

if [ "$STATUS" = "RUNNING" ] || [ "$STATUS" = "STARTING" ] || [ "$STATUS" = "AUTOSCALING" ]; then
    echo ""
    echo "Step 3: Stopping running application..."

    # Try graceful stop first, fall back to force stop
    aws kinesisanalyticsv2 stop-application \
        --application-name "$APP_NAME" \
        --region "$AWS_REGION" 2>/dev/null || {
        echo "  ⚠️  Graceful stop failed — using force stop (skips snapshot)..."
        aws kinesisanalyticsv2 stop-application \
            --application-name "$APP_NAME" \
            --region "$AWS_REGION" \
            --force 2>/dev/null || true
    }

    echo "  Waiting for READY state..."
    for i in $(seq 1 30); do
        STATUS=$(aws kinesisanalyticsv2 describe-application \
            --application-name "$APP_NAME" \
            --region "$AWS_REGION" \
            --query 'ApplicationDetail.ApplicationStatus' \
            --output text 2>/dev/null)
        if [ "$STATUS" = "READY" ]; then
            echo "  ✅ Application stopped"
            break
        fi
        sleep 10
    done
fi

# Update application code reference
echo ""
echo "Step 4: Updating application code reference..."
CURRENT_VERSION=$(aws kinesisanalyticsv2 describe-application \
    --application-name "$APP_NAME" \
    --region "$AWS_REGION" \
    --query 'ApplicationDetail.ApplicationVersionId' \
    --output text)

aws kinesisanalyticsv2 update-application \
    --application-name "$APP_NAME" \
    --region "$AWS_REGION" \
    --current-application-version-id "$CURRENT_VERSION" \
    --application-configuration-update '{
        "ApplicationCodeConfigurationUpdate": {
            "CodeContentUpdate": {
                "S3ContentLocationUpdate": {
                    "BucketARNUpdate": "arn:aws:s3:::'"$ARTIFACT_BUCKET"'",
                    "FileKeyUpdate": "'"$APP_S3_KEY"'"
                }
            }
        }
    }'

echo "  ✅ Application updated"
echo ""
echo "Next steps:"
echo "  1. Start app:      ./scripts/start_app.sh"
echo "  2. Generate data:  ./scripts/generate_data.sh"
echo ""
