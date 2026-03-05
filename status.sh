#!/bin/bash
#
# Status check for all streaming pipeline components.
# Run from customer_usecase/ directory.
#

export AWS_PROFILE=${AWS_PROFILE:-default}
export AWS_REGION=${AWS_REGION:-us-east-1}

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

# Colors
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[0;33m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color
BOLD='\033[1m'

status_icon() {
    local s
    s=$(echo "$1" | tr '[:lower:]' '[:upper:]')
    case "$s" in
        RUNNING|CREATE_COMPLETE|UPDATE_COMPLETE|ACTIVE|READY|STARTED|STOPPED|CREATED)
            echo -e "${GREEN}●${NC}" ;;
        STARTING|UPDATING|*_IN_PROGRESS|CREATING|STOPPING)
            echo -e "${YELLOW}◐${NC}" ;;
        NOT_FOUND|NOT_DEPLOYED)
            echo -e "${NC}○${NC}" ;;
        *)
            echo -e "${RED}●${NC}" ;;
    esac
}

check_stack() {
    local stack_name="$1"
    local status
    status=$(aws cloudformation describe-stacks \
        --stack-name "$stack_name" \
        --region "$AWS_REGION" \
        --query 'Stacks[0].StackStatus' \
        --output text 2>/dev/null || echo "NOT_FOUND")
    echo "$status"
}

echo ""
echo -e "${BOLD}════════════════════════════════════════════════════════════${NC}"
echo -e "${BOLD}  Streaming Pipeline Status  (${AWS_REGION})${NC}"
echo -e "${BOLD}════════════════════════════════════════════════════════════${NC}"
echo ""

# ── Common: MSK ──
echo -e "${CYAN}▸ Common Infrastructure${NC}"

MSK_STACK=$(check_stack "MSKStack")
echo -e "  $(status_icon "$MSK_STACK")  MSK Stack:              $MSK_STACK"

COMMON_DIR="$SCRIPT_DIR/common"

if [ -f "$COMMON_DIR/.env.msk" ]; then
    source "$COMMON_DIR/.env.msk"
    if [ -n "$MSK_CLUSTER_ARN" ]; then
        MSK_STATE=$(aws kafka describe-cluster-v2 \
            --cluster-arn "$MSK_CLUSTER_ARN" \
            --region "$AWS_REGION" \
            --query 'ClusterInfo.State' \
            --output text 2>/dev/null || echo "UNKNOWN")
        echo -e "  $(status_icon "$MSK_STATE")  MSK Cluster:             $MSK_STATE"
        [ -n "$KAFKA_BOOTSTRAP_SERVERS" ] && [ "$KAFKA_BOOTSTRAP_SERVERS" != "PENDING" ] && \
            echo -e "     Bootstrap Servers:      $KAFKA_BOOTSTRAP_SERVERS"
    fi
else
    echo -e "  $(status_icon "NOT_DEPLOYED")  .env.msk:                Not found"
fi

# ── Common: Lambda ──
LAMBDA_STACK=$(check_stack "LambdaDataGenStack")
echo -e "  $(status_icon "$LAMBDA_STACK")  Lambda Stack:            $LAMBDA_STACK"

if [ -f "$COMMON_DIR/.env.lambda" ]; then
    source "$COMMON_DIR/.env.lambda"
    if [ -n "$LAMBDA_FUNCTION" ]; then
        LAMBDA_STATE=$(aws lambda get-function \
            --function-name "$LAMBDA_FUNCTION" \
            --region "$AWS_REGION" \
            --query 'Configuration.State' \
            --output text 2>/dev/null || echo "NOT_FOUND")
        echo -e "  $(status_icon "$LAMBDA_STATE")  Lambda Function:         $LAMBDA_STATE ($LAMBDA_FUNCTION)"
        [ -n "$KAFKA_TOPIC" ] && echo -e "     Kafka Topic:           $KAFKA_TOPIC"
    fi
else
    echo -e "  $(status_icon "NOT_DEPLOYED")  .env.lambda:             Not found"
fi

echo ""

# ── Flink Streaming ──
echo -e "${CYAN}▸ Flink Streaming (Amazon MSF)${NC}"

FLINK_STACK=$(check_stack "FlinkStreamingStack")
echo -e "  $(status_icon "$FLINK_STACK")  CDK Stack:               $FLINK_STACK"

if [ -f "$SCRIPT_DIR/flink-streaming/.env" ]; then
    source "$SCRIPT_DIR/flink-streaming/.env"
    if [ -n "$APP_NAME" ]; then
        FLINK_STATUS=$(aws kinesisanalyticsv2 describe-application \
            --application-name "$APP_NAME" \
            --region "$AWS_REGION" \
            --query 'ApplicationDetail.ApplicationStatus' \
            --output text 2>/dev/null || echo "NOT_FOUND")
        echo -e "  $(status_icon "$FLINK_STATUS")  MSF Application:         $FLINK_STATUS ($APP_NAME)"
    fi
    [ -n "$DATABASE" ] && echo -e "     Database:              $DATABASE"
    [ -n "$WAREHOUSE_BUCKET" ] && echo -e "     Warehouse:             s3://$WAREHOUSE_BUCKET"
    [ -n "$LOG_GROUP" ] && echo -e "     Log Group:             $LOG_GROUP"
else
    echo -e "  $(status_icon "NOT_DEPLOYED")  .env:                    Not found (not deployed)"
fi

echo ""

# ── Spark Streaming S3 ──
echo -e "${CYAN}▸ Spark Streaming (Standard S3)${NC}"

SPARK_S3_STACK=$(check_stack "SparkStreamingS3Stack")
echo -e "  $(status_icon "$SPARK_S3_STACK")  CDK Stack:               $SPARK_S3_STACK"

if [ -f "$SCRIPT_DIR/spark-streaming-s3/.env" ]; then
    source "$SCRIPT_DIR/spark-streaming-s3/.env"
    if [ -n "$EMR_APP_ID" ]; then
        EMR_STATE=$(aws emr-serverless get-application \
            --application-id "$EMR_APP_ID" \
            --region "$AWS_REGION" \
            --query 'application.state' \
            --output text 2>/dev/null || echo "NOT_FOUND")
        echo -e "  $(status_icon "$EMR_STATE")  EMR Application:         $EMR_STATE ($EMR_APP_ID)"
    fi
    [ -n "$DATABASE" ] && echo -e "     Database:              $DATABASE"
    [ -n "$WAREHOUSE_BUCKET" ] && echo -e "     Warehouse:             s3://$WAREHOUSE_BUCKET"
else
    echo -e "  $(status_icon "NOT_DEPLOYED")  .env:                    Not found (not deployed)"
fi

echo ""

# ── Spark Streaming S3 Tables ──
echo -e "${CYAN}▸ Spark Streaming (S3 Tables)${NC}"

SPARK_S3T_STACK=$(check_stack "SparkStreamingStack")
echo -e "  $(status_icon "$SPARK_S3T_STACK")  CDK Stack:               $SPARK_S3T_STACK"

if [ -f "$SCRIPT_DIR/spark-streaming-s3tables/.env" ]; then
    source "$SCRIPT_DIR/spark-streaming-s3tables/.env"
    if [ -n "$EMR_APP_ID" ]; then
        EMR_STATE=$(aws emr-serverless get-application \
            --application-id "$EMR_APP_ID" \
            --region "$AWS_REGION" \
            --query 'application.state' \
            --output text 2>/dev/null || echo "NOT_FOUND")
        echo -e "  $(status_icon "$EMR_STATE")  EMR Application:         $EMR_STATE ($EMR_APP_ID)"
    fi
    [ -n "$NAMESPACE" ] && echo -e "     Namespace/Database:    $NAMESPACE"
    [ -n "$BUCKET_NAME" ] && echo -e "     Table Bucket:          $BUCKET_NAME"
else
    echo -e "  $(status_icon "NOT_DEPLOYED")  .env:                    Not found (not deployed)"
fi

echo ""
echo -e "${BOLD}════════════════════════════════════════════════════════════${NC}"
echo ""
