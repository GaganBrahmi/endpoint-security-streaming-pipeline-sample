#!/bin/bash
set -e

echo "=== Invoking Lambda Data Generator ==="
echo ""

export AWS_PROFILE=${AWS_PROFILE:-default}
COMMON_DIR="$(dirname "$0")/.."

# Load Lambda env
if [ ! -f "$COMMON_DIR/.env.lambda" ]; then
    echo "ERROR: .env.lambda not found. Deploy Lambda first: ./scripts/deploy_lambda.sh"
    exit 1
fi
source "$COMMON_DIR/.env.lambda"

INVOCATIONS=${1:-1}

echo "Lambda Function: $LAMBDA_FUNCTION"
echo "Kafka Topic:     $KAFKA_TOPIC"
echo "Invocations:     $INVOCATIONS"
echo ""

for i in $(seq 1 $INVOCATIONS); do
    echo "Invocation $i/$INVOCATIONS..."
    RESPONSE=$(aws lambda invoke \
        --function-name "$LAMBDA_FUNCTION" \
        --payload '{}' \
        --cli-binary-format raw-in-base64-out \
        /tmp/lambda_response.json \
        --query 'StatusCode' --output text)

    if [ "$RESPONSE" = "200" ]; then
        STATUS=$(cat /tmp/lambda_response.json | python3 -c "import sys,json; print(json.load(sys.stdin).get('statusCode','?'))" 2>/dev/null || echo "?")
        if [ "$STATUS" = "200" ]; then
            echo "  ✅ Success"
        else
            echo "  ⚠️  Lambda returned status $STATUS"
            cat /tmp/lambda_response.json
        fi
    else
        echo "  ❌ Invocation failed (HTTP $RESPONSE)"
    fi

    [ "$i" -lt "$INVOCATIONS" ] && sleep 1
done

echo ""
echo "Done. $INVOCATIONS invocation(s) completed."
rm -f /tmp/lambda_response.json
