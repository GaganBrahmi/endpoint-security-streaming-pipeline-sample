#!/bin/bash
set -e

echo "=== Cleaning up Spark Streaming Stack ==="
echo ""

export AWS_PROFILE=${AWS_PROFILE:-default}
export AWS_REGION=${AWS_REGION:-us-east-1}

STACK_NAME="SparkStreamingStack"

# Revoke Lake Formation permissions
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

            # Wait for the application to reach STOPPED state
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

    if [ -n "$EMR_ROLE_ARN" ] && [ -n "$S3_TABLES_CATALOG_ID" ]; then
        echo "Revoking Lake Formation permissions..."

        # Revoke catalog-level
        aws lakeformation revoke-permissions \
            --cli-input-json '{
                "Principal": { "DataLakePrincipalIdentifier": "'"$EMR_ROLE_ARN"'" },
                "Resource": { "Catalog": { "Id": "'"$S3_TABLES_CATALOG_ID"'" } },
                "Permissions": ["ALTER", "CREATE_DATABASE", "DESCRIBE", "DROP"]
            }' 2>/dev/null && echo "  ✅ Catalog permissions revoked" \
            || echo "  ⚠️  Catalog revoke failed (may already be removed)"

        # Revoke all remaining LF permissions for this principal
        echo "Checking for additional LF permissions..."
        PERMS=$(aws lakeformation list-permissions \
            --principal '{"DataLakePrincipalIdentifier": "'"$EMR_ROLE_ARN"'"}' \
            --output json 2>/dev/null || echo '{"PrincipalResourcePermissions": []}')

        echo "$PERMS" | python3 -c "
import sys, json, subprocess
data = json.load(sys.stdin)
for e in data.get('PrincipalResourcePermissions', []):
    p, r, perms = e.get('Principal',{}), e.get('Resource',{}), e.get('Permissions',[])
    if not perms: continue
    inp = {'Principal': p, 'Resource': r, 'Permissions': perms}
    result = subprocess.run(['aws','lakeformation','revoke-permissions','--cli-input-json',json.dumps(inp)], capture_output=True, text=True)
    status = '✅' if result.returncode == 0 else '⚠️'
    print(f'  {status} Revoked {perms}')
" 2>/dev/null || true
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
        # Empty the S3 Table bucket before CDK destroy (CDK can't delete non-empty buckets)
        if [ -f .env ]; then
            source .env
            if [ -n "$BUCKET_NAME" ] && [ -n "$BUCKET_ARN" ]; then
                echo "Emptying S3 Table bucket: $BUCKET_NAME..."
                NAMESPACES=$(aws s3tables list-namespaces \
                    --table-bucket-arn "$BUCKET_ARN" \
                    --query 'namespaces[].namespace[0]' \
                    --output text 2>/dev/null || echo "")

                for NS in $NAMESPACES; do
                    echo "  Deleting tables in namespace: $NS"
                    TABLES=$(aws s3tables list-tables \
                        --table-bucket-arn "$BUCKET_ARN" \
                        --namespace "$NS" \
                        --query 'tables[].name' \
                        --output text 2>/dev/null || echo "")

                    for TBL in $TABLES; do
                        aws s3tables delete-table \
                            --table-bucket-arn "$BUCKET_ARN" \
                            --namespace "$NS" \
                            --name "$TBL" 2>/dev/null \
                            && echo "    ✅ Deleted table: $NS.$TBL" \
                            || echo "    ⚠️  Failed to delete table: $NS.$TBL"
                    done

                    aws s3tables delete-namespace \
                        --table-bucket-arn "$BUCKET_ARN" \
                        --namespace "$NS" 2>/dev/null \
                        && echo "  ✅ Deleted namespace: $NS" \
                        || echo "  ⚠️  Failed to delete namespace: $NS"
                done
                echo ""
            fi
        fi

        echo "Deleting stack..."
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
echo "✅ Spark Streaming stack cleaned up"
echo "   MSK and Lambda are managed separately via common/scripts/"
echo ""
