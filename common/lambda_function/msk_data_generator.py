"""
Lambda function: Generate endpoint security events → MSK Serverless (IAM auth).

Reuses FakeDataGenerator from customer_usecase/common/lambda_function/lambda_data_generator.py
but uses kafka-python-ng with MSK IAM authentication (SASL_SSL + OAUTHBEARER).

Environment Variables:
    KAFKA_BOOTSTRAP_SERVERS: MSK IAM bootstrap servers (port 9098)
    KAFKA_TOPIC: Kafka topic name (default: endpoint_logs)
    NUM_EVENTS: Events per invocation (default: 100)
    NUM_CUSTOMERS: Unique customers (default: 5)
    NUM_TENANTS: Unique tenants (default: 3)
"""

import json
import os
import socket
from datetime import datetime

from kafka import KafkaProducer, KafkaAdminClient
from kafka.admin import NewTopic
from kafka.errors import TopicAlreadyExistsError

# Import FakeDataGenerator from the common module (packaged alongside this file)
from lambda_data_generator import FakeDataGenerator


def _msk_oauth_cb(config):
    """OAUTHBEARER callback for MSK IAM auth via kafka-python-ng."""
    # kafka-python-ng's OAUTHBEARER with MSK IAM uses the
    # aws_msk_iam_sasl_signer library if available, otherwise
    # we use boto3 directly.
    import botocore.session
    from botocore.credentials import Credentials

    session = botocore.session.get_session()
    credentials = session.get_credentials().get_frozen_credentials()
    region = os.environ.get("AWS_REGION", "us-east-1")

    # Build the signed token using MSK IAM signer
    from aws_msk_iam_sasl_signer import MSKAuthTokenProvider

    token, expiry_ms = MSKAuthTokenProvider.generate_auth_token(region)
    return token, expiry_ms / 1000.0


def _create_producer():
    """Create KafkaProducer with MSK IAM auth."""
    bootstrap = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

    # Detect if we should use IAM auth (port 9098 = IAM endpoint)
    use_iam = ":9098" in bootstrap

    if use_iam:
        producer = KafkaProducer(
            bootstrap_servers=bootstrap.split(","),
            security_protocol="SASL_SSL",
            sasl_mechanism="OAUTHBEARER",
            sasl_oauth_token_provider=_MSKTokenProvider(),
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8") if k else None,
            acks="all",
            retries=3,
            max_in_flight_requests_per_connection=1,
            request_timeout_ms=30000,
            api_version_auto_timeout_ms=30000,
        )
    else:
        producer = KafkaProducer(
            bootstrap_servers=bootstrap.split(","),
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8") if k else None,
            acks="all",
            retries=3,
            max_in_flight_requests_per_connection=1,
        )
    return producer


class _MSKTokenProvider:
    """OAUTHBEARER token provider for MSK IAM auth."""

    def token(self):
        from aws_msk_iam_sasl_signer import MSKAuthTokenProvider

        region = os.environ.get("AWS_REGION", "us-east-1")
        token, expiry_ms = MSKAuthTokenProvider.generate_auth_token(region)
        return token

    def extensions(self):
        return {}


def _ensure_topic(bootstrap_servers, topic, use_iam):
    """Create Kafka topic if it doesn't exist."""
    try:
        if use_iam:
            admin = KafkaAdminClient(
                bootstrap_servers=bootstrap_servers.split(","),
                security_protocol="SASL_SSL",
                sasl_mechanism="OAUTHBEARER",
                sasl_oauth_token_provider=_MSKTokenProvider(),
                request_timeout_ms=30000,
                api_version_auto_timeout_ms=30000,
            )
        else:
            admin = KafkaAdminClient(
                bootstrap_servers=bootstrap_servers.split(","),
            )

        admin.create_topics([
            NewTopic(name=topic, num_partitions=3, replication_factor=2)
        ])
        print(f"Created topic: {topic}")
        admin.close()
    except TopicAlreadyExistsError:
        print(f"Topic already exists: {topic}")
    except Exception as e:
        print(f"Topic creation note: {e}")


def lambda_handler(event, context):
    """Generate fake endpoint security events and publish to MSK."""
    bootstrap = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    topic = os.environ.get("KAFKA_TOPIC", "endpoint_logs")
    num_events = int(os.environ.get("NUM_EVENTS", "100"))
    num_customers = int(os.environ.get("NUM_CUSTOMERS", "5"))
    num_tenants = int(os.environ.get("NUM_TENANTS", "3"))
    use_iam = ":9098" in bootstrap

    print(f"Bootstrap: {bootstrap}")
    print(f"Topic: {topic}, Events: {num_events}, IAM: {use_iam}")

    try:
        # Ensure topic exists
        _ensure_topic(bootstrap, topic, use_iam)

        # Generate events
        generator = FakeDataGenerator(num_customers, num_tenants)
        events = generator.generate_batch(num_events)

        # Publish
        producer = _create_producer()
        success = 0
        failed = 0

        for evt in events:
            # Add enrichment metadata
            evt["ingestion_timestamp"] = datetime.utcnow().isoformat()
            evt["source"] = "msk_data_generator"
            evt["version"] = "2.0"
            evt["generated"] = True

            try:
                producer.send(topic, key=evt.get("tenant_id"), value=evt).get(timeout=10)
                success += 1
            except Exception as e:
                print(f"Send failed: {e}")
                failed += 1

        producer.flush()
        producer.close()

        print(f"Published {success}/{num_events} events ({failed} failed)")

        return {
            "statusCode": 200 if failed == 0 else 207,
            "body": json.dumps({
                "message": "Data generation complete",
                "total_generated": num_events,
                "success_count": success,
                "failure_count": failed,
                "kafka_topic": topic,
            }),
        }

    except Exception as e:
        import traceback
        traceback.print_exc()
        return {
            "statusCode": 500,
            "body": json.dumps({"message": "Failed", "error": str(e)}),
        }
