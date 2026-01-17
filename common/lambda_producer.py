#!/usr/bin/env python3
"""
AWS Lambda Function: Endpoint Security Logs Producer

This Lambda function receives endpoint security events and publishes them
to a Kafka topic for downstream processing.

Trigger: API Gateway, EventBridge, or direct invocation
Output: Kafka topic "endpoint_logs"
"""

import json
import os
from datetime import datetime
from typing import Dict, Any
from kafka import KafkaProducer
from kafka.errors import KafkaError


class EndpointLogsProducer:
    """
    Produces endpoint security log events to Kafka.
    """
    
    def __init__(self):
        """Initialize Kafka producer with configuration."""
        self.kafka_bootstrap_servers = os.environ.get(
            'KAFKA_BOOTSTRAP_SERVERS',
            'localhost:9092'
        )
        self.kafka_topic = os.environ.get('KAFKA_TOPIC', 'endpoint_logs')
        
        # Initialize Kafka producer
        self.producer = KafkaProducer(
            bootstrap_servers=self.kafka_bootstrap_servers.split(','),
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8') if k else None,
            acks='all',  # Wait for all replicas
            retries=3,
            max_in_flight_requests_per_connection=1
        )
    
    def enrich_event(self, event: Dict[str, Any]) -> Dict[str, Any]:
        """
        Enrich the security event with additional metadata.
        
        Args:
            event: Raw security event
            
        Returns:
            Enriched event with metadata
        """
        enriched = event.copy()
        
        # Add processing metadata
        enriched['ingestion_timestamp'] = datetime.utcnow().isoformat()
        enriched['source'] = 'lambda_producer'
        enriched['version'] = '1.0'
        
        # Ensure required fields exist
        if 'event_id' not in enriched:
            enriched['event_id'] = f"evt_{datetime.utcnow().timestamp()}"
        
        if 'severity' not in enriched:
            enriched['severity'] = 'INFO'
        
        return enriched
    
    def publish_event(self, event: Dict[str, Any]) -> bool:
        """
        Publish security event to Kafka topic.
        
        Args:
            event: Security event to publish
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Enrich event
            enriched_event = self.enrich_event(event)
            
            # Use tenant_id or customer_id as partition key for ordering
            partition_key = enriched_event.get('tenant_id') or enriched_event.get('customer_id')
            
            # Publish to Kafka
            future = self.producer.send(
                self.kafka_topic,
                key=partition_key,
                value=enriched_event
            )
            
            # Wait for confirmation
            record_metadata = future.get(timeout=10)
            
            print(f"Event published successfully: topic={record_metadata.topic}, "
                  f"partition={record_metadata.partition}, offset={record_metadata.offset}")
            
            return True
            
        except KafkaError as e:
            print(f"Failed to publish event to Kafka: {e}")
            return False
        except Exception as e:
            print(f"Unexpected error publishing event: {e}")
            return False
    
    def close(self):
        """Close Kafka producer connection."""
        if self.producer:
            self.producer.flush()
            self.producer.close()


def lambda_handler(event, context):
    """
    AWS Lambda handler function.
    
    Expected event format:
    {
        "customer_id": "cust_12345",
        "tenant_id": "tenant_abc",
        "device_id": "device_xyz",
        "device_name": "LAPTOP-001",
        "device_type": "laptop",
        "event_type": "file_access",
        "event_category": "security",
        "severity": "HIGH",
        "timestamp": "2024-01-16T10:30:00Z",
        "user": "john.doe@company.com",
        "process_name": "chrome.exe",
        "file_path": "/etc/passwd",
        "action": "read",
        "result": "blocked",
        "ip_address": "192.168.1.100",
        "os": "Windows 10",
        "os_version": "10.0.19045"
    }
    
    Args:
        event: Lambda event containing security log data
        context: Lambda context
        
    Returns:
        Response with status and message
    """
    producer = None
    
    try:
        # Initialize producer
        producer = EndpointLogsProducer()
        
        # Handle batch events
        events_to_process = []
        
        if isinstance(event, list):
            events_to_process = event
        elif 'Records' in event:
            # Handle SQS/SNS/EventBridge events
            events_to_process = [json.loads(record.get('body', '{}')) for record in event['Records']]
        else:
            # Single event
            events_to_process = [event]
        
        # Process each event
        success_count = 0
        failure_count = 0
        
        for evt in events_to_process:
            if producer.publish_event(evt):
                success_count += 1
            else:
                failure_count += 1
        
        return {
            'statusCode': 200 if failure_count == 0 else 207,
            'body': json.dumps({
                'message': 'Events processed',
                'success_count': success_count,
                'failure_count': failure_count,
                'total': len(events_to_process)
            })
        }
        
    except Exception as e:
        print(f"Lambda handler error: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps({
                'message': 'Internal server error',
                'error': str(e)
            })
        }
    
    finally:
        if producer:
            producer.close()


# For local testing
if __name__ == "__main__":
    # Sample endpoint security event
    sample_event = {
        "customer_id": "cust_12345",
        "tenant_id": "tenant_abc",
        "device_id": "device_xyz_001",
        "device_name": "LAPTOP-JOHN-001",
        "device_type": "laptop",
        "event_type": "file_access",
        "event_category": "security",
        "severity": "HIGH",
        "timestamp": "2024-01-16T10:30:00Z",
        "user": "john.doe@company.com",
        "process_name": "chrome.exe",
        "file_path": "/etc/passwd",
        "action": "read",
        "result": "blocked",
        "ip_address": "192.168.1.100",
        "os": "Windows 10",
        "os_version": "10.0.19045",
        "threat_detected": True,
        "threat_type": "unauthorized_access"
    }
    
    # Test the handler
    response = lambda_handler(sample_event, None)
    print(f"\nResponse: {json.dumps(response, indent=2)}")
