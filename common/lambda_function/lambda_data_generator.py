#!/usr/bin/env python3
"""
AWS Lambda Function: Endpoint Security Logs Data Generator

This Lambda function generates realistic fake endpoint security events
and publishes them to Kafka for testing and demo purposes.

Trigger: EventBridge (scheduled), CloudWatch Events, or direct invocation
Output: Kafka topic "endpoint_logs"

Environment Variables:
- KAFKA_BOOTSTRAP_SERVERS: Kafka broker addresses (default: localhost:9092)
- KAFKA_TOPIC: Kafka topic name (default: endpoint_logs)
- NUM_EVENTS: Number of events to generate per execution (default: 100)
- NUM_CUSTOMERS: Number of unique customers to generate (default: 5)
- NUM_TENANTS: Number of unique tenants to generate (default: 3)
"""

import json
import os
import random
from datetime import datetime, timedelta
from typing import Dict, Any, List
from kafka import KafkaProducer
from kafka.errors import KafkaError


class FakeDataGenerator:
    """
    Generates realistic fake endpoint security events.
    """
    
    # Event type configurations
    EVENT_TYPES = [
        "file_access",
        "network_connection",
        "process_execution",
        "registry_modification",
        "usb_device_connected",
        "data_exfiltration",
        "authentication",
        "privilege_escalation",
        "app_installation",
        "ransomware_activity",
        "malware_detection",
        "firewall_block",
        "dns_query",
        "certificate_validation"
    ]
    
    DEVICE_TYPES = ["laptop", "desktop", "server", "tablet", "mobile"]
    
    OPERATING_SYSTEMS = [
        ("Windows 10", "10.0.19045"),
        ("Windows 11", "10.0.22621"),
        ("Windows Server 2022", "10.0.20348"),
        ("macOS", "14.2.1"),
        ("Ubuntu 22.04", "22.04.3"),
        ("iOS", "17.2.1"),
        ("Android", "14.0")
    ]
    
    SEVERITIES = ["LOW", "MEDIUM", "HIGH", "CRITICAL"]
    
    THREAT_TYPES = [
        "unauthorized_access",
        "malware_execution",
        "data_exfiltration",
        "persistence_attempt",
        "privilege_escalation",
        "ransomware",
        "phishing",
        "brute_force",
        "sql_injection",
        "command_injection"
    ]
    
    PROCESS_NAMES = [
        "chrome.exe", "firefox.exe", "outlook.exe", "word.exe", "excel.exe",
        "powershell.exe", "cmd.exe", "python.exe", "java.exe", "node.exe",
        "regedit.exe", "taskmgr.exe", "explorer.exe", "svchost.exe", "system"
    ]
    
    FILE_PATHS = [
        "/etc/passwd", "/etc/shadow", "/var/log/auth.log",
        "C:\\Windows\\System32\\config\\SAM",
        "C:\\Users\\{user}\\Documents\\confidential.xlsx",
        "C:\\Users\\{user}\\AppData\\Local\\Temp\\malware.exe",
        "/tmp/suspicious.sh", "/home/{user}/.ssh/id_rsa",
        "HKEY_LOCAL_MACHINE\\SOFTWARE\\Microsoft\\Windows\\CurrentVersion\\Run"
    ]
    
    ACTIONS = ["read", "write", "execute", "delete", "modify", "connect", "upload", "download", "install", "encrypt"]
    
    RESULTS = ["allowed", "blocked", "quarantined"]
    
    def __init__(self, num_customers: int = 5, num_tenants: int = 3):
        """
        Initialize the fake data generator.
        
        Args:
            num_customers: Number of unique customer IDs to generate
            num_tenants: Number of unique tenant IDs to generate
        """
        # Generate customer and tenant IDs
        self.customer_ids = [f"cust_{i:05d}" for i in range(1, num_customers + 1)]
        self.tenant_ids = [f"tenant_{chr(97 + i)}" for i in range(num_tenants)]  # tenant_a, tenant_b, etc.
        self.event_counter = 0
        
        print(f"Initialized generator with {len(self.customer_ids)} customers and {len(self.tenant_ids)} tenants")
        print(f"  Customers: {', '.join(self.customer_ids[:3])}{'...' if len(self.customer_ids) > 3 else ''}")
        print(f"  Tenants: {', '.join(self.tenant_ids)}")
    
    def generate_event(self) -> Dict[str, Any]:
        """
        Generate a single realistic fake endpoint security event.
        
        Returns:
            Dictionary containing the fake event data
        """
        self.event_counter += 1
        
        # Select random attributes
        customer_id = random.choice(self.customer_ids)
        tenant_id = random.choice(self.tenant_ids)
        device_type = random.choice(self.DEVICE_TYPES)
        event_type = random.choice(self.EVENT_TYPES)
        severity = random.choice(self.SEVERITIES)
        os_name, os_version = random.choice(self.OPERATING_SYSTEMS)
        
        # Generate device info
        device_id = f"device_{random.randint(1000, 9999)}"
        device_name = f"{device_type.upper()}-{random.choice(['JOHN', 'JANE', 'BOB', 'ALICE', 'CHARLIE'])}-{random.randint(1, 999):03d}"
        
        # Generate user info
        user_names = ["john.doe", "jane.smith", "bob.wilson", "alice.brown", "charlie.davis", "diana.evans", "frank.garcia"]
        user = f"{random.choice(user_names)}@company.com"
        
        # Generate process and file info
        process_name = random.choice(self.PROCESS_NAMES)
        file_path = random.choice(self.FILE_PATHS).replace("{user}", user.split("@")[0].split(".")[0])
        
        # Generate action and result
        action = random.choice(self.ACTIONS)
        result = random.choice(self.RESULTS)
        
        # Determine if threat detected (higher probability for HIGH/CRITICAL severity)
        threat_detected = False
        threat_type = None
        
        if severity in ["HIGH", "CRITICAL"]:
            threat_detected = random.random() > 0.3  # 70% chance
        elif severity == "MEDIUM":
            threat_detected = random.random() > 0.7  # 30% chance
        else:
            threat_detected = random.random() > 0.9  # 10% chance
        
        if threat_detected:
            threat_type = random.choice(self.THREAT_TYPES)
            result = "blocked"  # Threats are usually blocked
        
        # Generate timestamp (within last 5 minutes)
        timestamp = datetime.utcnow() - timedelta(seconds=random.randint(0, 300))
        
        # Generate IP address
        ip_address = f"{random.randint(10, 192)}.{random.randint(0, 255)}.{random.randint(0, 255)}.{random.randint(1, 254)}"
        
        # Build event
        event = {
            "event_id": f"evt_{int(timestamp.timestamp())}_{self.event_counter}",
            "customer_id": customer_id,
            "tenant_id": tenant_id,
            "device_id": device_id,
            "device_name": device_name,
            "device_type": device_type,
            "event_type": event_type,
            "event_category": "security",
            "severity": severity,
            "timestamp": timestamp.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "user": user,
            "process_name": process_name,
            "file_path": file_path if event_type in ["file_access", "data_exfiltration", "malware_detection"] else None,
            "action": action,
            "result": result,
            "ip_address": ip_address,
            "os": os_name,
            "os_version": os_version,
            "threat_detected": threat_detected,
            "threat_type": threat_type
        }
        
        return event
    
    def generate_batch(self, num_events: int) -> List[Dict[str, Any]]:
        """
        Generate a batch of fake events.
        
        Args:
            num_events: Number of events to generate
            
        Returns:
            List of fake events
        """
        return [self.generate_event() for _ in range(num_events)]


class EndpointLogsDataGenerator:
    """
    Generates and publishes fake endpoint security events to Kafka.
    """
    
    def __init__(self):
        """Initialize Kafka producer and data generator with configuration."""
        # Kafka configuration
        self.kafka_bootstrap_servers = os.environ.get(
            'KAFKA_BOOTSTRAP_SERVERS',
            'localhost:9092'
        )
        self.kafka_topic = os.environ.get('KAFKA_TOPIC', 'endpoint_logs')
        
        # Data generation configuration
        self.num_events = int(os.environ.get('NUM_EVENTS', '100'))
        self.num_customers = int(os.environ.get('NUM_CUSTOMERS', '5'))
        self.num_tenants = int(os.environ.get('NUM_TENANTS', '3'))
        
        # Initialize Kafka producer
        self.producer = KafkaProducer(
            bootstrap_servers=self.kafka_bootstrap_servers.split(','),
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8') if k else None,
            acks='all',
            retries=3,
            max_in_flight_requests_per_connection=1
        )
        
        # Initialize data generator
        self.data_generator = FakeDataGenerator(self.num_customers, self.num_tenants)
    
    def enrich_event(self, event: Dict[str, Any]) -> Dict[str, Any]:
        """
        Enrich the generated event with additional metadata.
        
        Args:
            event: Generated security event
            
        Returns:
            Enriched event with metadata
        """
        enriched = event.copy()
        
        # Add processing metadata
        enriched['ingestion_timestamp'] = datetime.utcnow().isoformat()
        enriched['source'] = 'lambda_data_generator'
        enriched['version'] = '1.0'
        enriched['generated'] = True  # Mark as generated data
        
        return enriched
    
    def publish_event(self, event: Dict[str, Any]) -> bool:
        """
        Publish event to Kafka topic.
        
        Args:
            event: Event to publish
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Enrich event
            enriched_event = self.enrich_event(event)
            
            # Use tenant_id as partition key for ordering
            partition_key = enriched_event.get('tenant_id')
            
            # Publish to Kafka
            future = self.producer.send(
                self.kafka_topic,
                key=partition_key,
                value=enriched_event
            )
            
            # Wait for confirmation
            record_metadata = future.get(timeout=10)
            
            return True
            
        except KafkaError as e:
            print(f"Failed to publish event to Kafka: {e}")
            return False
        except Exception as e:
            print(f"Unexpected error publishing event: {e}")
            return False
    
    def generate_and_publish(self) -> Dict[str, Any]:
        """
        Generate fake events and publish them to Kafka.
        
        Returns:
            Summary of generation and publishing results
        """
        print(f"Generating {self.num_events} fake endpoint security events...")
        
        # Generate events
        events = self.data_generator.generate_batch(self.num_events)
        
        print(f"Publishing {len(events)} events to Kafka topic '{self.kafka_topic}'...")
        
        # Publish events
        success_count = 0
        failure_count = 0
        
        for event in events:
            if self.publish_event(event):
                success_count += 1
            else:
                failure_count += 1
        
        # Flush producer
        self.producer.flush()
        
        results = {
            "total_generated": len(events),
            "success_count": success_count,
            "failure_count": failure_count,
            "kafka_topic": self.kafka_topic,
            "num_customers": self.num_customers,
            "num_tenants": self.num_tenants,
            "customer_ids": self.data_generator.customer_ids,
            "tenant_ids": self.data_generator.tenant_ids
        }
        
        print(f"Generation complete: {success_count} published, {failure_count} failed")
        
        return results
    
    def close(self):
        """Close Kafka producer connection."""
        if self.producer:
            self.producer.flush()
            self.producer.close()


def lambda_handler(event, context):
    """
    AWS Lambda handler function for data generation.
    
    This function generates fake endpoint security events and publishes them to Kafka.
    The number of events is controlled by the NUM_EVENTS environment variable.
    
    Args:
        event: Lambda event (ignored for data generation)
        context: Lambda context
        
    Returns:
        Response with generation statistics
    """
    generator = None
    
    try:
        # Initialize generator
        generator = EndpointLogsDataGenerator()
        
        # Generate and publish events
        results = generator.generate_and_publish()
        
        return {
            'statusCode': 200 if results['failure_count'] == 0 else 207,
            'body': json.dumps({
                'message': 'Data generation complete',
                'total_generated': results['total_generated'],
                'success_count': results['success_count'],
                'failure_count': results['failure_count'],
                'kafka_topic': results['kafka_topic'],
                'num_customers': results['num_customers'],
                'num_tenants': results['num_tenants'],
                'sample_customer_ids': results['customer_ids'][:3],
                'sample_tenant_ids': results['tenant_ids']
            })
        }
        
    except Exception as e:
        print(f"Lambda handler error: {e}")
        import traceback
        traceback.print_exc()
        
        return {
            'statusCode': 500,
            'body': json.dumps({
                'message': 'Data generation failed',
                'error': str(e)
            })
        }
    
    finally:
        if generator:
            generator.close()


# For local testing
if __name__ == "__main__":
    print("=" * 60)
    print("Endpoint Security Logs Data Generator - Local Test")
    print("=" * 60)
    
    # Set test configuration
    os.environ['NUM_EVENTS'] = '20'  # Generate 20 events for testing
    os.environ['NUM_CUSTOMERS'] = '3'  # Generate 3 customers
    os.environ['NUM_TENANTS'] = '2'  # Generate 2 tenants
    
    # Test the handler
    response = lambda_handler({}, None)
    print(f"\nResponse: {json.dumps(response, indent=2)}")
