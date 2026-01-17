# Lambda Producer vs Lambda Data Generator

## Quick Comparison

| Feature | Lambda Producer | Lambda Data Generator |
|---------|----------------|----------------------|
| **Purpose** | Process real events | Generate fake events |
| **Input** | Real endpoint security events | None (generates internally) |
| **Output** | Enriched events to Kafka | Generated events to Kafka |
| **Use Case** | Production deployment | Testing, demos, load testing |
| **Deployment** | `./deploy_lambda.sh` | `./deploy_data_generator.sh` |
| **Trigger** | API Gateway, EventBridge, SQS | EventBridge schedule, manual |
| **Configuration** | Kafka settings only | Kafka + generation settings |

## When to Use Each

### Use Lambda Producer When:
- ✅ You have real endpoint agents sending events
- ✅ You're in production environment
- ✅ You need to process actual security events
- ✅ You're integrating with existing security infrastructure
- ✅ Compliance requires real event processing

### Use Lambda Data Generator When:
- ✅ You're testing the pipeline
- ✅ You need demo data for presentations
- ✅ You're doing load testing
- ✅ You don't have access to real endpoint agents
- ✅ You're in development/QA environment
- ✅ You want to validate analytics queries

## Configuration Comparison

### Lambda Producer Environment Variables
```bash
KAFKA_BOOTSTRAP_SERVERS=your-kafka-broker:9092
KAFKA_TOPIC=endpoint_logs
```

### Lambda Data Generator Environment Variables
```bash
KAFKA_BOOTSTRAP_SERVERS=your-kafka-broker:9092
KAFKA_TOPIC=endpoint_logs
NUM_EVENTS=100                              # NEW: Events per execution
NUM_CUSTOMERS=5                             # NEW: Number of unique customers (auto-generated)
NUM_TENANTS=3                               # NEW: Number of unique tenants (auto-generated)
```

**Note:** Customer and tenant IDs are automatically generated:
- Customers: `cust_00001`, `cust_00002`, ..., `cust_00005`
- Tenants: `tenant_a`, `tenant_b`, `tenant_c`

## Event Differences

### Lambda Producer Events
- Source: Real endpoint agents
- `source` field: `"lambda_producer"`
- `generated` field: Not present
- Data quality: Real-world variations
- Timing: Based on actual events

### Lambda Data Generator Events
- Source: Randomly generated
- `source` field: `"lambda_data_generator"`
- `generated` field: `true` (marks as fake)
- Data quality: Realistic but synthetic
- Timing: Recent timestamps (last 5 minutes)

## Example Event Comparison

### Real Event (Lambda Producer)
```json
{
  "event_id": "evt_1705410600_1",
  "customer_id": "cust_12345",
  "tenant_id": "tenant_abc",
  "device_id": "device_xyz",
  "event_type": "file_access",
  "severity": "HIGH",
  "timestamp": "2024-01-16T10:30:00Z",
  "ingestion_timestamp": "2024-01-16T10:30:05.123456",
  "source": "lambda_producer",
  "version": "1.0"
}
```

### Generated Event (Lambda Data Generator)
```json
{
  "event_id": "evt_1705410600_1",
  "customer_id": "cust_00001",
  "tenant_id": "tenant_a",
  "device_id": "device_5432",
  "event_type": "malware_detection",
  "severity": "CRITICAL",
  "timestamp": "2024-01-16T10:28:45Z",
  "ingestion_timestamp": "2024-01-16T10:30:05.123456",
  "source": "lambda_data_generator",
  "version": "1.0",
  "generated": true
}
```

**Note:** Customer and tenant IDs are automatically generated based on NUM_CUSTOMERS and NUM_TENANTS.

## Performance Characteristics

### Lambda Producer
- **Execution Time**: 100-500ms per event batch
- **Memory**: 256-512 MB sufficient
- **Timeout**: 60 seconds recommended
- **Concurrency**: Scales with incoming events
- **Cost**: Based on actual event volume

### Lambda Data Generator
- **Execution Time**: 2-10 seconds (depends on NUM_EVENTS)
- **Memory**: 512 MB recommended
- **Timeout**: 300 seconds (5 minutes) for large batches
- **Concurrency**: Typically 1 (scheduled)
- **Cost**: Predictable (scheduled executions)

## Scaling Recommendations

### Lambda Producer (Production)
```bash
# High-volume production
NUM_EVENTS=N/A (event-driven)
TIMEOUT=60
MEMORY=512
CONCURRENCY=100+
```

### Lambda Data Generator (Testing)
```bash
# Light testing
NUM_EVENTS=10-100
TIMEOUT=60
MEMORY=256
SCHEDULE="rate(5 minutes)"

# Load testing
NUM_EVENTS=1000-10000
TIMEOUT=300
MEMORY=512
SCHEDULE="rate(1 minute)"
```

## Cost Estimation

### Lambda Producer (Production)
Assuming 1M events/day:
- Lambda invocations: ~10,000/day (100 events per batch)
- Duration: ~5 seconds average
- Cost: ~$0.20/day

### Lambda Data Generator (Testing)
Assuming 100 events every 5 minutes:
- Lambda invocations: 288/day
- Duration: ~3 seconds average
- Events generated: 28,800/day
- Cost: ~$0.01/day

## Migration Path

### Development → Production

1. **Start with Data Generator** for development:
   ```bash
   ./deploy_data_generator.sh
   # Test pipeline with generated data
   ```

2. **Validate pipeline** with generated events:
   - Verify Kafka ingestion
   - Validate PySpark processing
   - Check Iceberg table structure
   - Test analytics queries

3. **Deploy Producer** for production:
   ```bash
   ./deploy_lambda.sh
   # Connect real endpoint agents
   ```

4. **Run both** during transition:
   - Producer handles real events
   - Generator provides baseline load
   - Compare data quality and performance

5. **Disable Generator** when fully migrated:
   ```bash
   aws events disable-rule --name endpoint-logs-generator-schedule
   ```

## Monitoring

### Lambda Producer Metrics
- Invocation count (should match event volume)
- Error rate (should be < 1%)
- Duration (should be < 5s)
- Kafka publish failures

### Lambda Data Generator Metrics
- Invocation count (should match schedule)
- Events generated per execution
- Duration (should be consistent)
- Kafka publish success rate

## Troubleshooting

### Lambda Producer Issues
- **No events received**: Check API Gateway, EventBridge configuration
- **High error rate**: Validate event schema, check Kafka connectivity
- **Slow processing**: Increase memory, optimize batch size

### Lambda Data Generator Issues
- **Timeout errors**: Reduce NUM_EVENTS or increase timeout
- **Memory errors**: Increase memory allocation
- **Too many events**: Adjust schedule or NUM_EVENTS
- **Not generating**: Check EventBridge rule is enabled

## Best Practices

### Lambda Producer
1. Use dead-letter queue for failed events
2. Implement retry logic with exponential backoff
3. Monitor Kafka lag
4. Set up CloudWatch alarms
5. Use VPC endpoints for Kafka in VPC

### Lambda Data Generator
1. Use different customer/tenant IDs than production (automatically generated)
2. Start with small NUM_EVENTS (10-100) and NUM_CUSTOMERS (2-5)
3. Gradually increase for load testing
4. Mark generated events clearly (`generated: true`)
5. Disable when not needed to save costs
6. Use separate Kafka topic for testing if possible

## Summary

Both Lambda functions serve important but different purposes:

- **Lambda Producer**: Production-ready event processor for real security events
- **Lambda Data Generator**: Testing and demo tool for synthetic events

Use the Data Generator to validate your pipeline, then deploy the Producer for production workloads.
