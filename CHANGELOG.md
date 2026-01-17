# Endpoint Security Streaming Pipeline - Changelog

## Version 2.0 - Auto-Generated IDs (Latest)

### Changes
- **Simplified Configuration**: Customer and tenant IDs are now automatically generated
- **New Environment Variables**:
  - `NUM_CUSTOMERS`: Number of unique customers to generate (default: 5)
  - `NUM_TENANTS`: Number of unique tenants to generate (default: 3)
- **Removed Environment Variables**:
  - `CUSTOMER_IDS`: No longer needed (auto-generated)
  - `TENANT_IDS`: No longer needed (auto-generated)

### ID Generation Format
- **Customer IDs**: `cust_00001`, `cust_00002`, ..., `cust_NNNNN`
  - Zero-padded 5-digit format
  - Example with NUM_CUSTOMERS=5: `cust_00001` through `cust_00005`
  
- **Tenant IDs**: `tenant_a`, `tenant_b`, `tenant_c`, ...
  - Alphabetic suffix (a-z)
  - Example with NUM_TENANTS=3: `tenant_a`, `tenant_b`, `tenant_c`
  - Supports up to 26 tenants (tenant_a through tenant_z)

### Benefits
1. **Simpler Deployment**: No need to manually specify customer/tenant IDs
2. **Consistent Format**: Predictable ID patterns for testing
3. **Scalable**: Easy to increase/decrease number of customers and tenants
4. **Less Configuration**: Fewer environment variables to manage

### Migration from Version 1.0

**Old Configuration (v1.0):**
```bash
CUSTOMER_IDS=cust_001,cust_002,cust_003
TENANT_IDS=tenant_a,tenant_b,tenant_c
```

**New Configuration (v2.0):**
```bash
NUM_CUSTOMERS=3
NUM_TENANTS=3
```

### Examples

#### Small Test Environment
```bash
NUM_EVENTS=10
NUM_CUSTOMERS=2
NUM_TENANTS=2
```
Generates: `cust_00001`, `cust_00002` and `tenant_a`, `tenant_b`

#### Medium Test Environment
```bash
NUM_EVENTS=100
NUM_CUSTOMERS=5
NUM_TENANTS=3
```
Generates: `cust_00001` through `cust_00005` and `tenant_a` through `tenant_c`

#### Large Load Test Environment
```bash
NUM_EVENTS=1000
NUM_CUSTOMERS=20
NUM_TENANTS=10
```
Generates: `cust_00001` through `cust_00020` and `tenant_a` through `tenant_j`

### Deployment

**Update existing Lambda function:**
```bash
aws lambda update-function-configuration \
  --function-name endpoint-logs-data-generator \
  --environment Variables="{KAFKA_BOOTSTRAP_SERVERS=your-broker:9092,KAFKA_TOPIC=endpoint_logs,NUM_EVENTS=100,NUM_CUSTOMERS=5,NUM_TENANTS=3}"
```

**Deploy new Lambda function:**
```bash
./deploy_data_generator.sh
```

---

## Version 1.0 - Initial Release

### Features
- Lambda function for generating fake endpoint security events
- Kafka integration for event publishing
- Configurable event generation via environment variables
- EventBridge scheduling support
- Manual customer and tenant ID specification

### Environment Variables (v1.0)
- `KAFKA_BOOTSTRAP_SERVERS`: Kafka broker addresses
- `KAFKA_TOPIC`: Kafka topic name
- `NUM_EVENTS`: Number of events per execution
- `CUSTOMER_IDS`: Comma-separated customer IDs
- `TENANT_IDS`: Comma-separated tenant IDs
