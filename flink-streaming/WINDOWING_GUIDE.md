# PyFlink Windowing Strategies Guide

This guide explains the three windowing strategies implemented in the PyFlink consumer and when to use each one for endpoint security analytics.

## Overview

The PyFlink consumer demonstrates three powerful windowing approaches:

1. **Tumbling Windows** - Fixed, non-overlapping time periods
2. **Sliding Windows (HOP)** - Overlapping windows with frequent updates
3. **Session Windows** - Activity-based windows that adapt to event patterns

Each strategy serves different security analytics use cases and can be enabled/disabled independently.

---

## 1. Tumbling Windows

### Description
Fixed, non-overlapping time periods that divide the event stream into discrete chunks.

```
Events:  |----5min----|----5min----|----5min----|
Windows: [  Window 1  ][  Window 2  ][  Window 3  ]
```

### Configuration
```python
# Multiple window sizes supported
window_minutes = [5, 15, 30, 60]
```

### Output Tables
- `threat_summary_tumbling_5min`
- `threat_summary_tumbling_15min`
- `threat_summary_tumbling_30min`
- `threat_summary_tumbling_60min`

### Use Cases

#### 1. Compliance Reporting
**Scenario:** Generate hourly security reports for SOC2, HIPAA, or PCI-DSS compliance.

**Example:**
```sql
-- Get hourly threat summary for compliance report
SELECT 
    customer_id,
    device_id,
    window_start,
    threat_count,
    critical_severity_count,
    blocked_count
FROM glue_catalog.data_generator.threat_summary_tumbling_60min
WHERE customer_id = 'cust_00001'
  AND window_start >= CURRENT_DATE
ORDER BY window_start;
```

#### 2. SLA Monitoring
**Scenario:** Ensure devices don't exceed 100 threats per hour (SLA requirement).

**Example:**
```sql
-- Check SLA violations
SELECT 
    device_id,
    device_name,
    window_start,
    threat_count,
    CASE 
        WHEN threat_count > 100 THEN 'SLA VIOLATED'
        ELSE 'OK'
    END AS sla_status
FROM glue_catalog.data_generator.threat_summary_tumbling_60min
WHERE customer_id = 'cust_00001'
  AND threat_count > 100
ORDER BY threat_count DESC;
```

#### 3. Historical Trend Analysis
**Scenario:** Analyze threat patterns over the past week.

**Example:**
```sql
-- Weekly threat trend (5-minute granularity)
SELECT 
    DATE(window_start) AS date,
    HOUR(window_start) AS hour,
    AVG(threat_count) AS avg_threats_per_5min,
    MAX(threat_count) AS peak_threats_per_5min
FROM glue_catalog.data_generator.threat_summary_tumbling_5min
WHERE customer_id = 'cust_00001'
  AND window_start >= CURRENT_DATE - INTERVAL '7' DAY
GROUP BY DATE(window_start), HOUR(window_start)
ORDER BY date, hour;
```

### Advantages
- ✅ Clean, non-overlapping periods (perfect for reports)
- ✅ Each event counted exactly once
- ✅ Easy to aggregate: sum all 5-min windows = hourly total
- ✅ Predictable storage: one record per window per device
- ✅ Low computational cost

### Disadvantages
- ❌ May miss threats that span window boundaries
- ❌ Updates only at window end (not continuous)
- ❌ Sudden drops at window edges in visualizations

---

## 2. Sliding Windows (HOP)

### Description
Overlapping windows that update more frequently, providing continuous monitoring.

```
Events:  |----5min----|----5min----|----5min----|
Windows: [  Window 1  ]
          [  Window 2  ]
           [  Window 3  ]
            [  Window 4  ]
```

### Configuration
```python
window_size_minutes = 5      # Look back 5 minutes
slide_interval_minutes = 1   # Update every 1 minute
```

### Output Tables
- `threat_summary_sliding_5min`

### Use Cases

#### 1. Real-Time Brute Force Detection
**Scenario:** Alert SOC when any device has > 10 failed logins in the last 5 minutes.

**Example:**
```sql
-- Real-time brute force alert
SELECT 
    customer_id,
    device_id,
    device_name,
    window_start,
    window_end,
    failed_login_count,
    threat_percentage
FROM glue_catalog.data_generator.threat_summary_sliding_5min
WHERE failed_login_count > 10
  AND window_end >= CURRENT_TIMESTAMP - INTERVAL '2' MINUTE
ORDER BY failed_login_count DESC;
```

#### 2. SOC Dashboard (Live Updating)
**Scenario:** Display continuously updating threat metrics on SOC dashboard.

**Example:**
```sql
-- Latest 5-minute threat metrics (updates every minute)
SELECT 
    device_id,
    device_name,
    window_end,
    threat_count,
    critical_severity_count,
    malware_count,
    data_exfiltration_count
FROM glue_catalog.data_generator.threat_summary_sliding_5min
WHERE customer_id = 'cust_00001'
  AND window_end >= CURRENT_TIMESTAMP - INTERVAL '10' MINUTE
ORDER BY window_end DESC, threat_count DESC
LIMIT 20;
```

#### 3. Gradual Attack Detection
**Scenario:** Detect slowly escalating attacks that wouldn't trigger in fixed windows.

**Example:**
```sql
-- Detect gradual increase in threats
WITH threat_trend AS (
    SELECT 
        device_id,
        window_end,
        threat_count,
        LAG(threat_count, 1) OVER (PARTITION BY device_id ORDER BY window_end) AS prev_threat_count
    FROM glue_catalog.data_generator.threat_summary_sliding_5min
    WHERE customer_id = 'cust_00001'
      AND window_end >= CURRENT_TIMESTAMP - INTERVAL '30' MINUTE
)
SELECT 
    device_id,
    window_end,
    threat_count,
    prev_threat_count,
    threat_count - prev_threat_count AS threat_increase
FROM threat_trend
WHERE threat_count > prev_threat_count * 1.5  -- 50% increase
ORDER BY threat_increase DESC;
```

### Advantages
- ✅ Continuous updates (every minute)
- ✅ Catches threats that span window boundaries
- ✅ Smoother graphs (no sudden drops)
- ✅ Earlier detection of gradual attacks
- ✅ Better for real-time alerting

### Disadvantages
- ❌ More storage (overlapping windows)
- ❌ Higher compute cost
- ❌ Same event counted in multiple windows
- ❌ More complex to aggregate

---

## 3. Session Windows

### Description
Variable-length windows that group events based on activity patterns. A session ends after a period of inactivity.

```
Events:  |xxx|x|xx|-----gap-----|x|xx|-----gap-----|xxx|x|
Windows: [Session 1 ]            [Session 2]        [Session 3]
```

### Configuration
```python
session_gap_minutes = 5  # End session after 5 minutes of inactivity
```

### Output Tables
- `threat_sessions_5min_gap`

### Use Cases

#### 1. Attack Campaign Detection
**Scenario:** Identify coordinated multi-stage attacks (reconnaissance → exploitation → exfiltration).

**Example:**
```sql
-- Detect coordinated attack campaigns
SELECT 
    customer_id,
    device_id,
    device_name,
    window_start AS campaign_start,
    window_end AS campaign_end,
    session_duration_seconds,
    threat_count,
    unique_threat_types,
    malware_count,
    data_exfiltration_count,
    event_burst_detected
FROM glue_catalog.data_generator.threat_sessions_5min_gap
WHERE threat_count >= 3  -- Multiple threats in one session
  AND session_duration_seconds <= 300  -- Within 5 minutes
  AND event_burst_detected = TRUE
ORDER BY threat_count DESC, session_duration_seconds ASC;
```

#### 2. Insider Threat Detection
**Scenario:** Detect unusual user activity sessions (burst of suspicious actions).

**Example:**
```sql
-- Detect suspicious user sessions
SELECT 
    customer_id,
    device_id,
    device_name,
    window_start AS session_start,
    window_end AS session_end,
    session_duration_seconds,
    total_events,
    threat_count,
    data_exfiltration_count,
    ROUND(threat_count * 100.0 / total_events, 2) AS threat_ratio
FROM glue_catalog.data_generator.threat_sessions_5min_gap
WHERE data_exfiltration_count > 0
  AND session_duration_seconds < 180  -- Quick session (< 3 minutes)
  AND threat_ratio > 50  -- More than 50% threats
ORDER BY threat_ratio DESC;
```

#### 3. Incident Correlation
**Scenario:** Group related security events into single incidents for investigation.

**Example:**
```sql
-- Generate incident reports from sessions
SELECT 
    customer_id,
    device_id,
    device_name,
    window_start AS incident_start,
    window_end AS incident_end,
    session_duration_seconds AS incident_duration,
    threat_count AS threats_in_incident,
    critical_severity_count,
    high_severity_count,
    unique_threat_types,
    CASE 
        WHEN threat_count >= 5 THEN 'CRITICAL'
        WHEN threat_count >= 3 THEN 'HIGH'
        ELSE 'MEDIUM'
    END AS incident_severity
FROM glue_catalog.data_generator.threat_sessions_5min_gap
WHERE threat_count > 0
  AND window_start >= CURRENT_TIMESTAMP - INTERVAL '24' HOUR
ORDER BY threat_count DESC;
```

### Advantages
- ✅ Groups related events automatically
- ✅ Variable window size (adapts to activity)
- ✅ Detects attack patterns/campaigns
- ✅ Identifies "burst" behavior vs steady threats
- ✅ Perfect for incident correlation

### Disadvantages
- ❌ Unpredictable window sizes
- ❌ Harder to aggregate across sessions
- ❌ Requires tuning gap duration
- ❌ May create very long sessions during sustained attacks

---

## Comparison Matrix

| Aspect | Tumbling | Sliding (HOP) | Session |
|--------|----------|---------------|---------|
| **Window Size** | Fixed | Fixed | Variable |
| **Overlap** | No | Yes | No |
| **Update Frequency** | End of window | Every slide | End of session |
| **Storage Cost** | Low | High | Medium |
| **Compute Cost** | Low | High | Medium |
| **Event Counting** | Once | Multiple | Once |
| **Best For** | Reports, SLAs | Real-time alerts | Attack campaigns |
| **Latency** | Medium | Low | Variable |
| **Predictability** | High | High | Low |

---

## Configuration

### Enable/Disable Features

Set environment variables to control which aggregations run:

```bash
# Enable/disable individual features
export ENABLE_RAW_EVENTS=true           # Raw event ingestion
export ENABLE_TUMBLING_WINDOWS=true     # Tumbling window aggregations
export ENABLE_SLIDING_WINDOWS=true      # Sliding window aggregations
export ENABLE_SESSION_WINDOWS=true      # Session window aggregations
```

### Customize Window Parameters

Edit `flink_consumer_with_aggregations.py`:

```python
# Tumbling windows - add/remove window sizes
create_tumbling_window_aggregations(
    table_env,
    config,
    window_minutes=[5, 15, 30, 60]  # Customize here
)

# Sliding window - adjust window size and slide interval
create_sliding_window_aggregation(
    table_env,
    config,
    window_size_minutes=5,      # Customize here
    slide_interval_minutes=1    # Customize here
)

# Session window - adjust inactivity gap
create_session_window_aggregation(
    table_env,
    config,
    session_gap_minutes=5  # Customize here
)
```

---

## Performance Considerations

### Storage Requirements

**Tumbling Windows:**
- 5-min: ~288 records/device/day
- 15-min: ~96 records/device/day
- 30-min: ~48 records/device/day
- 60-min: ~24 records/device/day

**Sliding Windows (5-min window, 1-min slide):**
- ~1,440 records/device/day (5x more than 5-min tumbling)

**Session Windows:**
- Variable (depends on activity patterns)
- Typically 10-50 records/device/day

### Compute Requirements

**Relative CPU/Memory Usage:**
- Tumbling: 1x (baseline)
- Sliding: 3-5x (due to overlapping windows)
- Session: 1.5-2x (state management overhead)

### Recommendations

**For Cost Optimization:**
1. Start with tumbling windows only
2. Add sliding windows for critical alerts
3. Add session windows for advanced threat detection

**For Performance:**
1. Use tumbling windows for historical analysis
2. Use sliding windows sparingly (only for real-time alerts)
3. Tune session gap based on observed attack patterns

---

## Query Examples

### Cross-Window Analysis

Compare threat trends across different window sizes:

```sql
-- Compare 5-min vs 60-min threat trends
SELECT 
    '5-min' AS window_type,
    AVG(threat_count) AS avg_threats,
    MAX(threat_count) AS max_threats
FROM glue_catalog.data_generator.threat_summary_tumbling_5min
WHERE customer_id = 'cust_00001'
  AND window_start >= CURRENT_TIMESTAMP - INTERVAL '24' HOUR

UNION ALL

SELECT 
    '60-min' AS window_type,
    AVG(threat_count) AS avg_threats,
    MAX(threat_count) AS max_threats
FROM glue_catalog.data_generator.threat_summary_tumbling_60min
WHERE customer_id = 'cust_00001'
  AND window_start >= CURRENT_TIMESTAMP - INTERVAL '24' HOUR;
```

### Alert Rules

Combine multiple window types for comprehensive alerting:

```sql
-- Multi-window threat detection
WITH recent_threats AS (
    -- Sliding window: immediate threats
    SELECT device_id, 'IMMEDIATE' AS alert_type, threat_count
    FROM glue_catalog.data_generator.threat_summary_sliding_5min
    WHERE threat_count > 10
      AND window_end >= CURRENT_TIMESTAMP - INTERVAL '2' MINUTE
    
    UNION ALL
    
    -- Session window: attack campaigns
    SELECT device_id, 'CAMPAIGN' AS alert_type, threat_count
    FROM glue_catalog.data_generator.threat_sessions_5min_gap
    WHERE event_burst_detected = TRUE
      AND window_end >= CURRENT_TIMESTAMP - INTERVAL '10' MINUTE
)
SELECT 
    device_id,
    alert_type,
    SUM(threat_count) AS total_threats
FROM recent_threats
GROUP BY device_id, alert_type
ORDER BY total_threats DESC;
```

---

## Troubleshooting

### High Memory Usage

**Symptom:** Flink job runs out of memory

**Solutions:**
1. Reduce number of tumbling window sizes
2. Disable sliding windows (most memory-intensive)
3. Increase session gap (reduces state size)
4. Increase parallelism and KPUs

### Delayed Aggregations

**Symptom:** Aggregations appear late

**Solutions:**
1. Check watermark configuration (currently 5 seconds)
2. Reduce checkpoint interval
3. Increase parallelism

### Missing Data

**Symptom:** Some windows have no data

**Solutions:**
1. Check Kafka has continuous data flow
2. Verify watermarks are progressing
3. Check for late events (beyond watermark)

---

## Next Steps

1. **Deploy the consumer** with all three windowing strategies
2. **Monitor CloudWatch metrics** for each aggregation type
3. **Query aggregated tables** to validate data
4. **Build dashboards** using the aggregated metrics
5. **Tune window parameters** based on your threat patterns
6. **Create alerts** using the aggregation tables

For deployment instructions, see the main [README.md](README.md).
