"""
Aggregation jobs module for PyFlink pipeline.

This module contains functions to create and start different types of
windowing aggregations: tumbling, sliding, and session windows.
"""

import logging
from typing import List
from pyflink.table import TableEnvironment
from aggregation_config import AggregationConfig
from table_creators import create_aggregation_table

logger = logging.getLogger(__name__)


def create_tumbling_window_aggregations(
    table_env: TableEnvironment,
    statement_set,
    config: AggregationConfig,
    window_minutes: List[int]
):
    """
    Create tumbling window aggregations for multiple time periods.
    
    Tumbling windows are fixed, non-overlapping time periods.
    Use case: Compliance reporting, SLA monitoring, historical analysis.
    
    Args:
        table_env: PyFlink TableEnvironment
        statement_set: PyFlink StatementSet for concurrent execution
        config: Aggregation configuration
        window_minutes: List of window sizes in minutes (e.g., [5, 15, 30, 60])
    """
    logger.info(f"Creating tumbling window aggregations for: {window_minutes} minutes")
    
    for minutes in window_minutes:
        table_name = f"threat_summary_tumbling_{minutes}min"
        
        # Create table
        create_aggregation_table(table_env, table_name, 'tumbling')
        
        # Add aggregation job to statement set
        _add_tumbling_window_job(statement_set, config, table_name, minutes)


def _add_tumbling_window_job(
    statement_set,
    config: AggregationConfig,
    table_name: str,
    window_minutes: int
):
    """
    Add a tumbling window aggregation job to the statement set.
    
    Args:
        statement_set: PyFlink StatementSet
        config: Aggregation configuration
        table_name: Target table name
        window_minutes: Window size in minutes
    """
    full_table_name = config.get_full_table_name(table_name)
    
    query = f"""
        INSERT INTO {full_table_name}
        SELECT
            customer_id,
            device_id,
            MAX(device_name) AS device_name,
            TUMBLE_START(event_time, INTERVAL '{window_minutes}' MINUTE) AS window_start,
            TUMBLE_END(event_time, INTERVAL '{window_minutes}' MINUTE) AS window_end,
            COUNT(*) AS total_events,
            SUM(CASE WHEN threat_detected = TRUE THEN 1 ELSE 0 END) AS threat_count,
            CAST(SUM(CASE WHEN threat_detected = TRUE THEN 1 ELSE 0 END) AS DOUBLE) / 
                NULLIF(COUNT(*), 0) * 100 AS threat_percentage,
            SUM(CASE WHEN severity = 'HIGH' THEN 1 ELSE 0 END) AS high_severity_count,
            SUM(CASE WHEN severity = 'CRITICAL' THEN 1 ELSE 0 END) AS critical_severity_count,
            SUM(CASE WHEN severity = 'MEDIUM' THEN 1 ELSE 0 END) AS medium_severity_count,
            SUM(CASE WHEN severity = 'LOW' THEN 1 ELSE 0 END) AS low_severity_count,
            COUNT(DISTINCT threat_type) AS unique_threat_types,
            SUM(CASE WHEN `result` = 'blocked' THEN 1 ELSE 0 END) AS blocked_count,
            SUM(CASE WHEN `result` = 'allowed' THEN 1 ELSE 0 END) AS allowed_count,
            SUM(CASE WHEN event_type = 'failed_login' THEN 1 ELSE 0 END) AS failed_login_count,
            SUM(CASE WHEN threat_type = 'malware' THEN 1 ELSE 0 END) AS malware_count,
            SUM(CASE WHEN threat_type = 'data_exfiltration' THEN 1 ELSE 0 END) AS data_exfiltration_count,
            {window_minutes} AS window_duration_minutes
        FROM default_catalog.default_database.kafka_source
        GROUP BY
            customer_id,
            device_id,
            TUMBLE(event_time, INTERVAL '{window_minutes}' MINUTE)
    """
    
    statement_set.add_insert_sql(query)
    logger.info(f"✅ Added tumbling window job: {window_minutes}-minute window → {table_name}")


def create_sliding_window_aggregation(
    table_env: TableEnvironment,
    statement_set,
    config: AggregationConfig,
    window_size_minutes: int,
    slide_interval_minutes: int
):
    """
    Create sliding window aggregation with overlapping windows.
    
    Sliding windows (HOP) update more frequently than tumbling windows.
    Use case: Real-time alerting, SOC dashboards, continuous monitoring.
    
    Args:
        table_env: PyFlink TableEnvironment
        statement_set: PyFlink StatementSet for concurrent execution
        config: Aggregation configuration
        window_size_minutes: Size of the window (e.g., 5 minutes)
        slide_interval_minutes: How often to slide (e.g., 1 minute)
    """
    logger.info(f"Creating sliding window aggregation: {window_size_minutes}min window, "
                f"{slide_interval_minutes}min slide")
    
    table_name = f"threat_summary_sliding_{window_size_minutes}min"
    
    # Create table
    create_aggregation_table(table_env, table_name, 'sliding')
    
    # Add aggregation job to statement set
    _add_sliding_window_job(
        statement_set,
        config,
        table_name,
        window_size_minutes,
        slide_interval_minutes
    )


def _add_sliding_window_job(
    statement_set,
    config: AggregationConfig,
    table_name: str,
    window_size_minutes: int,
    slide_interval_minutes: int
):
    """
    Add a sliding window aggregation job to the statement set.
    
    Args:
        statement_set: PyFlink StatementSet
        config: Aggregation configuration
        table_name: Target table name
        window_size_minutes: Window size in minutes
        slide_interval_minutes: Slide interval in minutes
    """
    full_table_name = config.get_full_table_name(table_name)
    
    query = f"""
        INSERT INTO {full_table_name}
        SELECT
            customer_id,
            device_id,
            MAX(device_name) AS device_name,
            HOP_START(event_time, INTERVAL '{slide_interval_minutes}' MINUTE, 
                      INTERVAL '{window_size_minutes}' MINUTE) AS window_start,
            HOP_END(event_time, INTERVAL '{slide_interval_minutes}' MINUTE, 
                    INTERVAL '{window_size_minutes}' MINUTE) AS window_end,
            COUNT(*) AS total_events,
            SUM(CASE WHEN threat_detected = TRUE THEN 1 ELSE 0 END) AS threat_count,
            CAST(SUM(CASE WHEN threat_detected = TRUE THEN 1 ELSE 0 END) AS DOUBLE) / 
                NULLIF(COUNT(*), 0) * 100 AS threat_percentage,
            SUM(CASE WHEN severity = 'HIGH' THEN 1 ELSE 0 END) AS high_severity_count,
            SUM(CASE WHEN severity = 'CRITICAL' THEN 1 ELSE 0 END) AS critical_severity_count,
            SUM(CASE WHEN severity = 'MEDIUM' THEN 1 ELSE 0 END) AS medium_severity_count,
            SUM(CASE WHEN severity = 'LOW' THEN 1 ELSE 0 END) AS low_severity_count,
            COUNT(DISTINCT threat_type) AS unique_threat_types,
            SUM(CASE WHEN `result` = 'blocked' THEN 1 ELSE 0 END) AS blocked_count,
            SUM(CASE WHEN `result` = 'allowed' THEN 1 ELSE 0 END) AS allowed_count,
            SUM(CASE WHEN event_type = 'failed_login' THEN 1 ELSE 0 END) AS failed_login_count,
            SUM(CASE WHEN threat_type = 'malware' THEN 1 ELSE 0 END) AS malware_count,
            SUM(CASE WHEN threat_type = 'data_exfiltration' THEN 1 ELSE 0 END) AS data_exfiltration_count,
            {window_size_minutes} AS window_duration_minutes,
            {slide_interval_minutes} AS slide_interval_minutes
        FROM default_catalog.default_database.kafka_source
        GROUP BY
            customer_id,
            device_id,
            HOP(event_time, INTERVAL '{slide_interval_minutes}' MINUTE, 
                INTERVAL '{window_size_minutes}' MINUTE)
    """
    
    statement_set.add_insert_sql(query)
    logger.info(f"✅ Added sliding window job: {window_size_minutes}min window, "
                f"{slide_interval_minutes}min slide → {table_name}")


def create_session_window_aggregation(
    table_env: TableEnvironment,
    statement_set,
    config: AggregationConfig,
    session_gap_minutes: int
):
    """
    Create session window aggregation for detecting activity bursts.
    
    Session windows group events that occur close together in time.
    Use case: Attack campaign detection, user behavior analysis, incident correlation.
    
    Args:
        table_env: PyFlink TableEnvironment
        statement_set: PyFlink StatementSet for concurrent execution
        config: Aggregation configuration
        session_gap_minutes: Inactivity gap to end session (e.g., 5 minutes)
    """
    logger.info(f"Creating session window aggregation: {session_gap_minutes}min gap")
    
    table_name = f"threat_sessions_{session_gap_minutes}min_gap"
    
    # Create table
    create_aggregation_table(table_env, table_name, 'session')
    
    # Add aggregation job to statement set
    _add_session_window_job(statement_set, config, table_name, session_gap_minutes)


def _add_session_window_job(
    statement_set,
    config: AggregationConfig,
    table_name: str,
    session_gap_minutes: int
):
    """
    Add a session window aggregation job to the statement set.
    
    Args:
        statement_set: PyFlink StatementSet
        config: Aggregation configuration
        table_name: Target table name
        session_gap_minutes: Session gap in minutes
    """
    full_table_name = config.get_full_table_name(table_name)
    
    query = f"""
        INSERT INTO {full_table_name}
        SELECT
            customer_id,
            device_id,
            MAX(device_name) AS device_name,
            SESSION_START(event_time, INTERVAL '{session_gap_minutes}' MINUTE) AS window_start,
            SESSION_END(event_time, INTERVAL '{session_gap_minutes}' MINUTE) AS window_end,
            COUNT(*) AS total_events,
            SUM(CASE WHEN threat_detected = TRUE THEN 1 ELSE 0 END) AS threat_count,
            CAST(SUM(CASE WHEN threat_detected = TRUE THEN 1 ELSE 0 END) AS DOUBLE) / 
                NULLIF(COUNT(*), 0) * 100 AS threat_percentage,
            SUM(CASE WHEN severity = 'HIGH' THEN 1 ELSE 0 END) AS high_severity_count,
            SUM(CASE WHEN severity = 'CRITICAL' THEN 1 ELSE 0 END) AS critical_severity_count,
            SUM(CASE WHEN severity = 'MEDIUM' THEN 1 ELSE 0 END) AS medium_severity_count,
            SUM(CASE WHEN severity = 'LOW' THEN 1 ELSE 0 END) AS low_severity_count,
            COUNT(DISTINCT threat_type) AS unique_threat_types,
            SUM(CASE WHEN `result` = 'blocked' THEN 1 ELSE 0 END) AS blocked_count,
            SUM(CASE WHEN `result` = 'allowed' THEN 1 ELSE 0 END) AS allowed_count,
            SUM(CASE WHEN event_type = 'failed_login' THEN 1 ELSE 0 END) AS failed_login_count,
            SUM(CASE WHEN threat_type = 'malware' THEN 1 ELSE 0 END) AS malware_count,
            SUM(CASE WHEN threat_type = 'data_exfiltration' THEN 1 ELSE 0 END) AS data_exfiltration_count,
            TIMESTAMPDIFF(SECOND, 
                SESSION_START(event_time, INTERVAL '{session_gap_minutes}' MINUTE),
                SESSION_END(event_time, INTERVAL '{session_gap_minutes}' MINUTE)
            ) AS session_duration_seconds,
            CASE 
                WHEN SUM(CASE WHEN threat_detected = TRUE THEN 1 ELSE 0 END) >= 3 
                THEN TRUE 
                ELSE FALSE 
            END AS event_burst_detected
        FROM default_catalog.default_database.kafka_source
        GROUP BY
            customer_id,
            device_id,
            SESSION(event_time, INTERVAL '{session_gap_minutes}' MINUTE)
    """
    
    statement_set.add_insert_sql(query)
    logger.info(f"✅ Added session window job: {session_gap_minutes}min gap → {table_name}")
