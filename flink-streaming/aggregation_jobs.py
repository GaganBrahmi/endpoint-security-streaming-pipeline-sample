"""
Aggregation jobs module for PyFlink pipeline.

Uses TVF (Table-Valued Function) window syntax with PROCTIME() for windowing.
Processing time windows fire reliably without watermark dependencies.
Event time is preserved in the raw events table for historical analysis.
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
    """Create tumbling window aggregations for multiple time periods."""
    logger.info(f"Creating tumbling window aggregations for: {window_minutes} minutes")
    for minutes in window_minutes:
        table_name = f"threat_summary_tumbling_{minutes}min"
        create_aggregation_table(table_env, table_name, 'tumbling')
        _add_tumbling_window_job(statement_set, config, table_name, minutes)


def _add_tumbling_window_job(statement_set, config, table_name, window_minutes):
    """Add a tumbling window aggregation job using TVF + PROCTIME."""
    full_table_name = config.get_full_table_name(table_name)
    query = f"""
        INSERT INTO {full_table_name}
        SELECT
            customer_id,
            device_id,
            MAX(device_name) AS device_name,
            window_start,
            window_end,
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
        FROM TABLE(
            TUMBLE(
                TABLE default_catalog.default_database.kafka_source,
                DESCRIPTOR(proc_time),
                INTERVAL '{window_minutes}' MINUTE
            )
        )
        GROUP BY
            customer_id,
            device_id,
            window_start,
            window_end
    """
    statement_set.add_insert_sql(query)
    logger.info(f"Added tumbling window job: {window_minutes}min -> {table_name}")


def create_sliding_window_aggregation(
    table_env: TableEnvironment,
    statement_set,
    config: AggregationConfig,
    window_size_minutes: int,
    slide_interval_minutes: int
):
    """Create sliding window aggregation with overlapping windows."""
    logger.info(f"Creating sliding window: {window_size_minutes}min window, {slide_interval_minutes}min slide")
    table_name = f"threat_summary_sliding_{window_size_minutes}min"
    create_aggregation_table(table_env, table_name, 'sliding')
    _add_sliding_window_job(statement_set, config, table_name, window_size_minutes, slide_interval_minutes)


def _add_sliding_window_job(statement_set, config, table_name, window_size_minutes, slide_interval_minutes):
    """Add a sliding window aggregation job using TVF HOP + PROCTIME."""
    full_table_name = config.get_full_table_name(table_name)
    query = f"""
        INSERT INTO {full_table_name}
        SELECT
            customer_id,
            device_id,
            MAX(device_name) AS device_name,
            window_start,
            window_end,
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
        FROM TABLE(
            HOP(
                TABLE default_catalog.default_database.kafka_source,
                DESCRIPTOR(proc_time),
                INTERVAL '{slide_interval_minutes}' MINUTE,
                INTERVAL '{window_size_minutes}' MINUTE
            )
        )
        GROUP BY
            customer_id,
            device_id,
            window_start,
            window_end
    """
    statement_set.add_insert_sql(query)
    logger.info(f"Added sliding window job: {window_size_minutes}min/{slide_interval_minutes}min -> {table_name}")


def create_session_window_aggregation(
    table_env: TableEnvironment,
    statement_set,
    config: AggregationConfig,
    session_gap_minutes: int
):
    """Create cumulate window aggregation (replaces session windows)."""
    logger.info(f"Creating cumulate window: {session_gap_minutes}min step")
    table_name = f"threat_sessions_{session_gap_minutes}min_gap"
    create_aggregation_table(table_env, table_name, 'session')
    _add_cumulate_window_job(statement_set, config, table_name, session_gap_minutes)


def _add_cumulate_window_job(statement_set, config, table_name, session_gap_minutes):
    """Add a cumulate window aggregation job using TVF + PROCTIME."""
    full_table_name = config.get_full_table_name(table_name)
    max_window_minutes = session_gap_minutes * 12
    query = f"""
        INSERT INTO {full_table_name}
        SELECT
            customer_id,
            device_id,
            MAX(device_name) AS device_name,
            window_start,
            window_end,
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
            TIMESTAMPDIFF(SECOND, window_start, window_end) AS session_duration_seconds,
            CASE
                WHEN SUM(CASE WHEN threat_detected = TRUE THEN 1 ELSE 0 END) >= 3
                THEN TRUE ELSE FALSE
            END AS event_burst_detected
        FROM TABLE(
            CUMULATE(
                TABLE default_catalog.default_database.kafka_source,
                DESCRIPTOR(proc_time),
                INTERVAL '{session_gap_minutes}' MINUTE,
                INTERVAL '{max_window_minutes}' MINUTE
            )
        )
        GROUP BY
            customer_id,
            device_id,
            window_start,
            window_end
    """
    statement_set.add_insert_sql(query)
    logger.info(f"Added cumulate window job: {session_gap_minutes}min step -> {table_name}")
