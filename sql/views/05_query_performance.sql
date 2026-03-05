-- Phase 3: Query Performance View
CREATE OR REPLACE VIEW {CATALOG}.{SCHEMA}.v_query_performance AS
SELECT
  executed_by AS user_name,
  compute.warehouse_id,
  statement_type,
  COUNT(*) AS query_count,
  AVG(total_duration_ms / 1000.0) AS avg_duration_seconds,
  PERCENTILE(total_duration_ms / 1000.0, 0.95) AS p95_duration_seconds,
  MAX(total_duration_ms / 1000.0) AS max_duration_seconds,
  SUM(rows_produced) AS total_rows_produced
FROM system.query.history
WHERE start_time >= CURRENT_TIMESTAMP - INTERVAL 7 DAYS
  AND execution_status = 'FINISHED'
GROUP BY executed_by, compute.warehouse_id, statement_type
ORDER BY avg_duration_seconds DESC;
