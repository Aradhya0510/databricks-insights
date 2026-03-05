-- Phase 3: Query Performance View
CREATE OR REPLACE VIEW {CATALOG}.{SCHEMA}.v_query_performance AS
SELECT
  user_name,
  warehouse_id,
  statement_type,
  COUNT(*) AS query_count,
  AVG(duration / 1000) AS avg_duration_seconds,
  PERCENTILE(duration / 1000, 0.95) AS p95_duration_seconds,
  MAX(duration / 1000) AS max_duration_seconds,
  SUM(rows_produced) AS total_rows_produced
FROM system.query.history
WHERE start_time >= CURRENT_TIMESTAMP - INTERVAL 7 DAYS
  AND status = 'FINISHED'
GROUP BY user_name, warehouse_id, statement_type
ORDER BY avg_duration_seconds DESC;
