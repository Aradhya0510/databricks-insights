-- Phase 3: Top Cost Drivers View
CREATE OR REPLACE VIEW observability.databricks_insights.v_top_cost_drivers AS
SELECT
  identity_metadata.run_as AS user_or_sp,
  billing_origin_product AS product,
  sku_name,
  COALESCE(usage_metadata.job_name, usage_metadata.notebook_path, 'N/A') AS workload_name,
  SUM(u.usage_quantity) AS total_dbus,
  SUM(u.usage_quantity * lp.pricing.effective_list.default) AS estimated_cost_usd
FROM system.billing.usage u
LEFT JOIN system.billing.list_prices lp
  ON lp.sku_name = u.sku_name
  AND u.usage_end_time >= lp.price_start_time
  AND (lp.price_end_time IS NULL OR u.usage_end_time < lp.price_end_time)
WHERE u.usage_date >= CURRENT_DATE - INTERVAL 30 DAYS
GROUP BY ALL
ORDER BY estimated_cost_usd DESC
LIMIT 50;
