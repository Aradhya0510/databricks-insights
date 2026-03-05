-- Phase 3: Real-Time Cost Burn Rate View
CREATE OR REPLACE VIEW observability.databricks_insights.v_realtime_burn_rate AS
WITH hourly AS (
  SELECT
    date_trunc('HOUR', usage_start_time) AS hour,
    SUM(u.usage_quantity * lp.pricing.effective_list.default) AS cost_usd
  FROM system.billing.usage u
  JOIN system.billing.list_prices lp
    ON lp.sku_name = u.sku_name
    AND u.usage_end_time >= lp.price_start_time
    AND (lp.price_end_time IS NULL OR u.usage_end_time < lp.price_end_time)
  WHERE u.usage_date >= CURRENT_DATE - INTERVAL 7 DAYS
  GROUP BY 1
)
SELECT
  hour,
  cost_usd,
  AVG(cost_usd) OVER (ORDER BY hour ROWS BETWEEN 23 PRECEDING AND CURRENT ROW) AS rolling_24h_avg,
  SUM(cost_usd) OVER (
    PARTITION BY DATE(hour)
    ORDER BY hour
  ) AS cumulative_daily_cost
FROM hourly
ORDER BY hour DESC;
