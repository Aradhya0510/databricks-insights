-- Phase 3: Security Anomaly View
CREATE OR REPLACE VIEW observability.databricks_insights.v_security_anomalies AS

-- IP Access Denials
SELECT
  'IP_ACCESS_DENIED' AS anomaly_type,
  event_time,
  workspace_id,
  user_identity.email AS user_email,
  source_ip_address,
  action_name AS detail
FROM system.access.audit
WHERE action_name IN ('IpAccessDenied', 'accountIpAclsValidationFailed')
  AND event_date >= CURRENT_DATE - INTERVAL 1 DAY

UNION ALL

-- High-volume destructive operations (>10 deletes by one user in 1 hour)
SELECT
  'HIGH_DESTRUCTIVE_OPS' AS anomaly_type,
  MAX(event_time) AS event_time,
  workspace_id,
  user_identity.email AS user_email,
  '' AS source_ip_address,
  CONCAT(COUNT(*), ' delete operations in 1 hour') AS detail
FROM system.access.audit
WHERE action_name LIKE 'delete%'
  AND event_date >= CURRENT_DATE - INTERVAL 1 DAY
  AND user_identity.email NOT LIKE '%System-User%'
GROUP BY
  workspace_id,
  user_identity.email,
  date_trunc('HOUR', event_time)
HAVING COUNT(*) > 10

UNION ALL

-- Admin group changes
SELECT
  'ADMIN_GROUP_CHANGE' AS anomaly_type,
  event_time,
  workspace_id,
  user_identity.email AS user_email,
  '' AS source_ip_address,
  CONCAT(action_name, ': ', request_params.targetUserName) AS detail
FROM system.access.audit
WHERE action_name IN ('addPrincipalToGroup', 'removePrincipalFromGroup')
  AND request_params.targetGroupName = 'admins'
  AND event_date >= CURRENT_DATE - INTERVAL 1 DAY

ORDER BY event_time DESC;
