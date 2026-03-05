-- Databricks Insights: Complete Setup SQL
-- This file contains all SQL setup needed for the platform
-- Run this as a metastore admin or workspace admin

-- ============================================================================
-- 1. CREATE CATALOG AND SCHEMA
-- ============================================================================

-- Create a dedicated catalog (or use an existing one)
-- Placeholder {CATALOG} will be replaced with actual catalog name from config
CREATE CATALOG IF NOT EXISTS {CATALOG};

-- Create the schema
-- Placeholders {CATALOG} and {SCHEMA} will be replaced with actual values from config
CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}
  COMMENT 'Databricks Insights workspace observability platform — curated tables for admin dashboards';

-- ============================================================================
-- 2. GRANT SYSTEM TABLE ACCESS
-- ============================================================================
-- Note: Replace 'databricks-insights-admins' with your actual group/service principal name
-- These grants require metastore admin privileges

-- Grant access to billing system tables
GRANT USE SCHEMA ON SCHEMA system.billing TO `databricks-insights-admins`;
GRANT SELECT ON SCHEMA system.billing TO `databricks-insights-admins`;

-- Grant access to access/audit system tables
GRANT USE SCHEMA ON SCHEMA system.access TO `databricks-insights-admins`;
GRANT SELECT ON SCHEMA system.access TO `databricks-insights-admins`;

-- Grant access to compute system tables
GRANT USE SCHEMA ON SCHEMA system.compute TO `databricks-insights-admins`;
GRANT SELECT ON SCHEMA system.compute TO `databricks-insights-admins`;

-- Grant access to lakeflow system tables
GRANT USE SCHEMA ON SCHEMA system.lakeflow TO `databricks-insights-admins`;
GRANT SELECT ON SCHEMA system.lakeflow TO `databricks-insights-admins`;

-- Grant access to query system tables
GRANT USE SCHEMA ON SCHEMA system.query TO `databricks-insights-admins`;
GRANT SELECT ON SCHEMA system.query TO `databricks-insights-admins`;

-- ============================================================================
-- 3. GRANT APP ACCESS
-- ============================================================================
-- Note: Replace 'databricks-insights-app-sp' with your actual service principal name

GRANT USE CATALOG ON CATALOG {CATALOG} TO `databricks-insights-app-sp`;
GRANT USE SCHEMA ON SCHEMA {CATALOG}.{SCHEMA} TO `databricks-insights-app-sp`;
GRANT SELECT ON SCHEMA {CATALOG}.{SCHEMA} TO `databricks-insights-app-sp`;
GRANT CREATE TABLE ON SCHEMA {CATALOG}.{SCHEMA} TO `databricks-insights-app-sp`;

-- ============================================================================
-- 4. VERIFY SYSTEM TABLE ACCESS (Optional - for validation)
-- ============================================================================

-- Check billing data recency
-- SELECT MAX(usage_date) AS latest_billing_date,
--        COUNT(*) AS total_records
-- FROM system.billing.usage
-- WHERE usage_date >= CURRENT_DATE - INTERVAL 7 DAYS;

-- Check audit log recency
-- SELECT MAX(event_date) AS latest_audit_date,
--        COUNT(*) AS total_events
-- FROM system.access.audit
-- WHERE event_date >= CURRENT_DATE - INTERVAL 1 DAY;

-- Check cluster data
-- SELECT COUNT(*) AS cluster_records
-- FROM system.compute.clusters
-- WHERE change_time >= CURRENT_DATE - INTERVAL 30 DAYS;

-- Check job data
-- SELECT COUNT(*) AS job_records
-- FROM system.lakeflow.jobs;
