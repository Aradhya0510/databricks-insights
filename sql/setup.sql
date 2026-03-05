-- Databricks Insights: Complete Setup SQL
-- This file contains all SQL setup needed for the platform
-- 
-- IMPORTANT: System table grants (section 2) require metastore admin privileges
-- If you don't have admin access, ask your workspace admin to run section 2 once
-- The grants work for all workspace users, so they only need to be done once
--
-- Schema creation (section 1) and app access (section 3) can be done by regular users

-- ============================================================================
-- 1. CREATE SCHEMA (No admin privileges needed - uses existing 'main' catalog)
-- ============================================================================

-- Create the schema in the main catalog (or your configured catalog)
-- Placeholders {CATALOG} and {SCHEMA} will be replaced with actual values from config
CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}
  COMMENT 'Databricks Insights workspace observability platform — curated tables for admin dashboards';

-- ============================================================================
-- 2. GRANT SYSTEM TABLE ACCESS (Requires metastore admin - run once per workspace)
-- ============================================================================
-- Note: These grants only need to be run ONCE by a metastore admin
-- After that, all workspace users can access system tables
-- Placeholder {INSIGHTS_USER} will be replaced with user email from config

-- Grant access to billing system tables
GRANT USE SCHEMA ON SCHEMA system.billing TO `{INSIGHTS_USER}`;
GRANT SELECT ON SCHEMA system.billing TO `{INSIGHTS_USER}`;

-- Grant access to access/audit system tables
GRANT USE SCHEMA ON SCHEMA system.access TO `{INSIGHTS_USER}`;
GRANT SELECT ON SCHEMA system.access TO `{INSIGHTS_USER}`;

-- Grant access to compute system tables
GRANT USE SCHEMA ON SCHEMA system.compute TO `{INSIGHTS_USER}`;
GRANT SELECT ON SCHEMA system.compute TO `{INSIGHTS_USER}`;

-- Grant access to lakeflow system tables
GRANT USE SCHEMA ON SCHEMA system.lakeflow TO `{INSIGHTS_USER}`;
GRANT SELECT ON SCHEMA system.lakeflow TO `{INSIGHTS_USER}`;

-- Grant access to query system tables
GRANT USE SCHEMA ON SCHEMA system.query TO `{INSIGHTS_USER}`;
GRANT SELECT ON SCHEMA system.query TO `{INSIGHTS_USER}`;

-- ============================================================================
-- 3. GRANT APP ACCESS (No admin privileges needed)
-- ============================================================================
-- Note: Placeholder {APP_USER} will be replaced with user email from config

GRANT USE SCHEMA ON SCHEMA {CATALOG}.{SCHEMA} TO `{APP_USER}`;
GRANT SELECT ON SCHEMA {CATALOG}.{SCHEMA} TO `{APP_USER}`;
GRANT CREATE TABLE ON SCHEMA {CATALOG}.{SCHEMA} TO `{APP_USER}`;

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
