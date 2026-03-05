-- Phase 3: Currently Failing Jobs View
CREATE OR REPLACE VIEW observability.databricks_insights.v_currently_failing_jobs AS
WITH latest_runs AS (
  SELECT
    workspace_id,
    job_id,
    run_id,
    result_state,
    MIN(period_start_time) AS run_start,
    MAX(period_end_time) AS run_end,
    ROW_NUMBER() OVER (
      PARTITION BY workspace_id, job_id
      ORDER BY MIN(period_start_time) DESC
    ) AS run_rank
  FROM system.lakeflow.job_run_timeline
  WHERE period_start_time >= CURRENT_TIMESTAMP - INTERVAL 7 DAYS
  GROUP BY workspace_id, job_id, run_id, result_state
)
SELECT
  lr.workspace_id,
  lr.job_id,
  j.name AS job_name,
  j.creator_user_name AS owner,
  lr.result_state,
  lr.run_start,
  lr.run_end,
  TIMESTAMPDIFF(SECOND, lr.run_start, lr.run_end) AS duration_seconds
FROM latest_runs lr
LEFT JOIN (
  SELECT *, ROW_NUMBER() OVER (
    PARTITION BY workspace_id, job_id ORDER BY change_time DESC
  ) AS rn
  FROM system.lakeflow.jobs
) j ON lr.workspace_id = j.workspace_id
  AND lr.job_id = j.job_id AND j.rn = 1
WHERE lr.run_rank = 1
  AND lr.result_state = 'FAILED'
ORDER BY lr.run_start DESC;
