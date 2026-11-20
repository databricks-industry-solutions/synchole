-- @param table_name STRING
-- @param config_table_name STRING
-- @param refresh_token INT
-- Every change-tracking profile, with how many source tables use it.
WITH usage AS (
  SELECT change_profile, count(*) AS used_by
  FROM IDENTIFIER(:config_table_name)
  WHERE change_tracking IN ('change_table', 'change_procedure')
  GROUP BY change_profile
)
SELECT
  p.profile_name,
  p.description,
  p.method,
  p.change_table_name,
  p.sequence_column,
  p.delete_flag_column,
  p.change_time_column,
  p.procedure_call,
  p.results_table,
  p.update_dttm,
  coalesce(u.used_by, 0) AS used_by
FROM IDENTIFIER(:table_name) p
LEFT JOIN usage u ON u.change_profile = p.profile_name
WHERE :refresh_token >= 0
ORDER BY p.profile_name;
