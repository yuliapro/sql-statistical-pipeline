


SELECT DISTINCT TOP 100
( max_level_reached)
--user_id, started_at, finished_at, duration_seconds, max_level_reached, funnel_category, funnel_type, experiment_name, experiment_group
FROM Datawarehouse.gold.dim_user_id;


SELECT funnel_type, funnel_category, experiment_name, experiment_group, max_level_reached, avg_labour_time, users_cnt, median, st_dev, var, dist_pearson, dist_fisher, gini_coef, CV
FROM Datawarehouse.gold.vw_funnel_statistics
WHERE users_cnt>100
ORDER BY users_cnt DESC
;

SELECT funnel_type, funnel_category, experiment_name, experiment_group, max_level_reached, duration_seconds, row_num, diff_cube
FROM Datawarehouse.gold.vw_user_statistics;
