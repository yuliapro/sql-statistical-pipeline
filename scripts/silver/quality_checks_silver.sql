SELECT TOP 100
*
FROM silver.user_funnel_activity 
WHERE user_id = 344344
;


SELECT  TOP 100
*
FROM silver.user_experiments
WHERE user_id = 344344;


SELECT user_id, event_type, event_time, funnel_type, experiment_name, experiment_group
FROM silver.interview_events_clean
WHERE user_id = 344344;

