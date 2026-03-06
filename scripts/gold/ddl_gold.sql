/* GOLD LAYER: ADVANCED ANALYTICS & STATISTICAL MODELING
   ---------------------------------------------------------
   The Gold Layer represents the final stage of data refinement.
   It provides business-ready insights through high-performance 
   Analytical Views. This layer focuses on:

   1. Statistical Distribution: Implementing Gini, Fisher, and Pearson metrics.
   2. Experiment Validation: Comparing control vs. test groups in A/B testing.
   3. Outlier Management: Automated user segmentation using Z-Scores.
   4. Stability: Using NULLIF and CAST logic to ensure calculation robustness.
*/

-- =========================================================
-- 1. VIEW: vw_user_statistics
-- Objective: Individual user grain with ranking and variance prep.
-- =========================================================
CREATE OR ALTER VIEW gold.vw_user_statistics AS
SELECT  
    user_id,
    funnel_type,
    funnel_category,
    experiment_name,
    experiment_group,
    max_level_reached,
    duration_seconds,
    ROW_NUMBER() OVER (
        PARTITION BY funnel_type, funnel_category, experiment_name, experiment_group, max_level_reached 
        ORDER BY duration_seconds
    ) AS row_num,
    ROUND(POWER(CAST(duration_seconds AS FLOAT) - 
        AVG(CAST(duration_seconds AS FLOAT)) OVER (
            PARTITION BY funnel_type, funnel_category, experiment_name, experiment_group, max_level_reached
        ), 3), 2) AS diff_cube
FROM Datawarehouse.gold.dim_user_id;
GO

-- =========================================================
-- 2. VIEW: vw_funnel_statistics
-- Objective: Group-level summary with Gini, Fisher, and Pearson.
-- =========================================================
CREATE OR ALTER VIEW gold.vw_funnel_statistics AS
WITH grouped_stats AS (
    SELECT  
        funnel_type, funnel_category, experiment_name, experiment_group, max_level_reached, 
        AVG(duration_seconds) AS avg_labour_time,
        COUNT(user_id) AS users_cnt
    FROM Datawarehouse.gold.dim_user_id
    GROUP BY funnel_type, funnel_category, experiment_name, experiment_group, max_level_reached
),
median_stats AS (
    SELECT DISTINCT 
        funnel_type, funnel_category, experiment_name, experiment_group, max_level_reached,
        PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY duration_seconds) OVER(
            PARTITION BY funnel_type, funnel_category, experiment_name, experiment_group, max_level_reached
        ) AS median,
        STDEV(duration_seconds) OVER (
            PARTITION BY funnel_type, funnel_category, experiment_name, experiment_group, max_level_reached
        ) AS st_dev
    FROM Datawarehouse.gold.dim_user_id
),
advanced_metrics AS (
    SELECT 
        funnel_type, funnel_category, experiment_name, experiment_group, max_level_reached,
        ((2.0 * SUM(CAST(row_num AS BIGINT) * duration_seconds)) / 
         NULLIF(COUNT(*) * SUM(CAST(duration_seconds AS BIGINT)), 0)) - 
        ((COUNT(*) + 1.0) / NULLIF(COUNT(*), 0)) AS gini_coef,
        SUM(diff_cube) AS sum_diff_cube
    FROM gold.vw_user_statistics
    GROUP BY funnel_type, funnel_category, experiment_name, experiment_group, max_level_reached
)
SELECT  
    g.*, m.median, m.st_dev,
    (3 * (g.avg_labour_time - m.median)) / NULLIF(m.st_dev, 0) AS dist_pearson,
    am.sum_diff_cube / NULLIF((g.users_cnt - 1) * POWER(CAST(m.st_dev AS FLOAT), 3), 0) AS dist_fisher,
    am.gini_coef,
    ROUND(m.st_dev / NULLIF(g.avg_labour_time, 0), 2) AS CV
FROM grouped_stats g
JOIN median_stats m ON g.experiment_group = m.experiment_group AND g.max_level_reached = m.max_level_reached
JOIN advanced_metrics am ON g.experiment_group = am.experiment_group AND g.max_level_reached = am.max_level_reached;
GO

-- =========================================================
-- 3. VIEW: vw_user_zscore_segmentation
-- Objective: Individual performance classification and outlier detection.
-- =========================================================
CREATE OR ALTER VIEW gold.vw_user_zscore_segmentation AS
WITH z_calculation AS (
    SELECT 
        u.*, f.avg_labour_time, f.st_dev,
        ROUND((u.duration_seconds - f.avg_labour_time) / NULLIF(f.st_dev, 0), 2) AS z_score
    FROM gold.vw_user_statistics u 
    LEFT JOIN gold.vw_funnel_statistics f ON u.max_level_reached = f.max_level_reached 
    AND u.experiment_group = f.experiment_group
)
SELECT *,
    CASE 
        WHEN ABS(z_score) <= 1 THEN 'Average'
        WHEN ABS(z_score) > 1 AND ABS(z_score) <= 3 THEN 'Unusual'
        WHEN ABS(z_score) > 3 AND ABS(z_score) <= 5 THEN 'Outlier'
        WHEN ABS(z_score) > 5 THEN 'Extreme Outlier'
        ELSE 'Not Classified' 
    END AS user_segmentation
FROM z_calculation;
GO
