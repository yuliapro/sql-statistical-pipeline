/* DATA QUALITY & INTEGRITY SUITE
    ---------------------------------------------------------
    This script performs automated checks across the Medallion 
    Architecture to ensure data consistency, mathematical 
    accuracy, and pipeline stability.
*/

-- =========================================================
-- 1. BRONZE LAYER CHECKS (Ingestion Integrity)
-- =========================================================

-- CHECK: Malformed JSON strings
-- Expected: 0 rows
SELECT 
    'Bronze' AS Layer, 
    'Malformed JSON' AS Issue, 
    COUNT(*) AS Failed_Rows
FROM bronze.interview_events 
WHERE ISJSON(event_params) = 0;

-- CHECK: Orphan Events (Missing User IDs)
-- Expected: 0 rows
SELECT 
    'Bronze' AS Layer, 
    'Null User IDs' AS Issue, 
    COUNT(*) AS Failed_Rows
FROM bronze.interview_events 
WHERE user_id IS NULL;


-- =========================================================
-- 2. SILVER LAYER CHECKS (Standardization & Cleaning)
-- =========================================================

-- CHECK: Duration Anomalies
-- Expected: 0 rows (Duration should not be negative)
SELECT 
    'Silver' AS Layer, 
    'Negative Duration' AS Issue, 
    COUNT(*) AS Failed_Rows
FROM silver.user_activity_refined
WHERE duration_seconds < 0;

-- CHECK: Uniqueness / Deduplication
-- Expected: 0 rows (No duplicate events per user/timestamp)
SELECT 
    'Silver' AS Layer, 
    'Duplicate Event Entry' AS Issue, 
    COUNT(*) AS Failed_Rows
FROM (
    SELECT user_id, event_timestamp
    FROM silver.user_activity_refined
    GROUP BY user_id, event_timestamp
    HAVING COUNT(*) > 1
) AS Dups;


-- =========================================================
-- 3. GOLD LAYER CHECKS (Statistical Sanity)
-- =========================================================

-- CHECK: Gini Index Boundaries
-- Expected: 0 rows (Gini must be between 0 and 1)
SELECT 
    'Gold' AS Layer, 
    'Gini Out of Bounds' AS Issue, 
    COUNT(*) AS Failed_Rows
FROM gold.vw_funnel_statistics 
WHERE gini_coef < 0 OR gini_coef > 1;

-- CHECK: Z-Score Segmentation Integrity
-- Expected: Check distribution. 'Average' should typically be ~68% of total.
SELECT 
    user_segmentation, 
    COUNT(*) AS User_Count,
    CAST(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER() AS DECIMAL(5,2)) AS Percentage
FROM gold.vw_user_zscore_segmentation
GROUP BY user_segmentation;

-- CHECK: Handling of Zero-Variance Groups
-- Expected: Rows with 'Not Classified' or NULL scores where st_dev is 0
SELECT 
    'Gold' AS Layer, 
    'Zero Variance Handling' AS Issue, 
    COUNT(*) AS Total_Groups
FROM gold.vw_funnel_statistics
WHERE st_dev = 0 AND (dist_fisher IS NOT NULL OR dist_pearson IS NOT NULL);
