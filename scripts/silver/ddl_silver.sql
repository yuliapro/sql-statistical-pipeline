/* SILVER LAYER: DATA CLEANING & STANDARDIZING
   ---------------------------------------------------------
   The Silver Layer acts as the intermediate refinement zone. 
   Its primary objective is to transform raw ingestion data into 
   a structured, queryable format. Key processes include:
   
   1. JSON Parsing: Extracting attributes from 'event_params'.
   2. Deduplication: Ensuring unique event records.
   3. Standardization: Formatting timestamps and numeric values.
   4. Flattening: Preparing a structured grain for the Gold Layer.
*/

-- Silver Layer Table Creation
DROP TABLE IF EXISTS silver.interview_events_clean;

-- 2. La creamos con TODAS las columnas
CREATE TABLE silver.interview_events_clean (
    user_id INT,
    event_type VARCHAR(50),
    event_time DATETIME2,
    funnel_type VARCHAR(50),
    experiment_name VARCHAR(50), -- Faltaba esta
    experiment_group VARCHAR(50)  -- Faltaba esta
);

INSERT INTO silver.interview_events_clean (
    user_id, 
    event_type, 
    event_time, 
    funnel_type, 
    experiment_name,
    experiment_group
)
SELECT 
    user_id,
    LOWER(TRIM(event_type)) AS event_type, -- Limpieza de texto
    event_time,
    -- Extracción y limpieza del JSON
    NULLIF(JSON_VALUE(event_params, '$.funnel_type'), '') AS funnel_type,
    NULLIF(JSON_VALUE(event_params, '$.experiment_name'), '') AS experiment_name,
    NULLIF(JSON_VALUE(event_params, '$.experiment_group'), '') AS experiment_group
FROM bronze.interview_events
WHERE user_id IS NOT NULL; -- Filtro de calidad básico




CREATE TABLE silver.user_experiments (
    user_id INT,
    funnel_type VARCHAR(20), -- 'experiment' o 'general'
    experiment_name VARCHAR(50),
    experiment_group VARCHAR(50),
    funnel_category VARCHAR(50)
    PRIMARY KEY (user_id) -- Asumiendo un experimento por usuario
);

INSERT INTO silver.user_experiments (
    user_id, 
    funnel_type, 
    experiment_name, 
    experiment_group, 
    funnel_category
)
SELECT 
    user_id,
    -- Clasificación de Experimento
    CASE 
        WHEN MAX(JSON_VALUE(event_params, '$.experiment_name')) IS NOT NULL THEN 'experiment'
        ELSE 'general' 
    END AS funnel_type,
    -- Datos del Experimento
    MAX(JSON_VALUE(event_params, '$.experiment_name')) AS experiment_name,
    MAX(JSON_VALUE(event_params, '$.experiment_group')) AS experiment_group,
    -- Clasificación de Categoría (Fasting, Yoga, Activity o NULL)
    MAX(NULLIF(JSON_VALUE(event_params, '$.funnel_type'), '')) AS funnel_category
FROM bronze.interview_events
WHERE user_id IS NOT NULL
GROUP BY user_id;




CREATE TABLE silver.user_funnel_activity (
    user_id INT,
    funnel_level VARCHAR(50), -- El evento limpio
    event_time DATETIME2
);

TRUNCATE TABLE silver.user_funnel_activity;
-- Insertar con Trimming y Normalización
INSERT INTO silver.user_funnel_activity
SELECT 
    user_id,
    LOWER(TRIM(event_type)) AS funnel_level,
    event_time
    ,
		CASE
			WHEN event_type = 'funnel_start'THEN 1
			WHEN event_type = 'profile_start' THEN 2
		    WHEN event_type = 'email_submit' THEN 3	
			WHEN event_type = 'paywall_show' THEN 4
			WHEN event_type = 'payment_done' THEN 5
		    ELSE 0
			END AS funnel_level_num
FROM bronze.interview_events
WHERE user_id IS NOT NULL 
	AND event_type <> 'experiment_exposure';


ALTER TABLE silver.user_experiments ADD funnel_category VARCHAR(50);
TRUNCATE TABLE silver.user_experiments;

ALTER TABLE silver.user_funnel_activity ADD funnel_level_num INT;

CREATE CLUSTERED INDEX IX_user_activity_id 
ON silver.user_funnel_activity (user_id);



CREATE CLUSTERED INDEX IX_events_clean_user_id
ON silver.interview_events_clean (user_id);
