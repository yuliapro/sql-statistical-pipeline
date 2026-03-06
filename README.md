# sql-statistical-pipeline
Advanced SQL analytics pipeline for A/B testing on 3M+ records. Features Gold Layer views with Gini Coefficient, Fisher Skewness, Pearson’s Skewness, and Z-Score outlier segmentation.
📖 README Summary (Key Metrics & Layers):
Project Overview
This project implements a robust Statistical Analytics Pipeline within a Data Warehouse environment (Medallion Architecture) to evaluate user behavior in A/B experiments.

Data Layers
Gold Layer: Transformation of raw user activity into high-value business logic.

Analytical Views: Decoupled logic for group-level aggregates and individual user performance.

Key Metrics Implemented
Gini Coefficient: Measures inequality in time distribution among test groups.

Fisher & Pearson Skewness: Identifies distribution asymmetry to validate experiment health.

Z-Score Segmentation: Automated outlier detection (Average, Unusual, Outlier, Extreme) for data cleaning.

Technical Highlights
Overflow Protection: Utilizes CAST to BIGINT/FLOAT for high-volume cubic power calculations.

Zero-Division Shielding: Implements NULLIF logic to ensure pipeline stability across inconsistent cohorts.

Window Functions: Adva
