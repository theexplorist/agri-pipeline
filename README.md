# Agricultural Sensor Data Pipeline — End-to-End Documentation

## Overview:

This project implements a modular, production-grade data processing pipeline for an agricultural monitoring system.
Sensors placed across farmlands collect data such as temperature, humidity, soil moisture, light intensity, and battery levels.

The pipeline processes this data through four major stages:

* Data Ingestion — reads raw Parquet files, validates schema & logs stats

* Data Transformation — cleans, calibrates, and enriches data

* Data Quality Validation — verifies post-transform integrity using DuckDB

* Data Loading & Storage — optimizes for analytics with Parquet partitioning and compression

All stages are fully modular, easily orchestrated (via Airflow/DAG), and production-ready (incremental load, DuckDB validation, config-driven design).

## Folder structure:

![WhatsApp Image 2025-10-20 at 2 58 56 AM](https://github.com/user-attachments/assets/60194512-f5e7-4e45-981f-1ee471022f10)

## High-Level Design (HLD)

```mermaid
flowchart TD

    %% --- INPUT SOURCE ---
    A[Raw Sensor Data - Parquet files in data/raw] -->|Daily Ingestion| B[Ingestion Pipeline]

    %% --- INGESTION STAGE ---
    B --> B1[Schema Validator - DuckDB Schema Check]
    B --> B2[Data Profiler - Summary Stats]
    B --> B3[Checkpointing and Logging]
    B -->|Valid Files| C[Processed Data - data/processed]
    B -->|Invalid Files| Q[Quarantine - data/quarantine]

    %% --- TRANSFORMATION STAGE ---
    C --> D[Transformation Pipeline]
    D --> D1[DataCleaner - Drop Duplicates and Fix Outliers]
    D --> D2[CalibrationService - Sensor Calibration]
    D --> D3[TimestampProcessor - Convert UTC to IST]
    D --> D4[FeatureEngineer - Rolling Averages and Anomalies]
    D -->|Transformed Files| E[Transformed Data - *_transformed.parquet]

    %% --- VALIDATION STAGE ---
    E --> F[Data Quality Validator - DuckDB Checks]
    F --> F1[Type Validation - Float and Timestamp]
    F --> F2[Range Validation - Config Based]
    F --> F3[Time Gap Check - Hourly Coverage]
    F -->|CSV Report| G[metadata/data_quality_report.csv]

    %% --- STORAGE STAGE ---
    E --> H[Data Loader - Storage Optimization]
    H --> H1[Partition by date and sensor_id]
    H --> H2[Snappy Compression]
    H --> I[data/analytics - Optimized Parquet Dataset]

    %% --- CONFIGURATION & STATE ---
    Z[sensor_config.json - Dynamic Ranges and Calibration] -.-> B
    Z -.-> D
    S[state/checkpoints.json - Incremental Load Tracking] -.-> B

    %% --- OUTPUTS ---
    I --> J[Analytics Layer - BI Tools or Athena Queries]
    G --> J
