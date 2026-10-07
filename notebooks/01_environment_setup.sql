-- Databricks notebook source
-- DBTITLE 1,Project Catalog Setup
-- MAGIC %md
-- MAGIC ## Project Catalog Setup & Medallion Schemas
-- MAGIC
-- MAGIC Creates the `rome_transport` Unity Catalog and its medallion-style schemas (`bronze`, `silver`, `features`, `ml`, `gold`). Unity Catalog provides centralized governance, access control, and lineage across all project objects, while the layered schemas enforce a clean bronze → silver → gold data flow with dedicated zones for features and ML artifacts.

-- COMMAND ----------

CREATE CATALOG IF NOT EXISTS rome_transport;

CREATE SCHEMA IF NOT EXISTS rome_transport.bronze;
CREATE SCHEMA IF NOT EXISTS rome_transport.silver;
CREATE SCHEMA IF NOT EXISTS rome_transport.features;
CREATE SCHEMA IF NOT EXISTS rome_transport.ml;
CREATE SCHEMA IF NOT EXISTS rome_transport.gold;

-- COMMAND ----------

-- DBTITLE 1,Raw GTFS Volume
-- MAGIC %md
-- MAGIC ## Raw GTFS Volume
-- MAGIC
-- MAGIC Creates a Unity Catalog volume (`rome_transport.bronze.gtfs_raw`) to store unprocessed GTFS static and realtime files. The volume acts as the landing zone for raw source data before it is parsed and promoted into bronze Delta tables.

-- COMMAND ----------

CREATE VOLUME IF NOT EXISTS rome_transport.bronze.gtfs_raw;

-- COMMAND ----------

-- DBTITLE 1,Environment Validation
-- MAGIC %md
-- MAGIC ## Environment Validation
-- MAGIC
-- MAGIC Verifies that the catalog schemas and the bronze volume were created successfully. This sanity check confirms the project foundation is ready for downstream ingestion and transformation notebooks.

-- COMMAND ----------

SHOW SCHEMAS IN rome_transport;

-- COMMAND ----------

SHOW VOLUMES IN rome_transport.bronze;

-- COMMAND ----------

