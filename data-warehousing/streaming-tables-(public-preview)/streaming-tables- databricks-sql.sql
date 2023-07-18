-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Streaming Tables & Materialized Views in Databricks SQL 
-- MAGIC
-- MAGIC ## In this Notebook 
-- MAGIC
-- MAGIC - Attach Notebook to Databricks SQL Warehouse
-- MAGIC - Look at sample incoming files for streaming
-- MAGIC - Create a Streaming table 
-- MAGIC - Monitor 
-- MAGIC - Create an aggregate table using Materialized View

-- COMMAND ----------

-- Discover and preview data in S3 using LIST

show external locations

-- COMMAND ----------

LIST "s3://one-env-uc-external-location/shared_location/tpcds2.13_sf1_parquet/web_sales"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Preview the data using ```read_files```

-- COMMAND ----------

SELECT * FROM read_files("s3://one-env-uc-external-location/shared_location/tpcds2.13_sf1_parquet/web_sales")
-- No need to pass the file types or schema, it is all inferred

-- COMMAND ----------

SELECT ws_sold_time_sk, ws_ship_date_sk FROM read_files("s3://one-env-uc-external-location/shared_location/tpcds2.13_sf1_parquet/web_sales")
-- No need to pass the file types or schema, it is all inferred

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Create a Streaming table

-- COMMAND ----------

create catalog if not exists rohit_play_area;
use catalog rohit_play_area;
create schema if not exists dbsql_examples;
use schema dbsql_examples;

-- COMMAND ----------

-- Although mentioned in the blog the SQL Warehouse not take SCHEDULE as a valid option

/* Continuous streaming ingest at scale */
CREATE STREAMING TABLE tpcds_web_sales 
-- SCHEDULE CRON ‘0 0 * ? * * *’
AS
SELECT * FROM STREAM read_files("s3://one-env-uc-external-location/shared_location/tpcds2.13_sf1_parquet/web_sales")

-- COMMAND ----------

-- Query the Streaming table
select * from tpcds_web_sales

-- COMMAND ----------

describe extended tpcds_web_sales

-- COMMAND ----------

select count(1) from tpcds_web_sales

-- COMMAND ----------

describe history tpcds_web_sales

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Questions
-- MAGIC - Is the DLT pipeline visible? -> No
-- MAGIC - How to monitor te pipelines?
-- MAGIC - Can it be modified? -> No?
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Create a Materialized View

-- COMMAND ----------


/* Create a Silver aggregate table */
drop materialized view if exists tpcds_web_agg;

CREATE MATERIALIZED VIEW tpcds_web_agg 
-- SCHEDULE CRON ‘0 0 * ? * * *’
AS
SELECT ws_sold_time_sk, sum(ws_net_profit) ws_net_profit from tpcds_web_sales group by all ;


-- COMMAND ----------

describe extended tpcds_web_agg

-- COMMAND ----------

select count(1) from tpcds_web_agg

-- COMMAND ----------

describe history tpcds_web_agg

-- COMMAND ----------

-- Unable to view history on a materialized view, although it is visible via Data Explorer

-- COMMAND ----------


