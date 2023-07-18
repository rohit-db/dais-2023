-- Databricks notebook source
-- MAGIC %md 
-- MAGIC # Setup Delta Live Tables Pipeline to Ingest Data

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC # Simplify Ingestion and Transformation with Delta Live Tables
-- MAGIC
-- MAGIC <img style="float: right" width="500px" src="https://github.com/databricks-demos/dbdemos-resources/raw/main/images/retail/lakehouse-churn/lakehouse-retail-c360-churn-1.png" />
-- MAGIC
-- MAGIC In this notebook, we'll work as a Data Engineer to build our database. <br>
-- MAGIC We'll consume and clean our raw data sources to prepare the tables required for our BI & ML workload.
-- MAGIC
-- MAGIC Databricks simplifies this task with Delta Live Table (DLT) by making Data Engineering accessible to all.
-- MAGIC
-- MAGIC DLT allows Data Analysts to create advanced pipelines with plain SQL.
-- MAGIC
-- MAGIC ## Delta Live Table: A simple way to build and manage data pipelines for fresh, high quality data!
-- MAGIC
-- MAGIC <div>
-- MAGIC   <div style="width: 45%; float: left; margin-bottom: 10px; padding-right: 45px">
-- MAGIC     <p>
-- MAGIC       <img style="width: 50px; float: left; margin: 0px 5px 30px 0px;" src="https://raw.githubusercontent.com/QuentinAmbard/databricks-demo/main/retail/resources/images/lakehouse-retail/logo-accelerate.png"/> 
-- MAGIC       <strong>Accelerate ETL development</strong> <br/>
-- MAGIC       Enable analysts and data engineers to innovate rapidly with simple pipeline development and maintenance 
-- MAGIC     </p>
-- MAGIC     <p>
-- MAGIC       <img style="width: 50px; float: left; margin: 0px 5px 30px 0px;" src="https://raw.githubusercontent.com/QuentinAmbard/databricks-demo/main/retail/resources/images/lakehouse-retail/logo-complexity.png"/> 
-- MAGIC       <strong>Remove operational complexity</strong> <br/>
-- MAGIC       By automating complex administrative tasks and gaining broader visibility into pipeline operations
-- MAGIC     </p>
-- MAGIC   </div>
-- MAGIC   <div style="width: 48%; float: left">
-- MAGIC     <p>
-- MAGIC       <img style="width: 50px; float: left; margin: 0px 5px 30px 0px;" src="https://raw.githubusercontent.com/QuentinAmbard/databricks-demo/main/retail/resources/images/lakehouse-retail/logo-trust.png"/> 
-- MAGIC       <strong>Trust your data</strong> <br/>
-- MAGIC       With built-in quality controls and quality monitoring to ensure accurate and useful BI, Data Science, and ML 
-- MAGIC     </p>
-- MAGIC     <p>
-- MAGIC       <img style="width: 50px; float: left; margin: 0px 5px 30px 0px;" src="https://raw.githubusercontent.com/QuentinAmbard/databricks-demo/main/retail/resources/images/lakehouse-retail/logo-stream.png"/> 
-- MAGIC       <strong>Simplify batch and streaming</strong> <br/>
-- MAGIC       With self-optimization and auto-scaling data pipelines for batch or streaming processing 
-- MAGIC     </p>
-- MAGIC </div>
-- MAGIC </div>
-- MAGIC
-- MAGIC <br style="clear:both">
-- MAGIC
-- MAGIC <img src="https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-logo.png" style="float: right;" width="200px">
-- MAGIC
-- MAGIC ## Delta Lake
-- MAGIC
-- MAGIC All the tables we'll create in the Lakehouse will be stored as Delta Lake tables. Delta Lake is an open storage framework for reliability and performance.<br>
-- MAGIC It provides many functionalities (ACID Transaction, DELETE/UPDATE/MERGE, Clone zero copy, Change data Capture...)<br>
-- MAGIC For more details on Delta Lake, run dbdemos.install('delta-lake')
-- MAGIC
-- MAGIC <!-- Collect usage data (view). Remove it to disable collection. View README for more details.  -->
-- MAGIC <img width="1px" src="https://www.google-analytics.com/collect?v=1&gtm=GTM-NKQ8TT7&tid=UA-163989034-1&cid=555&aip=1&t=event&ec=field_demos&ea=display&dp=%2F42_field_demos%2Fretail%2Flakehouse_churn%2Fdlt_sql&dt=LAKEHOUSE_RETAIL_CHURN">

-- COMMAND ----------

create or refresh streaming table vacation_purchase_logs_bronze 
comment "Raw purchase log files" 
as
select  *
from
  cloud_files(
    "/databricks-datasets/travel_recommendations_realtime/raw_travel_data/fs-demo_vacation-purchase_logs",
    "csv",
    map("header", "true")
  )

-- COMMAND ----------

create or refresh streaming table vacation_purchase_logs_silver 
(constraint valid_user EXPECT (user_id IS NOT NULL))
comment "Cleansed silver layer" 
as 
select cast(ts as timestamp) as ts,
  cast(user_id as int) as user_id,
  cast(destination_id as int) as destination_id,
  cast(user_latitude as double) as user_latitude,
  cast(user_longitude as double) as user_longitude, 
  cast(clicked as boolean) as clicked,
  cast(purchased as boolean) as purchased,
  cast(booking_date as date) as booking_date,
  cast(price as double) as price
from stream(live.vacation_purchase_logs_bronze)

-- COMMAND ----------

create or refresh live table destination_bronze 
(constraint valid_destination expect (destination_id > 0) on violation drop row)
comment "Raw purchase destination " 
as
select  _c0 as name, _c1 as destination_id, _c2 as latitude, _c3 as longitude 
from csv.`/databricks-datasets/travel_recommendations_realtime/raw_travel_data/fs-demo_destination-locations`

-- COMMAND ----------

create or refresh live table destination_silver 
(constraint valid_destination expect (destination_id > 0) on violation drop row)
comment "Raw purchase destination " 
as
select  cast(name as string) as name, 
  cast(destination_id as int) as destination_id, 
  cast(latitude as double) as latitude, 
  cast(longitude as double) as longitude
from live.destination_bronze

-- COMMAND ----------

create or refresh live table vacation_purchase_logs
(constraint valid_destination expect (destination_id > 0),
constraint valid_price expect (price > 0))
as 
select ts, user_id, p.destination_id, user_latitude, user_longitude, d.latitude as dest_latitude, d.longitude dest_longitude, purchased, booking_date, price
from live.vacation_purchase_logs_silver p left outer join live.destination_silver d on p.destination_id = d.destination_id
