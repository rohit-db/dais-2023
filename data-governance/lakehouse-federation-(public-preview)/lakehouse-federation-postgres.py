# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # In this Notebook
# MAGIC
# MAGIC - PostgreSQL 
# MAGIC   - Use `oetrta` for the demo
# MAGIC   - Review existing tables 
# MAGIC   - Try to add a single table to an existing catalog (this isn't supported yet)
# MAGIC   - Setup a new Connection and a Foreign catalog
# MAGIC   - Setup Permissions 
# MAGIC   - Test Lineage
# MAGIC   - Analyze spark plan
# MAGIC - Databricks to Databricks
# MAGIC - Snowflake (WIP)
# MAGIC - Redshift (WIP) 

# COMMAND ----------

# MAGIC %md 
# MAGIC # PostgreSQL

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC <img src="https://wiki.postgresql.org/images/9/9a/PostgreSQL_logo.3colors.540x557.png" width="200">
# MAGIC
# MAGIC | Specs                                   |
# MAGIC |----------------------|------------------|
# MAGIC | Cluster Name         | oetrta-postgres  |
# MAGIC | Cluster Instance Type| db.t2.micro      |
# MAGIC | Region/AZ            | us-west-2a       |
# MAGIC | RAM                  | 1GB              |
# MAGIC | Engine Version       | 12.5             |
# MAGIC | Storage Type         | SSD              |
# MAGIC | Storage              | 100GiB           |
# MAGIC <br>
# MAGIC 1. Attach IAM role `oetrta-IAM-access` to this cluster
# MAGIC 2. Create your tables with your name appended, e.g. "name_iris"
# MAGIC
# MAGIC We use the MySQL-connector that is built-in the Databricks Runtime. Not need to install any drivers.  
# MAGIC All tables will be written to `oetrta` database.
# MAGIC
# MAGIC Docs: https://docs.databricks.com/data/data-sources/sql-databases.html#connecting-to-sql-databases-using-jdbc

# COMMAND ----------

# MAGIC %md
# MAGIC ## Review the PostgreSQL database

# COMMAND ----------

# DBTITLE 1,Get Secret Credentials
hostname = dbutils.secrets.get( "oetrta", "postgres-hostname" )
port     = dbutils.secrets.get( "oetrta", "postgres-port"     )
database = dbutils.secrets.get( "oetrta", "postgres-database" )
username = dbutils.secrets.get( "oetrta", "postgres-username" )
password = dbutils.secrets.get( "oetrta", "postgres-password" )

# COMMAND ----------

# DBTITLE 1,Construct Postgres JDBC URL
postgres_url = f"jdbc:postgresql://{hostname}:{port}/{database}?user={username}&password={password}"

# COMMAND ----------

# Review existing tables using the information_schema
tables = ( spark.read 
       .format( "jdbc") 
       .option( "url",     postgres_url ) 
       .option( "dbtable", "information_schema.tables")
       .load()
     ).filter("table_schema = 'public'")

display( tables )

# COMMAND ----------

table_names = [row[0] for row in tables.selectExpr("table_name").collect()]

dbutils.widgets.dropdown("postgresql_table", "loan_data" , table_names)

# COMMAND ----------

# DBTITLE 0,Read from Postgres
postgres_table = dbutils.widgets.get("postgresql_table")

# Read the loan_data table 
df = ( spark.read 
       .format( "jdbc") 
       .option( "url",     postgres_url ) 
       .option( "dbtable", postgres_table) 
       .load()
     )

display( df )

# COMMAND ----------

dbutils.widgets.text("catalog_name", "rohit_play_area")
dbutils.widgets.text("schema", "lake_federation")

# COMMAND ----------

catalog = dbutils.widgets.get("catalog_name")
schema = dbutils.widgets.get("schema")

spark.sql(f"create catalog if not exists {catalog}")
spark.sql(f"use catalog {catalog}")

spark.sql(f"create schema if not exists {schema}")
spark.sql(f"use schema {schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Try to create a table in an existing catalog
# MAGIC
# MAGIC **Note: This does not work**

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists ${postgresql_table};
# MAGIC
# MAGIC CREATE TABLE postgresql_table USING postgresql OPTIONS (
# MAGIC   dbtable '${postgresql_table}',
# MAGIC   host secret("oetrta", "postgres-hostname"),
# MAGIC   port secret("oetrta", "postgres-port"),
# MAGIC   database secret("oetrta", "postgres-database"),
# MAGIC   user secret("oetrta", "postgres-username"),
# MAGIC   password secret("oetrta", "postgres-password")
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create a New Connection and a Foreign Catalog - Current support for Query Federation

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE connection if not exists oetrta_postgresql type postgresql 
# MAGIC
# MAGIC OPTIONS (
# MAGIC   host secret("oetrta", "postgres-hostname"),
# MAGIC   port secret("oetrta", "postgres-port"),
# MAGIC   user secret("oetrta", "postgres-username"),
# MAGIC   password secret("oetrta", "postgres-password")
# MAGIC );
# MAGIC
# MAGIC describe connection extended oetrta_postgresql;
# MAGIC

# COMMAND ----------

# MAGIC %sql 
# MAGIC show connections
# MAGIC
# MAGIC -- Can create multiple connections to the same source

# COMMAND ----------

dbutils.widgets.text("foreign_catalog", "rohit_play_area_postgres")

# COMMAND ----------

# MAGIC %sql 
# MAGIC --Creating foreign catalog
# MAGIC drop catalog if exists ${foreign_catalog};
# MAGIC create foreign catalog if not exists ${foreign_catalog} using connection oetrta_postgresql
# MAGIC options (database secret( "oetrta", "postgres-database"))

# COMMAND ----------

# MAGIC %sql
# MAGIC show schemas in  ${foreign_catalog};

# COMMAND ----------

# MAGIC %sql 
# MAGIC show tables in ${foreign_catalog}.public

# COMMAND ----------

# MAGIC %md
# MAGIC ## Governance on foreign tables

# COMMAND ----------

# MAGIC %sql select * from ${foreign_catalog}.public.${postgresql_table}

# COMMAND ----------

# MAGIC %sql explain select * from ${foreign_catalog}.public.${postgresql_table}

# COMMAND ----------

# MAGIC %sql 
# MAGIC grant select on ${foreign_catalog}.public.${postgresql_table} to `analysts`

# COMMAND ----------

# MAGIC %sql 
# MAGIC show grants on ${foreign_catalog}.public.${postgresql_table};

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lineage 

# COMMAND ----------

spark.table(f"{dbutils.widgets.get('foreign_catalog')}.public.{postgres_table}").write.format("delta").mode("overwrite").saveAsTable(f"{catalog}.{schema}.{postgres_table}")

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Query the system table for Lineage

# COMMAND ----------

spark.sql(
    f"""select source_table_schema, source_table_name, entity_type , *
    from system.lineage.table_lineage where target_table_full_name = '{catalog}.{schema}.{postgres_table}'"""
).display()

# COMMAND ----------

# Check the lineage - the lineage should capture the source table being a Postgres table 

# COMMAND ----------

# MAGIC %md
# MAGIC <img src="https://www.databricks.com/sites/default/files/inline-images/image1%20%281%29.png">

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Impact of dropping catalog / connections

# COMMAND ----------

# MAGIC %sql 
# MAGIC -- Cannot drop a connection if a foreign catalog is defined
# MAGIC drop connection oetrta_postgresql
# MAGIC

# COMMAND ----------

# MAGIC %sql 
# MAGIC
# MAGIC drop catalog ${foreign_catalog}
# MAGIC

# COMMAND ----------

# MAGIC %sql 
# MAGIC
# MAGIC drop connection oetrta_postgresql
# MAGIC

# COMMAND ----------

# Check the lineage -> The lineage will not have the source table!

# COMMAND ----------


