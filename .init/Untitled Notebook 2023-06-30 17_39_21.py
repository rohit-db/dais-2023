# Databricks notebook source
catalog =  'rohitb_play_area'
schema = 'sandbox'

spark.sql(f"use catalog {catalog}")
spark.sql(f"use schema {schema}")

# COMMAND ----------

username = dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().apply('user')


# COMMAND ----------

current_user = dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().apply('user')

# COMMAND ----------


