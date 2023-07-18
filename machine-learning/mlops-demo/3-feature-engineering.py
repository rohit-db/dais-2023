# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC ## 1: Create our Feature Tables
# MAGIC
# MAGIC <img src="https://raw.githubusercontent.com/databricks-demos/dbdemos-resources/main/images/product/feature_store/feature_store_creation_2.png" width="800px" style="float: right">
# MAGIC
# MAGIC In this example, we'll calculate features calculated with window functions.
# MAGIC
# MAGIC To simplify updates & refresh, we'll split them in 2 tables:
# MAGIC
# MAGIC * **User features**: contains all the features for a given user in a given point in time (location, previous purchases if any etc)
# MAGIC
# MAGIC * **Destination features**: data on the travel destination for a given point in time (interest tracked by the number of clicks & impression)||

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Point-in-time support for feature tables
# MAGIC
# MAGIC Databricks Feature Store supports use cases that require point-in-time correctness.
# MAGIC
# MAGIC The data used to train a model often has time dependencies built into it. 
# MAGIC
# MAGIC Because we are adding rolling-window features, our Feature Table will contain data on all the dataset timeframe. 
# MAGIC
# MAGIC When we build our model, we must consider only feature values up until the time of the observed target value. If you do not explicitly take into account the timestamp of each observation, you might inadvertently use feature values measured after the timestamp of the target value for training. This is called “data leakage” and can negatively affect the model’s performance.
# MAGIC
# MAGIC Time series feature tables include a timestamp key column that ensures that each row in the training dataset represents the latest known feature values as of the row’s timestamp. 
# MAGIC
# MAGIC In our case, this timestamp key will be the `ts` field, present in our 2 feature tables.

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Calculating the features
# MAGIC
# MAGIC Let's calculate the aggregated features from the vacation purchase logs for destinations and users. 
# MAGIC
# MAGIC The user features capture the user profile information such as past purchased price. Because the booking data does not change very often, it can be computed once per day in batch.
# MAGIC
# MAGIC The destination features include popularity features such as impressions and clicks, as well as pricing features such as price at the time of booking.

# COMMAND ----------

spark.sql("use catalog rohitb_sandbox")
spark.sql("use schema travel_agency")

# COMMAND ----------

spark.sql("create or replace table vacation_purchase_log_silver as select * from hive_metastore.default.travel_purchase")

# COMMAND ----------

# *****
# Loading Modules
# *****
import pyspark.sql.functions as F
import pyspark.sql.types as T
import pyspark.sql.window as w

from databricks import feature_store
from databricks.feature_store import feature_table, FeatureLookup

import mlflow
import mlflow.sklearn
from mlflow.models.signature import infer_signature
from mlflow.tracking import MlflowClient

import uuid
import os
import requests

from pyspark.sql.functions import lit, expr, rand, col, count, mean, unix_timestamp, window, when
from pyspark.sql.types import StringType, DoubleType, IntegerType, LongType
import numpy as np

#Add features from the time variable 
def add_time_features_spark(df):
    return add_time_features(df.pandas_api()).to_spark()

def add_time_features(df):
    # Extract day of the week, day of the month, and hour from the ts column
    df['day_of_week'] = df['ts'].dt.dayofweek
    df['day_of_month'] = df['ts'].dt.day
    df['hour'] = df['ts'].dt.hour
    
    # Calculate sin and cos values for the day of the week, day of the month, and hour
    df['day_of_week_sin'] = np.sin(df['day_of_week'] * (2 * np.pi / 7))
    df['day_of_week_cos'] = np.cos(df['day_of_week'] * (2 * np.pi / 7))
    df['day_of_month_sin'] = np.sin(df['day_of_month'] * (2 * np.pi / 30))
    df['day_of_month_cos'] = np.cos(df['day_of_month'] * (2 * np.pi / 30))
    df['hour_sin'] = np.sin(df['hour'] * (2 * np.pi / 24))
    df['hour_cos'] = np.cos(df['hour'] * (2 * np.pi / 24))
    df = df.drop(['day_of_week', 'day_of_month', 'hour'], axis=1)
    return df

# COMMAND ----------

def create_user_features(travel_purchase_df):
    """
    Computes the user_features feature group.
    """
    travel_purchase_df = travel_purchase_df.withColumn('ts_l', F.col("ts").cast("long"))
    travel_purchase_df = (
        # Sum total purchased for 7 days
        travel_purchase_df.withColumn("lookedup_price_7d_rolling_sum",
            F.sum("price").over(w.Window.partitionBy("user_id").orderBy(F.col("ts_l")).rangeBetween(start=-(7 * 86400), end=0))
        )
        # counting number of purchases per week
        .withColumn("lookups_7d_rolling_sum", 
            F.count("*").over(w.Window.partitionBy("user_id").orderBy(F.col("ts_l")).rangeBetween(start=-(7 * 86400), end=0))
        )
        # total price 7d / total purchases for 7 d 
        .withColumn("mean_price_7d",  F.col("lookedup_price_7d_rolling_sum") / F.col("lookups_7d_rolling_sum"))
         # converting True / False into 1/0
        .withColumn("tickets_purchased", F.col("purchased").cast('int'))
        # how many purchases for the past 6m
        .withColumn("last_6m_purchases", 
            F.sum("tickets_purchased").over(w.Window.partitionBy("user_id").orderBy(F.col("ts_l")).rangeBetween(start=-(6 * 30 * 86400), end=0))
        )
        .select("user_id", "ts", "mean_price_7d", "last_6m_purchases", "user_longitude", "user_latitude")
    )
    return add_time_features_spark(travel_purchase_df)


user_features_df = create_user_features(spark.table('vacation_purchase_log_silver'))
display(user_features_df)

# COMMAND ----------

def create_destination_features(travel_purchase_df):
    """
    Computes the destination_features feature group.
    """
    return (
        travel_purchase_df
          .withColumn("clicked", F.col("clicked").cast("int"))
          .withColumn("sum_clicks_7d", 
            F.sum("clicked").over(w.Window.partitionBy("destination_id").orderBy(F.col("ts").cast("long")).rangeBetween(start=-(7 * 86400), end=0))
          )
          .withColumn("sum_impressions_7d", 
            F.count("*").over(w.Window.partitionBy("destination_id").orderBy(F.col("ts").cast("long")).rangeBetween(start=-(7 * 86400), end=0))
          )
          .select("destination_id", "ts", "sum_clicks_7d", "sum_impressions_7d")
    )  
destination_features_df = create_destination_features(spark.table('vacation_purchase_log_silver'))
display(destination_features_df)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Creating the Feature Table
# MAGIC
# MAGIC Let's use the FeatureStore client to save our 2 tables. Note the `timestamp_keys='ts'` parameters that we're adding during the table creation.
# MAGIC
# MAGIC Databricks Feature Store will use this information to automatically filter features and prevent from potential leakage.

# COMMAND ----------

fs = feature_store.FeatureStoreClient()
# help(fs.create_table)

# first create a table with User Features calculated above 
fs_table_name_users = f"user_features_advanced"
fs.create_table(
    name=fs_table_name_users, # unique table name (in case you re-run the notebook multiple times)
    primary_keys=["user_id"],
    timestamp_keys="ts",
    df=user_features_df,
    description="User Features",
    # tags={"team":"analytics"}
)

# COMMAND ----------

fs_table_name_destinations = f"destination_features_advanced"
# second create another Feature Table from popular Destinations
# for the second table, we show how to create and write as two separate operations
fs.create_table(
    name=fs_table_name_destinations, # unique table name (in case you re-run the notebook multiple times)
    primary_keys=["destination_id"],
    timestamp_keys="ts", 
    schema=destination_features_df.schema,
    description="Destination Popularity Features",
    # tags={"team":"analytics"} # if you have multiple team creating tables, maybe worse of adding a tag 
)
fs.write_table(name=fs_table_name_destinations, df=destination_features_df, mode="overwrite")

# COMMAND ----------

fs_get_table = fs.get_table(fs_table_name_users)
print(f"Feature Store Table= {fs_table_name_users}. Description: {fs_get_table.description}")
print("The table contains those features: ", fs_get_table.features)

# COMMAND ----------

fs_get_table = fs.get_table(fs_table_name_destinations)
print(f"Feature Store Table= {fs_table_name_destinations}. Description: {fs_get_table.description}")
print("The table contains those features: ", fs_get_table.features)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC <img src="https://raw.githubusercontent.com/databricks-demos/dbdemos-resources/main/images/product/feature_store/feature_store_01.png" style="float: right" width="700px">
# MAGIC
# MAGIC #### Our table is now ready!
# MAGIC
# MAGIC We can explore the Feature store created using the UI. 
# MAGIC
# MAGIC Use the Machine Learning menu and select Feature Store, then your feature table.
# MAGIC
# MAGIC Note the section of **`Producers`**. This section indicates which notebook produced the feature table.
# MAGIC
# MAGIC For now, the consumers are empty. Let's create our first model

# COMMAND ----------


