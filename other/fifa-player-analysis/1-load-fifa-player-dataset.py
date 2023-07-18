# Databricks notebook source
# Use when using Spark Connect 

# from databricks.connect import DatabricksSession
# from databricks.sdk.core import Config

# runtime_131 = '0601-182128-dcbte59m'
# runtime_132 = '0630-162806-47pefcs5'

# # Use DEFAULT configuration and pass an existing cluster_id
# # Cluster Runtime > 13.2 
# config = Config(
#     profile = 'DEFAULT',
#   cluster_id = runtime_132
# )

# spark = DatabricksSession.builder.sdkConfig(config).getOrCreate()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

dbutils.widgets.text("catalog_name", "rohit_play_area")
dbutils.widgets.text("schema_name", "fifa_dataset")
dbutils.widgets.text("kaggle_username", "kaggle_username")
dbutils.widgets.text("kaggle_key", "key")

# COMMAND ----------

catalog =  dbutils.widgets.get("catalog_name")
schema = dbutils.widgets.get("schema_name")

spark.sql(f"create catalog if not exists {catalog}")
spark.sql(f"use catalog {catalog}")

spark.sql(f"create schema if not exists {schema}")
spark.sql(f"use schema {schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC # Download fifa dataset from Kaggle

# COMMAND ----------

# MAGIC %pip install kaggle

# COMMAND ----------

import os

kaggle_username = dbutils.widgets.get("kaggle_username") 
kaggle_key = dbutils.widgets.get("kaggle_key") 

os.environ['KAGGLE_USERNAME'] = kaggle_username
os.environ['KAGGLE_KEY'] = kaggle_key

tmp_directory = '/tmp/fifadata/bryanb/fifa-player-stats-database'

import kaggle
kaggle.api.dataset_download_files("bryanb/fifa-player-stats-database",  path = tmp_directory)
file_name = os.listdir(tmp_directory)[0]

# COMMAND ----------

# Unzip 
import zipfile

# Create a ZipFile Object
with zipfile.ZipFile(f"{tmp_directory}/{file_name}", 'r') as zip_obj:
   # Extract all the contents of the zip file
   zip_obj.extractall(path = tmp_directory)
   
   # list file names
   print("Files in the zip file:")
   for file in zip_obj.namelist():
       print(file)

# COMMAND ----------

# MAGIC %md
# MAGIC # Load the data into the Lakehouse

# COMMAND ----------

import pandas as pd
import glob
import re

# Get a list of all CSV files in the target directory
files = glob.glob(f"{tmp_directory}/*.csv")

# Create an empty list to hold the individual dataframes
dataframes = []

for file in files:
    # Extract the year from the filename using regular expressions
    year = re.search(r'FIFA(\d+)_official_data\.csv',  file).group(1)
    df = pd.read_csv(file, na_values=["nan"])
    df["year"] = int(year) + 2000
    dataframes.append(df)

# Concatenate all the dataframes 
players_df = pd.concat(dataframes, ignore_index=True,)

# conssitent column names and handle NaN
players_df.columns = [col.lower().replace(' ','_') for col in players_df.columns] 
players_df.replace({pd.np.nan: None}, inplace=True)

# COMMAND ----------

df = spark.createDataFrame(players_df)

# COMMAND ----------

bronze_table_name = "fifa_player_details_bronze"
df.write.format("delta").mode("overwrite").saveAsTable(bronze_table_name)

# COMMAND ----------

spark.table(bronze_table_name).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup data and load into Silver table

# COMMAND ----------

# Cleanup, ensure consistent datatypes and load silver table
df = spark.sql(
    f"""
        select
          CAST(id AS BIGINT) AS id,
          name,
          CAST(age AS BIGINT) AS age,
          photo,
          nationality,
          flag,
          CAST(overall AS BIGINT) AS overall,
          CAST(potential AS BIGINT) AS potential,
          club,
          club_logo,
          CAST(
            CASE
              WHEN value ILIKE '%k%' THEN CAST(SUBSTRING(value, 2, LENGTH(value) - 2) AS FLOAT) * 1000
              WHEN value ILIKE '%m%' THEN CAST(SUBSTRING(value, 2, LENGTH(value) - 2) AS FLOAT) * 1000000
              ELSE NULL
            END AS FLOAT
          ) AS value,
          CAST(
            CASE
              WHEN wage ILIKE '%k%' THEN CAST(SUBSTRING(wage, 2, LENGTH(wage) - 2) AS FLOAT) * 1000
              WHEN wage ILIKE '%m%' THEN CAST(SUBSTRING(wage, 2, LENGTH(wage) - 2) AS FLOAT) * 1000000
              ELSE NULL
            END AS FLOAT
          ) AS wage,
          CAST(special AS BIGINT) AS special,
          preferred_foot,
          CAST(international_reputation AS DOUBLE) AS international_reputation,
          CAST(weak_foot AS DOUBLE) AS weak_foot,
          CAST(skill_moves AS DOUBLE) AS skill_moves,
          work_rate,
          body_type,
          real_face,
          REGEXP_REPLACE(position, '<.*?>', '') AS position,
          CAST(jersey_number AS DOUBLE) AS jersey_number,
          TO_DATE(joined, 'MMM d, yyyy') AS joined,
          REGEXP_REPLACE(loaned_from, '<.*?>', '') AS loaned_from,
          TO_DATE(
            CASE
              WHEN LENGTH(contract_valid_until) = 4 THEN CONCAT(contract_valid_until, '-05-31') -- Year-only format
              ELSE DATE_FORMAT(contract_valid_until, 'yyyy-MM-dd') -- Full date format
            END
          ) AS contract_valid_until,
          CASE 
            WHEN height ILIKE '%cm' THEN CAST(SUBSTRING(height, 1, LENGTH(height) - 2) AS FLOAT) * 0.393701
            WHEN height LIKE '%''%' THEN CAST(SPLIT(height, "'")[0] AS FLOAT) * 12 + CAST(SPLIT(height, "'")[1] AS FLOAT)
            ELSE NULL 
          END AS height,
          CASE 
            WHEN weight ILIKE '%kg' THEN CAST(SUBSTRING(weight, 1, LENGTH(weight) - 2) AS FLOAT) * 2.20462
            WHEN weight ILIKE '%lbs' THEN CAST(SUBSTRING(weight, 1, LENGTH(weight) - 3) AS FLOAT)
            ELSE NULL 
          END AS weight,
          CAST(crossing AS DOUBLE) AS crossing,
          CAST(finishing AS DOUBLE) AS finishing,
          CAST(headingaccuracy AS DOUBLE) AS headingaccuracy,
          CAST(shortpassing AS DOUBLE) AS shortpassing,
          CAST(volleys AS DOUBLE) AS volleys,
          CAST(dribbling AS DOUBLE) AS dribbling,
          CAST(curve AS DOUBLE) AS curve,
          CAST(fkaccuracy AS DOUBLE) AS fkaccuracy,
          CAST(longpassing AS DOUBLE) AS longpassing,
          CAST(ballcontrol AS DOUBLE) AS ballcontrol,
          CAST(acceleration AS DOUBLE) AS acceleration,
          CAST(sprintspeed AS DOUBLE) AS sprintspeed,
          CAST(agility AS DOUBLE) AS agility,
          CAST(reactions AS DOUBLE) AS reactions,
          CAST(balance AS DOUBLE) AS balance,
          CAST(shotpower AS DOUBLE) AS shotpower,
          CAST(jumping AS DOUBLE) AS jumping,
          CAST(stamina AS DOUBLE) AS stamina,
          CAST(strength AS DOUBLE) AS strength,
          CAST(longshots AS DOUBLE) AS longshots,
          CAST(aggression AS DOUBLE) AS aggression,
          CAST(interceptions AS DOUBLE) AS interceptions,
          CAST(positioning AS DOUBLE) AS positioning,
          CAST(vision AS DOUBLE) AS vision,
          CAST(penalties AS DOUBLE) AS penalties,
          CAST(composure AS DOUBLE) AS composure,
          CAST(marking AS DOUBLE) AS marking,
          CAST(standingtackle AS DOUBLE) AS standingtackle,
          CAST(slidingtackle AS DOUBLE) AS slidingtackle,
          CAST(gkdiving AS DOUBLE) AS gkdiving,
          CAST(gkhandling AS DOUBLE) AS gkhandling,
          CAST(gkkicking AS DOUBLE) AS gkkicking,
          CAST(gkpositioning AS DOUBLE) AS gkpositioning,
          CAST(gkreflexes AS DOUBLE) AS gkreflexes,
          best_position,
          best_overall_rating,
          CAST(
            CASE
              WHEN release_clause ILIKE '%k%' THEN CAST(
                SUBSTRING(release_clause, 2, LENGTH(release_clause) - 2) AS FLOAT
              ) * 1000
              WHEN release_clause ILIKE '%m%' THEN CAST(
                SUBSTRING(release_clause, 2, LENGTH(release_clause) - 2) AS FLOAT
              ) * 1000000
              ELSE NULL
            END AS FLOAT
          ) AS release_clause,
          CAST(defensiveawareness AS DOUBLE) AS defensiveawareness,
          CAST(year AS BIGINT) AS year,
          CAST(kit_number AS DOUBLE) AS kit_number      
        from {bronze_table_name}
    """
)

# COMMAND ----------

display(df)

# COMMAND ----------

# spark.sql(f"drop table {silver_table_name}")
input_table = "fifa_player_input"
spark.sql(f"drop table {input_table}")


# COMMAND ----------

silver_table_name = "fifa_player_details_silver"
df.write.format("delta").mode("overwrite").saveAsTable(silver_table_name)

# COMMAND ----------

input_table = "fifa_player_input"
df.select("id", "name", "year", "value").write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(input_table)

# COMMAND ----------

# %pip install bamboolib

# COMMAND ----------

# import bamboolib as bam

# bam

# df = spark.table(silver_table_name).limit(100000).toPandas()
# df
