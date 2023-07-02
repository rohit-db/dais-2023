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

catalog =  'rohitb_play_area'
schema = 'fun_with_fifa'

spark.sql(f"create catalog if not exists {catalog}")
spark.sql(f"use catalog {catalog}")

spark.sql(f"create schema if not exists {schema}")
spark.sql(f"use schema {schema}")

# COMMAND ----------

dbutils.widgets.text("kaggle_username", "kaggle_username")
dbutils.widgets.text("kaggle_key", "key")

# COMMAND ----------

# MAGIC %md
# MAGIC # Download fifa dataset from Kaggle

# COMMAND ----------

# MAGIC %pip install kaggle

# COMMAND ----------

import os

kaggle_username = dbutils.widgets.get("kaggle_username") 
kaggle_key = dbutils.widgets.get("kaggle_key") 

import os
os.environ['KAGGLE_USERNAME'] = "rohitbhagwat"
os.environ['KAGGLE_KEY'] = ""

# COMMAND ----------

tmp_directory = '/tmp/fifadata'

import kaggle
kaggle.api.dataset_download_files("stefanoleone992/fifa-20-complete-player-dataset",  path = tmp_directory)

file_name = os.listdir(tmp_directory)[0]
file_name

# COMMAND ----------

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

import pandas as pd
import glob
import re

# Get a list of all CSV files in the target directory
files = glob.glob(f'{tmp_directory}/players_*.csv')

# Create an empty list to hold the individual dataframes
dataframes = []

for file in files:
    # Extract the year from the filename using regular expressions
    year = re.search(r'players_(\d+).csv', file).group(1)
    
    df = pd.read_csv(file)
    df['year'] = year
    
    # Append the dataframe to the list
    dataframes.append(df)

# Concatenate all the dataframes in the list into one big dataframe
players_df = pd.concat(dataframes, ignore_index=True)

players_df

# COMMAND ----------

df = spark.createDataFrame(players_df)

# COMMAND ----------

df.write.format("delta").mode("overwrite").saveAsTable('players_data')

# COMMAND ----------

teams_df = pd.read_csv(f"{tmp_directory}/teams_and_leagues.csv")
tdf = spark.createDataFrame(teams_df)

tdf.write.format("delta").mode("overwrite").saveAsTable('teams')

# COMMAND ----------


