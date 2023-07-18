# Databricks notebook source
dbutils.widgets.text("catalog_name", "rohit_play_area")
dbutils.widgets.text("schema_name", "fifa_dataset")

# COMMAND ----------

catalog =  dbutils.widgets.get("catalog_name")
schema = dbutils.widgets.get("schema_name")

spark.sql(f"use catalog {catalog}")
spark.sql(f"use schema {schema}")

silver_table = 'fifa_player_details_silver'

# COMMAND ----------

# Review the dataset 
display(spark.table(silver_table))

# COMMAND ----------

# MAGIC %sql select year, count(distinct id, name), count(1), count(distinct id) from fifa_player_details_silver group by all 

# COMMAND ----------

from pyspark.sql import functions as F

def demographic_features(df):
    # Create 'Age_Group'
    df = df.withColumn('Age_Group', F.when(df['age'] <= 25, 'Young')
                                      .when((df['age'] > 25) & (df['age'] <= 30), 'Mid')
                                      .otherwise('Old'))

    # Create 'High_Reputation' column
    df = df.withColumn('High_Reputation', (df['international_reputation'] > 3).cast('int'))

    # Create 'Work_Rate_Score' column
    df = df.withColumn('Work_Rate_Score', F.when(df['work_rate'] == 'High/High', 5)
                                            .when(df['work_rate'] == 'Medium/Medium', 3)
                                            .when(df['work_rate'] == 'Low/Low', 1)
                                            .otherwise(0))

    # Select all the original and new columns for the new dataframe
    demographic_attributes = ['age', 'nationality', 'preferred_foot',
                              'international_reputation', 'work_rate', 'position', 'best_position','year']
    df_demographic_features = df.select('id', 'Age_Group', 'High_Reputation', 'Work_Rate_Score', *demographic_attributes)

    return df_demographic_features


# COMMAND ----------

def skill_attributes_features(df):

    df = df.fillna(0)
    # Define offensive and defensive skills
    offensive_skills = ['crossing', 'finishing', 'volleys', 'dribbling', 'curve', 'fkaccuracy', 'longshots', 'penalties']
    # Ignore marking as the values are all null 
    defensive_skills = ['marking', 'standingtackle', 'slidingtackle', 'aggression', 'interceptions', 'defensiveawareness']
    goalkeeper_skills = ['gkdiving', 'gkhandling', 'gkkicking', 'gkpositioning', 'gkreflexes']

    # Compute 'Offensive Score' and 'Defensive Score'
    df = df.withColumn('Offensive_Score', sum(df[skill] for skill in offensive_skills) / len(offensive_skills))
    df = df.withColumn('Defensive_Score', sum(df[skill] for skill in defensive_skills) / len(defensive_skills))

    # Compute 'Special Skill'
    df = df.withColumn('Special_Skill', (F.array_max(F.array([df[skill] for skill in offensive_skills + defensive_skills])) > 85).cast('int'))

    # Compute 'Goalkeeper Skill Score'
    df = df.withColumn('Goalkeeper_Skill_Score', sum(df[skill] for skill in goalkeeper_skills) / len(goalkeeper_skills))

    # Compute 'Skill Variance'
    skill_attributes = offensive_skills + defensive_skills
    skill_mean = sum(df[skill] for skill in skill_attributes) / len(skill_attributes)
    df = df.withColumn('Skill_Variance', sum((df[skill] - skill_mean)**2 for skill in skill_attributes) / len(skill_attributes))

    # Select all the original and new columns for the new dataframe
    df_skill_features = df.select('id', 'Offensive_Score', 'Defensive_Score', 'Special_Skill', 'Goalkeeper_Skill_Score', 'Skill_Variance','year', *skill_attributes)

    return df_skill_features


# COMMAND ----------

def physical_attributes_features(df):
    # Compute 'BMI'
    df = df.withColumn('BMI', df['weight'] / ((df['height']/100)**2))

    # Compute 'Speed Score'
    df = df.withColumn('Speed_Score', (df['acceleration'] + df['sprintspeed']) / 2)

    # Compute 'Mobility Score'
    df = df.withColumn('Mobility_Score', (df['acceleration'] + df['sprintspeed'] + df['agility']) / 3)

    # Compute 'Endurance Score'
    df = df.withColumn('Endurance_Score', (df['stamina'] + df['strength'] + df['jumping']) / 3)

    # Compute 'Physical Variance'
    physical_attributes = ['height', 'weight', 'strength', 'stamina', 'acceleration', 'sprintspeed', 'agility', 'jumping']
    physical_mean = sum(df[attr] for attr in physical_attributes) / len(physical_attributes)
    df = df.withColumn('Physical_Variance', sum((df[attr] - physical_mean)**2 for attr in physical_attributes) / len(physical_attributes))

    # Select all the original and new columns for the new dataframe
    df_physical_features = df.select('id', 'BMI', 'Speed_Score', 'Mobility_Score', 'Endurance_Score', 'Physical_Variance','year', *physical_attributes)

    return df_physical_features


# COMMAND ----------

def contractual_attributes_features(df):
    # Compute 'Contract Duration'
    df = df.withColumn('Contract_Duration', F.year(df['contract_valid_until']) - F.year(df['joined']))

    # Select all the original and new columns for the new dataframe
    contractual_attributes = ['club', 'contract_valid_until', 'release_clause', 'wage']
    df_contractual_features = df.select('id', 'Contract_Duration', 'year', *contractual_attributes)

    return df_contractual_features


# COMMAND ----------

df = spark.table(silver_table)

demographic_features_df = demographic_features(df)
skill_attributes_features_df = skill_attributes_features(df)
physical_attributes_features_df = physical_attributes_features(df)
contractual_attributes_features_df = contractual_attributes_features(df)

# COMMAND ----------

fs_table_name_demographic = f"{catalog}.{schema}.player_demographic_features"
fs_table_name_skill = f"{catalog}.{schema}.player_skill_features"
fs_table_name_physical = f"{catalog}.{schema}.player_physical_features"
fs_table_name_contractual = f"{catalog}.{schema}.player_contractual_features"

# fs.drop_table(fs_table_name_demographic)
# fs.drop_table(fs_table_name_skill)
# fs.drop_table(fs_table_name_physical)
# fs.drop_table(fs_table_name_contractual)


# COMMAND ----------

from databricks import feature_store
from databricks.feature_store import feature_table, FeatureLookup

fs = feature_store.FeatureStoreClient()

# Create Physical attributes
fs.create_table(
    name=fs_table_name_demographic, # unique table name (in case you re-run the notebook multiple times)
    primary_keys=["id", "year"],
    # timestamp_keys="year",
    df=demographic_features_df,
    description="Player Demographic features"
    # tags={"team":"fifa"}
)
fs.write_table(name=fs_table_name_demographic, df=demographic_features_df, mode="overwrite")

# COMMAND ----------

# Create skill feature table
fs.create_table(
    name=fs_table_name_skill, # unique table name (in case you re-run the notebook multiple times)
    primary_keys=["id", "year"],
    # timestamp_keys="year",
    df=skill_attributes_features_df,
    description="Player skill features"
    # tags={"team":"fifa"}
)
fs.write_table(name=fs_table_name_skill, df=skill_attributes_features_df, mode="overwrite")

# COMMAND ----------

# Create Physical Attributes
fs.create_table(
    name=fs_table_name_physical, # unique table name (in case you re-run the notebook multiple times)
    primary_keys=["id", "year"],
    df=physical_attributes_features_df,
    description="Player physical features"
)
fs.write_table(name=fs_table_name_physical, df=physical_attributes_features_df, mode="overwrite")

# COMMAND ----------

# Create contractual attributes feature table
fs.create_table(
    name=fs_table_name_contractual, # unique table name (in case you re-run the notebook multiple times)
    primary_keys=["id", "year"],
    df=contractual_attributes_features_df,
    description="Player contractual features"
)
fs.write_table(name=fs_table_name_contractual, df=contractual_attributes_features_df, mode="overwrite")

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Build the training dataset 
# MAGIC
# MAGIC Let's start by building the dataset, retrieving features from our feature table.

# COMMAND ----------

model_feature_lookups = [
    {
        # Adding the correct syntax for declaring a key-value pair in dictionary
        "table_name": fs_table_name_demographic,
        "lookup_key": ["id", "year"] # Adding a missing comma after the list
    },
    {
        "table_name": fs_table_name_skill,
        "lookup_key": ["id", "year"]
    },
    {
        "table_name": fs_table_name_physical,
        "lookup_key": ["id", "year"] # Adding a missing comma after the dictionary
    }, # Adding a comma at the end of each dictionary in the list, except for the last one
    {
        "table_name": fs_table_name_contractual,
        "lookup_key": ["id", "year"]
    }
]

# COMMAND ----------

fifa_player_input = spark.table(f"{catalog}.{schema}.fifa_player_input")

from databricks import automl

automl.regress(
  dataset = fifa_player_input,
  target_col ="value",
  exclude_cols= ["id"],
  # experiment_name = "rohitb_fifa_player_regresser",
  feature_store_lookups = model_feature_lookups,
  timeout_minutes = 10
)

# COMMAND ----------

import mlflow
mlflow.set_registry_uri("databricks-uc")

# COMMAND ----------

import mlflow
# mlflow.set_registry_uri("databricks-uc")

experiment = mlflow.get_experiment(969987237984485)

best_model = mlflow.search_runs(experiment_ids=[969987237984485], order_by=["metrics.val_f1_score DESC"], max_results=1, filter_string="status = 'FINISHED'")
best_model

# COMMAND ----------

# Once we have our best model, we can now deploy it in production using it's run ID

run_id = best_model.iloc[0]['run_id']

#add some tags that we'll reuse later to validate the model
client = mlflow.tracking.MlflowClient()
client.set_tag(run_id, key='features', value='demographic,skill,physical,contractual')
client.set_tag(run_id, key='db_table', value=f'f"{catalog}.{schema}.fifa_player_input')



#Deploy our autoML run in MLFlow registry
# model_details = mlflow.register_model(f"runs:/{run_id}/model", "rohitb_fifa_player_regresser")

# COMMAND ----------

model_details = mlflow.register_model(f"runs:/{run_id}/model", "rohitb_fifa_player_regresser")

# COMMAND ----------

from mlflow.models.signature import infer_signature



# COMMAND ----------

from mlflow.models.signature import infer_signature

# Infer and log model signature
X_test = spark.table(f"{catalog}.{scchema}.fifa_player_input")
signature = infer_signature(X_test, fs.score_batch(model_uri=model_uri, df=test))
mlflow.sklearn.log_model(best_model, "rohitb_fifa_player_regresser", signature=signature)

# COMMAND ----------

signature

# COMMAND ----------

f"runs:/{run_id}/{best_model['artifact_uri'][0]})"

# COMMAND ----------

mlflow.pyfunc.get

# COMMAND ----------

 test = spark.table(f"{catalog}.{schema}.fifa_player_input").filter("year = 2022")

# COMMAND ----------

model_uri = f"runs:/{run_id}/model"

from databricks.feature_store import FeatureStoreClient
fs = FeatureStoreClient()

df = fs.score_batch(model_uri=model_uri, df=test) # specify `result_type` if it is not "double"

        

# COMMAND ----------

display(df.select("name", "club", "wage", "release_clause", "value", "prediction", "*").filter("name ilike '%mess%'"))

# COMMAND ----------

# MAGIC %sql select * from rohit_play_area.fifa_dataset.fifa_player_details_silver where name ilike '%messi'

# COMMAND ----------

# MAGIC %sql select * from rohit_play_area.fifa_dataset.fifa_player_details_bronze where name ilike '%messi'

# COMMAND ----------

# MAGIC %sql select distinct weight from rohit_play_area.fifa_dataset.fifa_player_details_bronze

# COMMAND ----------

# MAGIC %sql select distinct height from rohit_play_area.fifa_dataset.fifa_player_details_bronze

# COMMAND ----------


