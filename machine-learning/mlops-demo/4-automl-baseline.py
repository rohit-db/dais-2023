# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## 2: Train a model with FS and timestamp lookup

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create the training dataset
# MAGIC
# MAGIC The next step is to build a training dataset. 
# MAGIC
# MAGIC Because we have 2 feature tables, we'll add 2 `FeatureLookup` entries, specifying the key so that the feature store engine can join using this field.
# MAGIC
# MAGIC We will also add the `timestamp_lookup_key` property to `ts` so that the engine filter the features based on this key.

# COMMAND ----------

spark.sql("use catalog rohitb_sandbox")
spark.sql("use schema travel_agency")

# COMMAND ----------

ground_truth_df = spark.table('vacation_purchase_log_silver').select('user_id', 'destination_id', 'purchased', 'ts')

# Split based on time to define a training and inference set (we'll do train+eval on the past & test in the most current value)
training_labels_df = ground_truth_df.where("ts < '2022-11-23'")
test_labels_df = ground_truth_df.where("ts >= '2022-11-23'")

display(test_labels_df)

# COMMAND ----------

from databricks import feature_store
from databricks.feature_store import feature_table, FeatureLookup

fs_table_name_users = f"user_features_advanced"
fs_table_name_destinations = f"destination_features_advanced"


fs = feature_store.FeatureStoreClient()

model_feature_lookups = [
      FeatureLookup(
          table_name=fs_table_name_destinations,
          lookup_key="destination_id",
          timestamp_lookup_key="ts"
      ),
      FeatureLookup(
          table_name=fs_table_name_users,
          lookup_key="user_id",
          feature_names=["mean_price_7d", "last_6m_purchases", "day_of_week_sin", "day_of_week_cos", "day_of_month_sin", "day_of_month_cos", "hour_sin", "hour_cos"], # if you dont specify here the FS will take all your feature apart from primary_keys 
          timestamp_lookup_key="ts"
      )
]

# fs.create_training_set will look up features in model_feature_lookups with matched key from training_labels_df
training_set = fs.create_training_set(
    training_labels_df, # joining the original Dataset, with our FeatureLookupTable
    feature_lookups=model_feature_lookups,
    exclude_columns=["ts", "destination_id", "user_id"], # exclude id columns as we don't want them as feature
    label='purchased',
)

training_pd = training_set.load_df()
display(training_pd)

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Creating the model using Databricks AutoML 
# MAGIC
# MAGIC Instead of creating a basic model like previously, we will use <a href="https://docs.databricks.com/machine-learning/automl/index.html#classification" target="_blank">Databricks AutoML</a> to train our model, using best practices out of the box.
# MAGIC
# MAGIC While you can do that using the UI directly (+New => AutoML), we'll be using the `databricks.automl` API to have a reproductible flow.
# MAGIC
# MAGIC
# MAGIC After running the previous cell, you will notice two notebooks and an MLflow experiment:
# MAGIC
# MAGIC * **Data exploration notebook**: we can see a Profiling Report which organizes the input columns and discusses values, frequency and other information
# MAGIC * **Best trial notebook**: shows the source code for reproducing the best trial conducted by AutoML
# MAGIC * **MLflow experiment**: contains high level information, such as the root artifact location, experiment ID, and experiment tags. The list of trials contains detailed summaries of each trial, such as the notebook and model location, training parameters, and overall metrics.

# COMMAND ----------

import databricks.automl as db_automl

summary_cl = db_automl.classify(
    training_pd,
    target_col="purchased",
    primary_metric="log_loss",
    timeout_minutes=5,
    experiment_dir="/dbdemos/experiments/feature_store",
)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC #### Get best run from automl MLFlow experiment
# MAGIC
# MAGIC <img src="https://raw.githubusercontent.com/databricks-demos/dbdemos-resources/main/images/product/feature_store/automl_experm.png" alt="step12" width="700" style="float: right; margin-left: 10px" />
# MAGIC
# MAGIC Open the **MLflow experiment** from the link above and explore your best run.
# MAGIC
# MAGIC In a real deployment, we would review the notebook generated and potentially improve it using our domain knowledge before deploying it in production.
# MAGIC
# MAGIC For this Feature Store demo, we'll simply get the best model and deploy it in the registry.

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Saving our best model to MLflow registry
# MAGIC
# MAGIC Next, we'll get Automl best model and add it to our registry. Because we the feature store to keep track of our model & features, we'll log the best model as a new run using the `FeatureStoreClient.log_model()` function.
# MAGIC
# MAGIC **summary_cl** provides the automl information required to automate the deployment process. We'll use it to select our best run and deploy as Production production.
# MAGIC
# MAGIC *Note that another way to find the best run would be to use search_runs function from mlflow API, sorting by our accuracy metric.*

# COMMAND ----------

# creating sample input to be logged
df_sample = training_pd.limit(10).toPandas()
x_sample = df_sample.drop(columns=["purchased"])
y_sample = df_sample["purchased"]

# getting the model created by AutoML 
best_model = summary_cl.best_trial.load_model()

import mlflow
import mlflow.sklearn
from mlflow.models.signature import infer_signature
from mlflow.tracking import MlflowClient

mlflow.set_registry_uri("databricks-uc")

env = mlflow.pyfunc.get_default_conda_env()
with open(mlflow.artifacts.download_artifacts("runs:/"+summary_cl.best_trial.mlflow_run_id+"/model/requirements.txt"), 'r') as f:
    env['dependencies'][-1]['pip'] = f.read().split('\n')

#Create a new run in the same experiment as our automl run.
with mlflow.start_run(run_name="best_fs_model", experiment_id=summary_cl.experiment.experiment_id) as run:
  #Use the feature store client to log our best model
  fs.log_model(
              model=best_model, # object of your model
              artifact_path="model", #name of the Artifact under MlFlow
              flavor=mlflow.sklearn, # flavour of the model (our LightGBM model has a SkLearn Flavour)
              training_set=training_set, # training set you used to train your model with AutoML
              input_example=x_sample, # example of the dataset, should be Pandas
              signature=infer_signature(x_sample, y_sample), # schema of the dataset, not necessary with FS, but nice to have 
              conda_env = env
          )
  mlflow.log_metrics(summary_cl.best_trial.metrics)
  mlflow.log_params(summary_cl.best_trial.params)
  mlflow.set_tag(key='feature_store', value='advanced_demo')
  

model_name_advanced = "rohitb_travel_agency_propensity"
model_registered = mlflow.register_model(f"runs:/{run.info.run_id}/model", model_name_advanced)

# COMMAND ----------

# MAGIC %md ## View ML models in the UI
# MAGIC
# MAGIC You can view ML models in UC in the [Data Explorer](./explore/data?filteredObjectTypes=REGISTERED_MODEL), under the catalog and schema in which the model was created.

# COMMAND ----------

from mlflow import MlflowClient

client = MlflowClient()
client.set_registered_model_alias(name=model_name_advanced, 
                                  alias="Champion", 
                                  version=model_registered.version)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ## 3: Running inference
# MAGIC
# MAGIC <img src="https://raw.githubusercontent.com/databricks-demos/dbdemos-resources/main/images/product/feature_store/feature_store_inference_advanced.png" style="float: right" width="850px" />
# MAGIC
# MAGIC As previously, we can easily leverage the feature store to get our predictions.
# MAGIC
# MAGIC No need to fetch or recompute the feature, we just need the lookup ids and the feature store will automatically fetch them from the feature store table. 

# COMMAND ----------

## For sake of simplicity, we will just predict on the same inference_data_df
batch_scoring = test_labels_df.select('user_id', 'destination_id', 'ts', 'purchased')
scored_df = fs.score_batch(f"models:/{model_name_advanced}@Champion", batch_scoring, result_type="boolean")
display(scored_df)

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ## Share models across workspaces
# MAGIC
# MAGIC As long as you have the appropriate permissions, you can access models in UC from any workspace that you have access to. For example, Data Scientists developing models in a "dev" workspace may lack permissions in a "prod" workspace. Using models in Unity Catalog, they would be able to access models trained in the "prod" workspaces - and registered to the "prod" catalog - from the "dev" workspace. Thus enabling those Data Scientists to compare newly-developed models to the production baseline.
# MAGIC
# MAGIC If you’re not ready to move full production model training or inference pipelines to the private preview UC model registry, you can still leverage UC for cross-workspace model sharing, by registering new model versions to both Unity Catalog and workspace model registries.

# COMMAND ----------

import mlflow
catalog = "rohitb_sandbox"
schema = "travel_agency"
model_name = "propensity"
mlflow.set_registry_uri("databricks-uc")
mlflow.register_model("runs:/f3600c74487549f39cbae8847374efdc/model", f"{catalog}.{schema}.{model_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Enable model inference via the UI
# MAGIC
# MAGIC After calling `log_model`, a new version of the model is saved. To provision a serving endpoint, follow the steps below.
# MAGIC
# MAGIC 1. Within the Machine Learning menu, click **Serving** in the left sidebar. 
# MAGIC 2. Create a new endpoint, select the most recent model version and start the serverless model serving
# MAGIC
# MAGIC <img src="https://raw.githubusercontent.com/databricks-demos/dbdemos-resources/main/images/product/feature_store/notebook4_ff_model_serving_screenshot2_1.png" alt="step12" width="1500"/>

# COMMAND ----------


