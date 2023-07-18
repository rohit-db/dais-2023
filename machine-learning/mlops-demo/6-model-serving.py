# Databricks notebook source
# MAGIC %md ## Deploy Serverless Model serving Endpoint
# MAGIC
# MAGIC We're now ready to deploy our model using Databricks Model Serving endpoints. This will provide a REST API to serve our model in realtime.

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

# MAGIC %md
# MAGIC ### Set up & start a Serverless model serving endpoint using the API:
# MAGIC
# MAGIC We will use the API to programatically start the endpoint:

# COMMAND ----------

client = mlflow.tracking.MlflowClient()
latest_model = client.get_latest_versions(model_name_expert, stages=["Production"])[0]

#See the 00-init-expert notebook for the endpoint API details
serving_client = EndpointApiClient()

#Start the enpoint using the REST API (you can do it using the UI directly)
serving_client.create_enpoint_if_not_exists("dbdemos_feature_store_endpoint", model_name=model_name_expert, model_version = latest_model.version, workload_size="Small", scale_to_zero_enabled=True, wait_start = True)

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC Your endpoint was created. Open the [Endpoint UI](/#mlflow/endpoints/dbdemos_feature_store_endpoint) to see the creation logs.
# MAGIC
# MAGIC
# MAGIC ### Send payloads via REST call
# MAGIC
# MAGIC With Databricks's Serverless Model Serving, the endpoint takes a different score format.
# MAGIC You can see that users in New York can see high scores for Florida, whereas users in California can see high scores for Hawaii.
# MAGIC
# MAGIC ```
# MAGIC {
# MAGIC   "dataframe_records": [
# MAGIC     {"user_id": 4, "ts": "2022-11-23 00:28:40.053000", "booking_date": "2022-11-23", "destination_id": 16, "user_latitude": 40.71277, "user_longitude": -74.005974}, 
# MAGIC     {"user_id": 39, "ts": "2022-11-23 00:30:29.345000", "booking_date": "2022-11-23", "destination_id": 1, "user_latitude": 37.77493, "user_longitude": -122.41942}
# MAGIC   ]
# MAGIC }
# MAGIC ```

# COMMAND ----------

import timeit

dataset = {
  "dataframe_records": [
    {"user_id": 4, "ts": "2022-11-23 00:28:40.053000", "booking_date": "2022-11-23", "destination_id": 16, "user_latitude": 40.71277, "user_longitude": -74.005974}, 
    {"user_id": 39, "ts": "2022-11-23 00:30:29.345000", "booking_date": "2022-11-23", "destination_id": 1, "user_latitude": 37.77493, "user_longitude": -122.41942}
  ]
}

endpoint_url = f"{serving_client.base_url}/realtime-inference/dbdemos_feature_store_endpoint/invocations"
print(f"Sending requests to {endpoint_url}")
for i in range(3):
    starting_time = timeit.default_timer()
    inferences = requests.post(endpoint_url, json=dataset, headers=serving_client.headers).json()
    print(f"Inference time, end 2 end :{round((timeit.default_timer() - starting_time)*1000)}ms")
    print(inferences)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Sending inference using the UI
# MAGIC
# MAGIC Once your serving endpoint is ready, your previous cell's `score_model` code should give you the model inference result. 
# MAGIC
# MAGIC You can also directly use the model serving UI to try your realtime inference. Click on "Query endpoint" (upper right corner) to test your model. 
# MAGIC <img src="https://raw.githubusercontent.com/databricks-demos/dbdemos-resources/main/images/product/feature_store/notebook4_ff_model_serving_screenshot2_3.png" alt="step12" width="1500"/>

# COMMAND ----------


