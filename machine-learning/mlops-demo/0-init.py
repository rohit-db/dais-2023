# Databricks notebook source
# MAGIC %md 
# MAGIC # End-to-End MLOps demo with MLFlow, Feature Store and Auto ML
# MAGIC
# MAGIC ## Challenges moving ML project into production
# MAGIC
# MAGIC
# MAGIC Moving ML project from a standalone notebook to a production-grade data pipeline is complex and require multiple competencies. 
# MAGIC
# MAGIC Having a model up and running in a notebook isn't enough. We need to cover the end to end ML Project life cycle and solve the following challenges:
# MAGIC
# MAGIC * Update data over time (production-grade ingestion pipeline)
# MAGIC * How to save, share and re-use ML features in the organization
# MAGIC * How to ensure a new model version respect quality standard and won't break the pipeline
# MAGIC * Model governance: what is deployed, how is it trained, by who, which data?
# MAGIC * How to monitor and re-train the model...
# MAGIC
# MAGIC In addition, these project typically invole multiple teams, creating friction and potential silos
# MAGIC
# MAGIC * Data Engineers, in charge of ingesting, preparing and exposing the data
# MAGIC * Data Scientist, expert in data analysis, building ML model
# MAGIC * ML engineers, setuping the ML infrastructure pipelines (similar to devops)
# MAGIC
# MAGIC This has a real impact on the business, slowing down projects and preventing them from being deployed in production and bringing ROI.
# MAGIC
# MAGIC ## What's MLOps ?
# MAGIC
# MAGIC MLOps is is a set of standards, tools, processes and methodology that aims to optimize time, efficiency and quality while ensuring governance in ML projects.
# MAGIC
# MAGIC MLOps orchestrate a project life-cycle and adds the glue required between the component and teams to smoothly implement such ML pipelines.
# MAGIC
# MAGIC
# MAGIC Databricks is uniquely positioned to solve this challenge with the Lakehouse pattern. Not only we bring Data Engineers, Data Scientists and ML Engineers together in a unique platform, but we also provide tools to orchestrate ML project and accelerate the go to production.
# MAGIC
# MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/product_demos/mlops-end2end-flow-0.png" width="1200">
# MAGIC
# MAGIC <!-- Collect usage data (view). Remove it to disable collection. View README for more details.  -->
# MAGIC <img width="1px" src="https://www.google-analytics.com/collect?v=1&gtm=GTM-NKQ8TT7&tid=UA-163989034-1&cid=555&aip=1&t=event&ec=field_demos&ea=display&dp=%2F42_field_demos%2Ffeatures%2Fmlops%2F01_presentation&dt=MLOPS">
# MAGIC <!-- [metadata={"description":"MLOps end2end workflow: workflow presentation & introduction",
# MAGIC  "authors":["quentin.ambard@databricks.com"],
# MAGIC  "db_resources":{},
# MAGIC   "search_tags":{"vertical": "retail", "step": "Data Engineering", "components": ["EDA"]},
# MAGIC                  "canonicalUrl": {"AWS": "", "Azure": "", "GCP": ""}}] -->

# COMMAND ----------

# MAGIC %md 
# MAGIC # Building a propensity score to book travels & hotels
# MAGIC
# MAGIC Fot this demo, we'll step in the shoes of a **Travel Agency offering deals in their website**.
# MAGIC
# MAGIC Our job is to increase our revenue by boosting the amount of purchases, pushing personalized offer based on what our customers are the most likely to buy.
# MAGIC In order to personalize offer recommendation in our application, we have have been asked as a Data Scientist to create the **TraveRecommendationModel** that predicts the probability of purchasing a given travel. 
# MAGIC
# MAGIC For this we'll use a single data source: **Travel Purchased by users**

# COMMAND ----------


