# System Tables Announcements 


# Introduction to Databricks System Tables 

<img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/uc/system_tables/uc-system-tables-explorer.png?raw=true" style="float: right; margin: 10px 0px 0px 20px" width="700px" />

System Tables are a Databricks-hosted analytical store for operational and usage data. 

System Tables can be used for monitoring, analyzing performance, usage, and behavior of Databricks Platform components. By querying these tables, users can gain insights into how their jobs, notebooks, users, clusters, ML endpoints, and SQL warehouses are functioning and changing over time. This historical data can be used to optimize performance, troubleshoot issues, track usage patterns, and make data-driven decisions.

Overall, System Tables provide a means to enhance observability and gain valuable insights into the operational aspects of Databricks usage, enabling users to better understand and manage their workflows and resources.
- Cost and usage analytics 
- Efficiency analytics 
- Audit analytics 
- GDPR regulation
- Service Level Objective analytics 
- Data Quality analytics 

## Accessing your System tables With Unity Catalog 

System Tables are available to customers who have Unity Catalog activated in at least one workspace. The data provided is collected from all workspaces in a Databricks account, regardless of the workspace's status with Unity Catalog. For example, if I have 10 workspaces and only one of them have Unity Catalog enabled then data is collected for all the workspaces and is made available via the single workspace in which Unity Catalog is active. 



%md-sandbox
## System Table Dashboard - Leverage AI with Lakehouse

<img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/uc/system_tables/dashboard-governance-billing.png?raw=true" width="600px" style="float:right">

We installed a Dashboard to track your billing and Unity Catalog usage leveraging the System tables.

[Open the dashboard](/sql/dashboards/ab3b5298-e09e-4998-8ef3-ae456a7b888d) to review the informations available for you.<br/><br/>

### A note on Forecasting billing usage

Please note that this dashboard forecasts your usage to predict your future spend and trigger potential alerts.

To do so, we train multiple ML models leveraging `prophet` (the timeseries forecasting library). <br/>
**Make sure you run the `01-billing-tables/02-forecast-billing-tables` notebook to generate the forecast data.** <br/>
If you don't, data won't be available in the dashboard. `dbdemos` started a job in the background to initialize this data, but you can also directly run the notebook. 

*For production-grade tracking, make sure you run your forecasting notebook as a job every day.*


# Demo

```
%pip install dbdemos
```

```
import dbdemos
dbdemos.install('uc-04-system-tables')
```

Dbdemos is a Python library that installs complete Databricks demos in your workspaces.

Dbdemos will load and start notebooks, Delta Live Tables pipelines, clusters, Databricks SQL dashboards, warehouse models … See how to use dbdemos

**Databricks SQL Dashboard - Billing Forecast**
<img src="https://www.databricks.com/en-website-assets/static/d30ca18898b46615016f7e1d969912fc/71f6f/cost-management-and-forecasting-1.webp"

**Databricks SQL Dashboard - Schema & Audit**
<img src="https://www.databricks.com/en-website-assets/static/60dc353185ec6f05134c65b0a01f8314/27785/cost-management-and-foreasting-2.webp">
