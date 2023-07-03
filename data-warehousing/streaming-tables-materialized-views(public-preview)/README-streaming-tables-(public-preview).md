# Streaming Tables in Databricks SQL 

[Blog]()

[Summit Video]()

Streaming tables and materialized views empower SQL analysts with data engineering best practices. Consider an example of continuously ingesting newly arrived files from an S3 location and preparing a simple reporting table. 
With Databricks SQL the analyst can quickly discover and preview the files in S3 and set up a simple ETL pipeline in minutes, using only a few lines of code.


## Benefits of streaming tables:

- **Unlock real-time use cases**. Ability to support real-time analytics/BI, machine learning, and operational use cases with streaming data.
- **Better scalability**. More efficiently handle high volumes of data via incremental processing vs large batches.
- **Enable more practitioners**. Simple SQL syntax makes data streaming accessible to all data engineers and analysts.

### Streaming Tables
<img src="https://cms.databricks.com/sites/default/files/inline-images/db-675-blog-img-1.png">


## Benefits of materialized views:

- **Accelerate BI dashboards**. Because MVs precompute data, end users' queries are much faster because they don’t have to re-process the data by querying the base tables directly.
- **Reduce data processing costs**. MVs results are refreshed incrementally avoiding the need to completely rebuild the view when new data arrives.
- **Improve data access control for secure sharing**. More tightly govern what data can be seen by consumers by controlling access to base tables.

### Materialized Views
<img src="https://cms.databricks.com/sites/default/files/inline-images/db-675-blog-img-2.png">