# Delta Lake 3.0 Announcements 

[Blog](https://www.databricks.com/blog/announcing-delta-lake-30-new-universal-format-and-liquid-clustering)  

[Session Video](https://www.databricks.com/dataaisummit/session/introducing-universal-format-iceberg-and-hudi-support-delta-lake/)

[Release Notes](https://github.com/delta-io/delta/releases/tag/v3.0.0rc1)

## Delta Universal Format (UniForm)
Delta Universal Format (UniForm) will allow you to read Delta tables with Hudi and Iceberg clients. Iceberg support is available with this preview and Hudi will be coming soon. 
UniForm takes advantage of the fact that all table storage formats (Delta, Iceberg, and Hudi) actually consist of Parquet data files and a metadata layer. In this release, UniForm automatically generates Iceberg metadata, allowing Iceberg clients to read Delta tables as if they were Iceberg tables. Create an UniForm-enabled table using the following command:

```
CREATE TABLE T (c1 INT) USING DELTA SET TBLPROPERTIES (
  'delta.universalFormat.enabledFormats' = 'iceberg');
```

Every write to this table will automatically keep Iceberg metadata updated. See the documentation [here](https://docs.delta.io/3.0.0rc1/delta-uniform.html) for more details.


<img src = "https://cms.databricks.com/sites/default/files/inline-images/image1_5.png">


## Liquid Partitioning
Liquid Clustering, a new effort to revamp how clustering works in Delta, which addresses the shortcomings of Hive-style partitioning and current ZORDER clustering. This feature will be available to preview soon; meanwhile, for more information, please refer to Liquid Clustering [#1874](https://github.com/delta-io/delta/issues/1874).

<img src = "https://cms.databricks.com/sites/default/files/inline-images/image2_3.png">


## Delta Kernel 
The Delta Kernel project is a set of Java libraries (Rust will be coming soon) for building Delta connectors that can read (and soon, write to) Delta tables without the need to understand the [Delta protocol details](https://github.com/delta-io/delta/blob/master/PROTOCOL.md).

You can use this library to do the following:

- Read data from small Delta tables in a single thread in a single process.
- Read data from large Delta tables using multiple threads in a single process.
- Build a complex connector for a distributed processing engine and read very large Delta tables.
- [soon!] Write to Delta tables from multiple threads / processes / distributed engines.

<img src = "https://cms.databricks.com/sites/default/files/inline-images/image4_2.png">

## Delta Spark
Delta Spark 3.0.0 is built on top of Apache Spark™ 3.4.    
Documentation: https://docs.delta.io/3.0.0rc1/


The key features of this release are

- [Delta Universal Format](https://github.com/delta-io/delta/commit/9b50cd206004ae28105846eee9d910f39019ab8b). Write as Delta, read as Iceberg! 
- [Up to 2x faster MERGE operation]() . MERGE now better leverages data skipping, the ability to use the insert-only code path in more cases, and an overall improved execution to achieve up to 2x better performance in various scenarios.
- [Performance of DELETE using Deletion Vectors improved by more than 2x](). This fix improves the file path canonicalization logic by avoiding calling expensive Path.toUri.toString calls for each row in a table, resulting in a several hundred percent speed boost on DELETE operations (only when Deletion Vectors have been enabled on the table).
- [Support streaming reads from column mapping enabled tables]() when DROP COLUMN and RENAME COLUMN have been used. This includes streaming support for Change Data Feed. See the documentation here for more details.
- S[upport specifying the columns for which Delta will collect file-skipping statistics via the table property delta]().dataSkippingStatsColumns. Previously, Delta would only collect file-skipping statistics for the first N columns in the table schema (default to 32). Now, users can easily customize this.
- [Support zero-copy convert to Delta from Iceberg tables]() on Apache Spark 3.4 using CONVERT TO DELTA. This feature was excluded from the Delta Lake 2.4 release since Iceberg did not yet support Apache Spark 3.4. This command generates a Delta table in the same location and does not rewrite any parquet files.

## Delta Flink

Delta-Flink 3.0.0 is built on top of Apache Flink™ 1.16.1.

- Documentation: https://github.com/delta-io/delta/blob/branch-3.0/connectors/flink/README.md
- Maven artifacts: delta-flink   
The key features of this release are

- Support for Flink SQL and Catalog. You can now use the Flink/Delta connector for Flink SQL jobs. You can CREATE Delta tables, SELECT data from them (uses the Delta Source), and INSERT new data into them (uses the Delta Sink). Note: for correct operations on the Delta tables, you must first configure the Delta Catalog using CREATE CATALOG before running a SQL command on Delta tables. For more information, please see the documentation here.
- Significant performance improvement to Global Committer initialization. The last-successfully-committed delta version by a given Flink application is now loaded lazily, significantly reducing the CPU utilization in the most common scenarios.

## Delta Standalone

- Documentation: https://docs.delta.io/3.0.0rc1/delta-standalone.html
- Maven artifacts: delta-standalone_2.12, delta-standalone_2.13   
The key features in this release are:

- Support for disabling Delta checkpointing during commits. For very large tables with millions of files, performing Delta checkpoints can become an expensive overhead during writes. Users can now disable this checkpointing by setting the hadoop configuration property io.delta.standalone.checkpointing.enabled to false. This is only safe and suggested to do if another job will periodically perform the checkpointing.
- Performance improvement to snapshot initialization. When a delta table is loaded at a particular version, the snapshot must contain, at a minimum, the latest protocol and metadata. This PR improves the snapshot load performance for repeated table changes.
- Support adding absolute paths to the Delta log. This now enables users to manually perform SHALLOW CLONEs and create Delta tables with external files.
- Fix in schema evolution to prevent adding non-nullable columns to existing Delta tables
- Dropped support for Scala 2.11. Due to lack to community demand and very low number of downloads, we have dropped Scala 2.11 support.