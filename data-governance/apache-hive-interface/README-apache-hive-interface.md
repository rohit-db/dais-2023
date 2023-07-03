# Unity Catalog HMS Interface

Unity Catalog expands lakehouse governance with an engine-agnostic Hive Metastore Interface, allowing you to seamlessly connect tools like EMR, OSS Spark, Trino and Athena.  This interface securely supports both reading and writing into your data lake through Unity Catalog.


## Limitations
- DDL (e.g creating new tables) is not currently supported and will be available in a future release.
- Concurrent writes from OSS Spark clusters and DBR to S3 Delta tables can lead to data loss due to S3's lack of mutual exclusion. Support for concurrent writes will be available in a future release.
- Partition support is limited on non-delta formats. The partition support relies on Spark's built-in capability to use the filesystem as the source of truth for partitions. Commands which manipulate partitions are not supported.


