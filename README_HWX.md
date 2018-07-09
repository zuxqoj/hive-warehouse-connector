
# HiveWarehouseConnector

A library to read/write Apache Spark&trade; DataFrames and Streaming DataFrames to/from
Apache Hive&trade; + LLAP.
With Apache Ranger&trade;, this library provides row/column level fine-grained access controls.

Requirements
=====
1. HDP3
2. Hive with _HiveServer2 Interactive_
3. Spark2

Configuration
=====
Add the following Spark properties via `Custom spark2-defaults` in Ambari.
Alternatively, configuration can be provided for each job using `--conf`.

| Property      | Description   |  Note |
| ------------- |:-------------:|-----:|
| `spark.sql.hive.hiveserver2.jdbc.url` | URL for HiveServer2 Interactive | Copy value from _Hive Summary_ `HIVESERVER2 INTERACTIVE JDBC URL` |
| `spark.datasource.hive.warehouse.metastoreUri` | URI for Metastore | Copy value from `hive.metastore.uris` |
| `spark.datasource.hive.warehouse.load.staging.dir` | HDFS temp directory for batch writes to Hive | e.g. `/tmp` |
| `spark.hadoop.hive.llap.daemon.service.hosts` | App name for LLAP service | Copy value from _Advanced hive-interactive-site_ `hive.llap.daemon.service.hosts` |
| `spark.hadoop.hive.zookeeper.quorum` | Zookeeper hosts used by LLAP | Copy value from _Advanced hive-site_ `hive.zookeeper.quorum` |

For use in Spark client-mode on kerberized Yarn cluster, set:

| Property      | Description   | Note  |
| ------------- |:-------------:| -----:|
| spark.sql.hive.hiveserver2.jdbc.url.principal | Kerberos principal for HiveServer2 Interactive  | Copy from _Advanced hive-site_ `hive.server2.authentication.kerberos.principal` |

For use in Spark cluster-mode on kerberized Yarn cluster, set:

| Property      | Description   | Note  |
| ------------- |:-------------:| -----:|
| spark.security.credentials.hiveserver2.enabled | Only enable for kerberized cluster-mode | true by default |

Submitting Applications
=====
Support is currently available for `spark-shell`, `pyspark`, and `spark-submit`.

Scala/Java usage:
-----

1. Locate the `hive-warehouse-connector-assembly` jar in `/usr/hdp/current/hive_warehouse_connector/`
2. Use `--jars` to add the connector jar to app submission, e.g.

`spark-shell --jars /usr/hdp/current/hive-warehouse-connector/hive-warehouse-connector-assembly-<version>.jar`

Python usage:
-----

1. Follow the instructions above to add the connector jar to app submission.
2. Locate the `pyspark_hwc` zip package in `/usr/hdp/current/hive_warehouse_connector/`
2. Additionally add the Python package to app submission, e.g.

`pyspark --jars /usr/hdp/current/hive-warehouse-connector/hive-warehouse-connector-assembly-<version>.jar
         --py-files /usr/hdp/current/hive-warehouse-connector/pyspark_hwc-<version>.zip`

API Usage
=====

`HiveWarehouseSession` acts as an API to bridge Spark with Hive.
In your Spark source code, create an instance of `HiveWarehouseSession`.
Use the language-specific code to create `HiveWarehouseSession`, other operations below are the same for each language.

Assuming `spark` is an existing `SparkSession`:

Scala:
```
import com.hortonworks.hwc.HiveWarehouseSession
import com.hortonworks.hwc.HiveWarehouseSession._
val hive = HiveWarehouseSession.session(spark).build()
```
Java:
```
import com.hortonworks.hwc.HiveWarehouseSession;
import static com.hortonworks.hwc.HiveWarehouseSession.*;
HiveWarehouseSession hive = HiveWarehouseSession.session(spark).build();
```
Python:
```
from pyspark_llap import HiveWarehouseSession
hive = HiveWarehouseSession.session(spark).build()
```

Catalog Operations
------------------

* Set the current database for unqualified Hive table references:

`hive.setDatabase(<database>)`

* Execute catalog operation and return DataFrame, e.g.

`hive.execute("describe extended web_sales").show(100, false)`

Note: `hive.execute` does not support SELECT queries,
use `hive.executeQuery` instead.

* Show databases:

`hive.showDatabases().show(100, false)`

* Show tables for current database:

`hive.showTables().show(100, false)`

* Describe table:

`hive.describeTable(<table_name>).show(100, false)`

* Create a database:

`hive.createDatabase(<database_name>)`

* Create ORC table, e.g.:

`hive.createTable("web_sales")
     .ifNotExists()
     .column("sold_time_sk", "bigint")
     .column("ws_ship_date_sk", "bigint")
     .create()`

See below `CreateTableBuilder` interface for additional table creation options.
Note: You can also create tables through standard HiveQL using `hive.executeUpdate`

* Drop a database:

`hive.dropDatabase(<databaseName>, <ifExists>, <useCascade>)`

* Drop a table:

`hive.dropTable(<tableName>, <ifExists>, <usePurge>)`

Read Operations
---------------

* Execute Hive SELECT query and return a DataFrame, e.g.

`hive.executeQuery("select * from web_sales")`

Write Operations
----------------

* Execute Hive update statement, e.g.

`hive.executeUpdate("ALTER TABLE old_name RENAME TO new_name")`

Note: This can also be used for CREATE, UPDATE, DELETE, INSERT, and MERGE statements.

* Write a DataFrame to Hive in batch (uses Hive's LOAD DATA INTO TABLE), e.g.

`df.write.format(HIVE_WAREHOUSE_CONNECTOR)
   .option("table", <tableName>)
   .save()`

* Write a DataFrame to Hive using HiveStreaming, e.g.

```
 df.write.format(DATAFRAME_TO_STREAM)
   .option("table", <tableName>)
   .save()

 // To write to static partition
 df.write.format(DATAFRAME_TO_STREAM)
   .option("table", <tableName>)
   .option("partition", <partition>)
   .save()
```

* Write a Spark Stream to Hive using HiveStreaming, e.g.
```
stream.writeStream
    .format(STREAM_TO_STREAM)
    .option("table", "web_sales")
    .start()
```

ETL Example
====
Read table data from Hive, transform in Spark, write to new Hive table
```
    import com.hortonworks.hwc.HiveWarehouseSession
    import com.hortonworks.hwc.HiveWarehouseSession._
	val hive = HiveWarehouseSession.session(spark).build()
	hive.setDatabase("tpcds_bin_partitioned_orc_1000")
	val df = hive.executeQuery("select * from web_sales")
	df.createOrReplaceTempView("web_sales")
	hive.setDatabase("testDatabase")
	hive.createTable("newTable")
	  .ifNotExists()
	  .column("ws_sold_time_sk", "bigint")
	  .column("ws_ship_date_sk", "bigint")
	  .create()
	sql("SELECT ws_sold_time_sk, ws_ship_date_sk FROM web_sales WHERE ws_sold_time_sk > 80000)
	  .write.format(HIVE_WAREHOUSE_CONNECTOR)
	  .option("table", "newTable")
	  .save()
```

Supported Types
=====
| Spark Type      | Hive Type               |
| --------------- | ----------------------- |
| ByteType        | TinyInt                 |
| ShortType       | SmallInt                |
| IntegerType     | Integer                 |
| LongType        | BigInt                  |
| FloatType       | Float                   |
| DoubleType      | Double                  |
| DecimalType     | Decimal                 |
| StringType\*    | String, Varchar\* |
| BinaryType      | Binary                  |
| BooleanType     | Boolean                 |
| TimestampType\* | Timestamp\*             |
| DateType        | Date                    |
| ArrayType       | Array                   |
| StructType      | Struct                  |

- A Hive String or Varchar column will be converted into a Spark StringType column.
- When a Spark StringType column has maxLength metadata, it will be converted into a Hive Varchar column. Otherwise, it will be converted into a Hive String column.
- A Hive Timestamp column will lose sub-microsecond precision when it is converted into a Spark TimestampType column. Because a Spark TimestampType column is microsecond precision, while a Hive Timestamp column is nanosecond precision.

Not currently supported types
-----
| Spark Type           | Hive Type |
| -------------------- | --------- |
| CalendarIntervalType | Interval  |
| MapType              | Map       |
| N/A                  | Union     |
| NullType             | N/A       |
| N/A                  | Char      |

HiveWarehouseSession Interface
==============================
```
    package com.hortonworks.hwc;

	public interface HiveWarehouseSession {

        //Execute Hive SELECT query and return DataFrame
	    Dataset<Row> executeQuery(String sql);

        //Execute Hive update statement
	    boolean executeUpdate(String sql);

        //Execute Hive catalog-browsing operation and return DataFrame
	    Dataset<Row> execute(String sql);

        //Reference a Hive table as a DataFrame
	    Dataset<Row> table(String sql);

        //Return the SparkSession attached to this HiveWarehouseSession
	    SparkSession session();

        //Set the current database for unqualified Hive table references
	    void setDatabase(String name);

        /**
         * Helpers: wrapper functions over execute or executeUpdate
         */

        //Helper for show databases
	    Dataset<Row> showDatabases();

        //Helper for show tables
	    Dataset<Row> showTables();

        //Helper for describeTable
	    Dataset<Row> describeTable(String table);

        //Helper for create database
	    void createDatabase(String database, boolean ifNotExists);

        //Helper for create table stored as ORC
	    CreateTableBuilder createTable(String tableName);

        //Helper for drop database
	    void dropDatabase(String database, boolean ifExists, boolean cascade);

        //Helper for drop table
	    void dropTable(String table, boolean ifExists, boolean purge);
	}
```

CreateTableBuilder Interface
==============================
```
    package com.hortonworks.hwc;

    public interface CreateTableBuilder {

      //Silently skip table creation if table name exists
      CreateTableBuilder ifNotExists();

      //Add a column with the specific name and Hive type
      //Use more than once to add multiple columns
      CreateTableBuilder column(String name, String type);

      //Specific a column as table partition
      //Use more than once to specify multiple partitions
      CreateTableBuilder partition(String name, String type);

      //Add a table property
      //Use more than once to add multiple properties
      CreateTableBuilder prop(String key, String value);

      //Make table bucketed, with given number of buckets and bucket columns
      CreateTableBuilder clusterBy(long numBuckets, String ... columns);

      //Creates ORC table in Hive from builder instance
      void create();
    }
```
Zeppelin Configuration
==============================
HiveWarehouseConnector can be used in Zeppelin notebooks using the `spark2` interpreter.
Modify or add the following properties to your `spark2` interpreter settings
(see the section "Modifying Interpreter Settings" in the HDP _Apache Zeppelin Component Guide_)

| Property        | Description             |
| --------------- | ----------------------- |
| spark.jars       | /usr/hdp/current/hive-warehouse-connector/hive-warehouse-connector-assembly-`<version>`.jar |
| spark.submit.pyfiles | /usr/hdp/current/hive-warehouse-connector/pyspark_hwc-`<version>`.zip |
| spark.hadoop.hive.llap.daemon.service.hosts | See section on HiveWarehouseConnector Configuration |
| spark.sql.hive.hiveserver2.jdbc.url | See section on HiveWarehouseConnector Configuration  |
| spark.yarn.security.credentials.hiveserver2.enabled | See section on HiveWarehouseConnector Configuration  |
| spark.sql.hive.hiveserver2.jdbc.url.principal | See section on HiveWarehouseConnector Configuration  |
| spark.hadoop.hive.zookeeper.quorum | See section on HiveWarehouseConnector Configuration  |
