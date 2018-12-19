# Hive Warehouse Connector with R on Apache Spark

SparkRHWC is an R package that provides a light-weight frontend to use Hive Warehouse Connector (HWC) in Apache Spark via R.

### Building SparkRHWC

This process is included in HWC's regular SBT build. So, for instance, you can build HWC as below:

```bash
$ ./build/sbt assembly -DskipTests -Dversion=1.0.0.3.1.0.0-78 -Dspark.version=2.3.2.3.1.0.0-78 -Dhadoop.version=3.1.1.3.1.0.0-78 -Dhive.version=3.1.0.3.1.0.0-78 -Dtez.version=0.9.1.3.1.0.0-78
```

The build output is as below:

```bash
target
│   # A regular SparkRHWC library so that RStudio users can manually import and use.
├── SparkRHWC-1.0.0.3.1.0.0-78
│   └── SparkRHWC
│           ├── DESCRIPTION
│           ...
│   # A zipped archive that contains Python API.
├── pyspark_hwc-1.0.0.3.1.0.0-78.zip
├── scala-2.11
│   │   # A jar that contains Scala and Java API, and also R package source. See below for more details.
│   ├── hive-warehouse-connector-assembly-1.0.0.3.1.0.0-78.jar
...
```

In more details, SparkRHWC packages R source together so that Spark submit and Spark shell automatically handle (see also [RPackageUtils#L45-L65](https://github.com/apache/spark/blob/5264164a67df498b73facae207eda12ee133be7d/core/src/main/scala/org/apache/spark/deploy/RPackageUtils.scala#L45-L65)). Therefore, if you take a look for the jar, for instnace as below:

```
jar tf hive-warehouse-connector-assembly-1.0.0.3.1.0.0-78.jar | sort | more
```

It contains R source as below

```
META-INF
...
R/
R/pkg/
R/pkg/DESCRIPTION
R/pkg/NAMESPACE
R/pkg/R/
R/pkg/R/session.R
...
```

and `META-INF/MANIFEST.MF` contains the property as below:

```
Spark-HasRPackage: true
```

**Note:** this SBT build process includes some steps to create libraries of SparkRHWC under `./R/lib`. This also can be done by manually running the script `./R/install-dev.sh`.
By default, the above script uses the system wide installation of R. However, this can be changed to any user installed location of R by setting the environment variable `R_HOME`, the full path of the base directory where R is installed, before running `install-dev.sh` script.

### Running SparkRHWC Tests

Tests can be ran by running `./run-tests.sh` script under `./R` directory. Before running this script, firstly, SparkRHWC should be built via the regular SBT build procedure above.

Secondly, the test classes in HWC should be compiled because SparkRHWC accesses to JVM and uses the classes defined in tests. This can be done, for instance, as below:

```bash
$ ./build/sbt test:package -DskipTests -Dversion=1.0.0.3.1.0.0-78 -Dspark.version=2.3.2.3.1.0.0-78 -Dhadoop.version=3.1.1.3.1.0.0-78 -Dhive.version=3.1.0.3.1.0.0-78 -Dtez.version=0.9.1.3.1.0.0-78
```

Lastly, `SPARK_HOME` must be set. Therefore, the tests can be ran as below:

```bash
$ cd R
$ SPARK_HOME=/.../spark ./run-tests.sh
```


### Using SparkRHWC from Spark Submit and SparkR Shell

It does not require additional steps for end users rather than adding HWC's jar because SparkRHWC library is included in the jar, and it is automatically installed for Spark submit and SparkR shell.

For instance, this can be used as below:

```bash 
$ ./bin/sparkr --jars /.../hive-warehouse-connector-assembly-1.0.0.3.1.0.0-78.jar
```

```
R version 3.5.1 (2018-07-02) -- "Feather Spray"
Copyright (C) 2018 The R Foundation for Statistical Computing
Platform: x86_64-apple-darwin15.6.0 (64-bit)
...
Launching java with spark-submit command /.../spark/bin/spark-submit   "--jars" "/.../hive-warehouse-connector-assembly-1.0.0.3.1.0.0-78.jar" 
"sparkr-shell" /.../hive-warehouse-connector-assembly-1.0.0.3.1.0.0-78.jar contains R source code. 
Now installing package.
* installing *source* package ‘SparkRHWC’ ...
...
* DONE (SparkRHWC)
... 
SparkSession available as 'spark'.
> library(SparkRHWC)
```

### Using SparkRHWC from RStudio

If you wish to use SparkRHWC from RStudio or other R frontends you will need to set `SPARK_HOME` environment variable which points SparkRHWC to your Spark installation. For example

```R
if (nchar(Sys.getenv("SPARK_HOME")) < 1) {
  stop("SPARK_HOME is not set, existing.")
}
library(SparkR, lib.loc = c(file.path(Sys.getenv("SPARK_HOME"), "R", "lib")))
library(SparkRHWC, lib.loc = c(file.path("/.../hive-warehouse-connector", "SparkRHWC-1.0.0.3.1.0.0-78")))
hive <- build(HiveWarehouseBuilder.session(sparkR.session()))
```

### API differences between Scala, Java, Python and R APIs.

API usages are virtually same across languages except few differences in R API due to the nature of language, and to be consistent with R API in Apache Spark. See the summary below.

1.. Unlike the builder pattern from other language APIs, the instance is always placed as its first argument. See the differences as below in other language APIs.
  
```scala
// Scala
import com.hortonworks.hwc.HiveWarehouseSession

val hive = HiveWarehouseSession.session(session)
  .userPassword("user", "password")
  .defaultDB("default")
  .build()
```

```python
# Python
from pyspark_llap.sql.session import HiveWarehouseSession

hive = HiveWarehouseSession.session(session) \
    .userPassword("user", "password") \
    .defaultDB("default") \
    .build()
```

```R
# R
library(SparkRHWC)

hwcbuilder <- HiveWarehouseSession.session(session)
hwcbuilder <- userPassword(hwcbuilder, "user", "password")
hwcbuilder <- defaultDB(hwcbuilder, "default")
hive <- build(hwcbuilder)
```

2.. Other methods belonging to instances also require to be placed as the first argument of each API call. For example, see the differences below.

```scala
// Scala
val df = hive.executeQuery("SELECT * FROM t1")
```

```python
# Python
df = hive.executeQuery("SELECT * FROM t1")
```

```R
# R
df <- executeQuery(hive, "SELECT * FROM t1")
```

3.. Static methods can be called in a consistent manner with other APIs. For example, see below:

```R
# R
> HiveWarehouseSession.session(spark)
An object of class "HiveWarehouseBuilder"
Slot "jsession":
Java ref type org.apache.spark.sql.SparkSession id 1

Slot "jhwbuilder":
Java ref type com.hortonworks.spark.sql.hive.llap.HiveWarehouseBuilder id 2

> HiveWarehouseSession.HIVE_WAREHOUSE_CONNECTOR
[1] "com.hortonworks.spark.sql.hive.llap.HiveWarehouseConnector"
> HiveWarehouseSession.DATAFRAME_TO_STREAM
[1] "com.hortonworks.spark.sql.hive.llap.HiveStreamingDataSource"
> HiveWarehouseSession.STREAM_TO_STREAM
[1] "com.hortonworks.spark.sql.hive.llap.streaming.HiveStreamingDataSource"
```

4.. R API does not have `q` alias for `executeQuery` becuase `q` function is a built-in function that terminates the R interpreter session.

