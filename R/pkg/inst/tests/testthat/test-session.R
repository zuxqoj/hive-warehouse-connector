#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

TEST_USER <- "userX"
TEST_PASSWORD <- "passwordX"
TEST_HS2_URL <- "jdbc:hive2://nohost:10084"
TEST_DBCP2_CONF <- "defaultQueryTimeout=100"
TEST_EXEC_RESULTS_MAX <- "12345"
TEST_DEFAULT_DB <- "default12345"

# getwd() is ./R/lib/inst/tests/testthat
root <- normalizePath(paste0(getwd(), "/../../../../../target"))
basepath <- Sys.glob(file.path(root, "scala-*"))
if (length(basepath) == 0) {
  stop("Build the package first. ./target/scala-* directory was not found.")
}
basepath <- basepath[[1]]

jarpath <- Sys.glob(file.path(basepath, "/hive-warehouse-connector-assembly-*"))
if (length(jarpath) != 1) {
  stop(paste0("Multiple assemply jars were detected or no jar found by ",
              "./target/scala-*/hive-warehouse-connector-assembly-*. ",
              "Please clean up and build again by 'sbt assembly'."))
}
jarpath <- jarpath[[1]]

testjarpath <- Sys.glob(file.path(basepath, "/hive-warehouse-connector*tests.jar"))
if (length(testjarpath) != 1) {
  stop(paste0("Multiple test:package jars were detected or no jar found by ",
              "./target/scala-*/hive-warehouse-connector*tests.jar. ",
              "Please clean up and build again by 'sbt test:package'."))
}
testjarpath <- testjarpath[[1]]

jarpaths <- paste0(jarpath, ":", testjarpath)
sparkSession <- sparkR.session("local[4]", "SparkR", Sys.getenv("SPARK_HOME"),
                               list(spark.driver.extraClassPath = jarpaths,
                               spark.executor.extraClassPath = jarpaths),
                               enableHiveSupport = FALSE)

tryCatch({
  sparkR.newJObject("com.hortonworks.spark.sql.hive.llap.MockConnection")
}, error = function(e) {
  stop(paste0("SparkRHWC tests are dependent on mock classes defined in test ",
              "codes. These should be compiled together, for example, by ",
              "'sbt test:package'."))
})

confPairs <- new.env()
confPairs$spark.datasource.hive.warehouse.password <- TEST_PASSWORD
confPairs$spark.datasource.hive.warehouse.dbcp2.conf <- TEST_DBCP2_CONF
confPairs$spark.datasource.hive.warehouse.default.db <- TEST_DEFAULT_DB
confPairs$spark.datasource.hive.warehouse.user.name <- TEST_USER
confPairs$spark.datasource.hive.warehouse.exec.results.max <- TEST_EXEC_RESULTS_MAX

hwcbuilder <- HiveWarehouseBuilder.session(sparkSession)
hwcbuilder <- userPassword(hwcbuilder, TEST_USER, TEST_PASSWORD)
hwcbuilder <- hs2url(hwcbuilder, TEST_HS2_URL)
hwcbuilder <- dbcp2Conf(hwcbuilder, TEST_DBCP2_CONF)
hwcbuilder <- maxExecResults(hwcbuilder, as.integer(TEST_EXEC_RESULTS_MAX))
hwcbuilder <- defaultDB(hwcbuilder, TEST_DEFAULT_DB)
jstate <- sparkR.callJMethod(hwcbuilder@jhwbuilder, "sessionStateForTest")
jhwsession <- sparkR.newJObject(
  "com.hortonworks.spark.sql.hive.llap.MockHiveWarehouseSessionImpl", jstate)
hive <- new("HiveWarehouseSessionImpl", sparkSession, jhwsession)
mockExecuteResultSize <- sparkR.callJMethod(
  sparkR.callJMethod(
    sparkR.callJStatic(
      "com.hortonworks.spark.sql.hive.llap.MockHiveWarehouseSessionImpl", "testFixture"),
    "getData"),
  "size")
RESULT_SIZE <- 10


context("Hive Warehouse Connector - SQL execution")

test_that("Test executeQuery", {
  expect_equal(count(executeQuery(hive, "SELECT * FROM t1")), RESULT_SIZE)
})

test_that("Test executeUpdate", {
  expect_true(executeUpdate(hive, "SELECT * FROM t1"))
})

test_that("Test setDatabase", {
  setDatabase(hive, TEST_DEFAULT_DB)
})

test_that("Test describeTable", {
  expect_equal(count(describeTable(hive, "testTable")), mockExecuteResultSize)
})

test_that("Test createDatabase", {
  createDatabase(hive, TEST_DEFAULT_DB, FALSE)
  createDatabase(hive, TEST_DEFAULT_DB, TRUE)
})

test_that("Test showTable", {
  expect_equal(count(showTables(hive)), mockExecuteResultSize)
})

test_that("Test createTable", {
  tablebuilder <- new("CreateTableBuilder",
                      sparkSession,
                      createTable(hive, "TestTable")@jtablebuilder)
  tablebuilder <- ifNotExists(tablebuilder)
  tablebuilder <- column(tablebuilder, "id", "int")
  tablebuilder <- column(tablebuilder, "val", "string")
  tablebuilder <- partition(tablebuilder, "id", "int")
  tablebuilder <- clusterBy(tablebuilder, 100, "val")
  tablebuilder <- prop(tablebuilder, "key", "value")
  create(tablebuilder)
})


context("Hive Warehouse Connector - session build")

test_that("Test all HWC builder configurations", {
  hwcbuilder <- HiveWarehouseBuilder.session(sparkSession)
  hwcbuilder <- userPassword(hwcbuilder, TEST_USER, TEST_PASSWORD)
  hwcbuilder <- hs2url(hwcbuilder, TEST_HS2_URL)
  hwcbuilder <- dbcp2Conf(hwcbuilder, TEST_DBCP2_CONF)
  hwcbuilder <- maxExecResults(hwcbuilder, as.integer(TEST_EXEC_RESULTS_MAX))
  hwcbuilder <- defaultDB(hwcbuilder, TEST_DEFAULT_DB)
  jstate <- sparkR.callJMethod(hwcbuilder@jhwbuilder, "sessionStateForTest")
  hive <- sparkR.newJObject("com.hortonworks.spark.sql.hive.llap.MockHiveWarehouseSessionImpl",
                            jstate)
  properties <- sparkR.callJMethod(sparkR.callJMethod(hive, "sessionState"), "getProps")

  expect_equal(properties, confPairs)
})


test_that("Test HWC session build", {
  expect_false(is.null(build(HiveWarehouseBuilder.session(sparkSession))))
})


test_that("Test all configurations via Spark Session", {
  sparkR.session.stop()
  pairs <- as.list(confPairs)

  tryCatch({
    sparkSession <- sparkR.session("local[4]",
                                   "SparkR",
                                   Sys.getenv("SPARK_HOME"),
                                   pairs,
                                   enableHiveSupport = FALSE)
    for (name in names(pairs)) {
      expect_equal(sparkR.conf(name)[[1]], pairs[[name]])
    }
  }, finally = {
    sparkR.session.stop()
    sparkSession <- sparkR.session("local[4]", "SparkR", Sys.getenv("SPARK_HOME"),
                                   list(spark.driver.extraClassPath = jarpaths,
                                        spark.executor.extraClassPath = jarpaths),
                                   enableHiveSupport = FALSE)
  })
})

test_that("Test new entry point", {
  sparkR.session.stop()
  HIVESERVER2_JDBC_URL <- "spark.sql.hive.hiveserver2.jdbc.url"

  tryCatch({
    sparkSession <- sparkR.session("local[4]", "SparkR", Sys.getenv("SPARK_HOME"),
                                   list(spark.driver.extraClassPath = jarpaths,
                                        spark.executor.extraClassPath = jarpaths,
                                        spark.sql.hive.hiveserver2.jdbc.url="test"),
                                   enableHiveSupport = FALSE)

    hwcbuilder <- HiveWarehouseSession.session(sparkSession)
    hwcbuilder <- userPassword(hwcbuilder, TEST_USER, TEST_PASSWORD)
    hwcbuilder <- hs2url(hwcbuilder, TEST_HS2_URL)
    hwcbuilder <- dbcp2Conf(hwcbuilder, TEST_DBCP2_CONF)
    hwcbuilder <- maxExecResults(hwcbuilder, as.integer(TEST_EXEC_RESULTS_MAX))
    hwcbuilder <- defaultDB(hwcbuilder, TEST_DEFAULT_DB)
    hwcsession <- build(hwcbuilder)
    expect_equal(session(hwcsession), sparkSession)
  }, finally = {
    sparkR.session.stop()
    sparkSession <- sparkR.session("local[4]", "SparkR", Sys.getenv("SPARK_HOME"),
                                   list(spark.driver.extraClassPath = jarpaths,
                                        spark.executor.extraClassPath = jarpaths),
                                   enableHiveSupport = FALSE)
  })
})

sparkR.session.stop()
