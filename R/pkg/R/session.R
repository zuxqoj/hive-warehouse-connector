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

requireNamespace("SparkR")

HiveWarehouseSession.HIVE_WAREHOUSE_CONNECTOR <-
  "com.hortonworks.spark.sql.hive.llap.HiveWarehouseConnector"
HiveWarehouseSession.DATAFRAME_TO_STREAM <-
  "com.hortonworks.spark.sql.hive.llap.HiveStreamingDataSource"
HiveWarehouseSession.STREAM_TO_STREAM <-
  "com.hortonworks.spark.sql.hive.llap.streaming.HiveStreamingDataSource"


# HiveWarehouseSession and HiveWarehouseBuilder class and methods implemented in S4 OO classes
HiveWarehouseBuilder <- setClass("HiveWarehouseBuilder",
                                 slots = list(jsession = "jobj", jhwbuilder = "jobj"))
HiveWarehouseSession <- HiveWarehouseBuilder

HiveWarehouseBuilder.session <- function(jsession) { new("HiveWarehouseBuilder", jsession) }
HiveWarehouseSession.session <- HiveWarehouseBuilder.session

setMethod("initialize", "HiveWarehouseBuilder", function(.Object, jsession) {
  .Object@jsession <- jsession
  .Object@jhwbuilder <- sparkR.callJStatic(
    "com.hortonworks.spark.sql.hive.llap.HiveWarehouseBuilder", "session", jsession)
  .Object
})

setGeneric("userPassword", function(hwbuilder, user, password) { standardGeneric("userPassword") })
setGeneric("hs2url", function(hwbuilder, hs2url) { standardGeneric("hs2url") })
setGeneric("maxExecResults",
  function(hwbuilder, maxExecResults) {
    standardGeneric("maxExecResults")
  })
setGeneric("dbcp2Conf", function(hwbuilder, dbcp2Conf) { standardGeneric("dbcp2Conf") })
setGeneric("defaultDB", function(hwbuilder, defaultDB) { standardGeneric("defaultDB") })
setGeneric("principal", function(hwbuilder, principal) { standardGeneric("principal") })
setGeneric("credentialsEnabled", function(hwbuilder) { standardGeneric("credentialsEnabled") })
setGeneric("build", function(hwbuilder) { standardGeneric("build") })

setMethod("userPassword",
          signature(hwbuilder = "HiveWarehouseBuilder", user = "character", password = "character"),
          function(hwbuilder, user, password) {
            sparkR.callJMethod(hwbuilder@jhwbuilder, "userPassword", user, password)
            hwbuilder
          })

setMethod("hs2url",
          signature(hwbuilder = "HiveWarehouseBuilder", hs2url = "character"),
          function(hwbuilder, hs2url) {
            sparkR.callJMethod(hwbuilder@jhwbuilder, "hs2url", hs2url)
            hwbuilder
          })

setMethod("maxExecResults",
          signature(hwbuilder = "HiveWarehouseBuilder", maxExecResults = "numeric"),
          function(hwbuilder, maxExecResults) {
            sparkR.callJMethod(hwbuilder@jhwbuilder, "maxExecResults", as.integer(maxExecResults))
            hwbuilder
          })

setMethod("dbcp2Conf",
          signature(hwbuilder = "HiveWarehouseBuilder", dbcp2Conf = "character"),
          function(hwbuilder, dbcp2Conf) {
            sparkR.callJMethod(hwbuilder@jhwbuilder, "dbcp2Conf", dbcp2Conf)
            hwbuilder
          })

setMethod("defaultDB",
          signature(hwbuilder = "HiveWarehouseBuilder", defaultDB = "character"),
          function(hwbuilder, defaultDB) {
            sparkR.callJMethod(hwbuilder@jhwbuilder, "defaultDB", defaultDB)
            hwbuilder
          })

setMethod("principal",
          signature(hwbuilder = "HiveWarehouseBuilder", principal = "character"),
          function(hwbuilder, principal) {
            sparkR.callJMethod(hwbuilder@jhwbuilder, "principal", principal)
            hwbuilder
          })

setMethod("credentialsEnabled",
          signature(hwbuilder = "HiveWarehouseBuilder"),
          function(hwbuilder) {
            sparkR.callJMethod(hwbuilder@jhwbuilder, "credentialsEnabled")
            hwbuilder
          })

setMethod("build",
          signature(hwbuilder = "HiveWarehouseBuilder"),
          function(hwbuilder) {
            jhwsession <- sparkR.callJMethod(hwbuilder@jhwbuilder, "build")
            new("HiveWarehouseSessionImpl", hwbuilder@jsession, jhwsession)
          })


# HiveWarehouseSessionImpl class and methods implemented in S4 OO classes
HiveWarehouseSessionImpl <- setClass("HiveWarehouseSessionImpl",
                                     slots = list(jsession = "jobj", jhwsession = "jobj"))

setMethod("initialize", "HiveWarehouseSessionImpl", function(.Object, jsession, jhwsession) {
  .Object@jsession <- jsession
  .Object@jhwsession <- jhwsession
  .Object
})

setGeneric("executeQuery", function(hwsession, sql) { standardGeneric("executeQuery") })
setGeneric("execute", function(hwsession, sql) { standardGeneric("execute") })
setGeneric("executeUpdate", function(hwsession, sql) { standardGeneric("executeUpdate") })
setGeneric("table", function(hwsession, sql) { standardGeneric("table") })
setGeneric("session", function(hwsession) { standardGeneric("session") })
setGeneric("setDatabase", function(hwsession, name) { standardGeneric("setDatabase") })
setGeneric("showDatabases", function(hwsession) { standardGeneric("showDatabases") })
setGeneric("showTables", function(hwsession) { standardGeneric("showTables") })
setGeneric("describeTable", function(hwsession, table) { standardGeneric("describeTable") })
setGeneric("createDatabase",
           function(hwsession, database, ifNotExists) {
             standardGeneric("createDatabase")
           })
setGeneric("createTable", function(hwsession, tableName) { standardGeneric("createTable") })
setGeneric("dropDatabase",
           function(hwsession, database, ifNotExists, cascade) {
             standardGeneric("dropDatabase")
           })
setGeneric("dropTable",
           function(hwsession, table, ifExists, purge) {
             standardGeneric("dropTable")
           })

setMethod("executeQuery",
          signature(hwsession = "HiveWarehouseSessionImpl", sql = "character"),
          function(hwsession, sql) {
            jdf <- sparkR.callJMethod(hwsession@jhwsession, "executeQuery", sql)
            SparkR:::dataFrame(jdf)
          })

setMethod("execute",
          signature(hwsession = "HiveWarehouseSessionImpl", sql = "character"),
          function(hwsession, sql) {
            jdf <- sparkR.callJMethod(hwsession@jhwsession, "execute", sql)
            SparkR:::dataFrame(jdf)
          })

setMethod("executeUpdate",
          signature(hwsession = "HiveWarehouseSessionImpl", sql = "character"),
          function(hwsession, sql) {
            as.logical(sparkR.callJMethod(hwsession@jhwsession, "executeUpdate", sql))
          })

setMethod("table",
          signature(hwsession = "HiveWarehouseSessionImpl", sql = "character"),
          function(hwsession, sql) {
            jdf <- sparkR.callJMethod(hwsession@jhwsession, "table", sql)
            SparkR:::dataFrame(jdf)
          })

setMethod("session",
          signature(hwsession = "HiveWarehouseSessionImpl"),
          function(hwsession) { hwsession@jsession })

setMethod("setDatabase",
          signature(hwsession = "HiveWarehouseSessionImpl", name = "character"),
          function(hwsession, name) {
            sparkR.callJMethod(hwsession@jhwsession, "setDatabase", name)
          })

setMethod("showDatabases",
          signature(hwsession = "HiveWarehouseSessionImpl"),
          function(hwsession) {
            jdf <- sparkR.callJMethod(hwsession@jhwsession, "showDatabases")
            SparkR:::dataFrame(jdf)
          })

setMethod("showTables",
          signature(hwsession = "HiveWarehouseSessionImpl"),
          function(hwsession) {
            jdf <- sparkR.callJMethod(hwsession@jhwsession, "showTables")
            SparkR:::dataFrame(jdf)
          })

setMethod("describeTable",
          signature(hwsession = "HiveWarehouseSessionImpl", table = "character"),
          function(hwsession, table) {
            jdf <- sparkR.callJMethod(hwsession@jhwsession, "describeTable", table)
            SparkR:::dataFrame(jdf)
          })

setMethod("createDatabase",
          signature(hwsession = "HiveWarehouseSessionImpl",
                    database = "character",
                    ifNotExists = "logical"),
          function(hwsession, database, ifNotExists) {
            sparkR.callJMethod(hwsession@jhwsession, "createDatabase", database, ifNotExists)
          })

setMethod("createTable",
          signature(hwsession = "HiveWarehouseSessionImpl", tableName = "character"),
          function(hwsession, tableName) {
              jtablebuilder <- sparkR.callJMethod(hwsession@jhwsession, "createTable", tableName)
            new("CreateTableBuilder", hwsession@jsession, jtablebuilder)
          })

setMethod("dropDatabase",
          signature(hwsession = "HiveWarehouseSessionImpl",
                    database = "character",
                    ifNotExists = "logical",
                    cascade = "logical"),
          function(hwsession, database, ifNotExists, cascade) {
            sparkR.callJMethod(
              hwsession@jhwsession,"dropDatabase", database, ifNotExists, cascade)
          })

setMethod("dropTable",
          signature(hwsession = "HiveWarehouseSessionImpl",
                    table = "character",
                    ifExists = "logical",
                    purge = "logical"),
          function(hwsession, table, ifExists, purge) {
            sparkR.callJMethod(hwsession@jhwsession, "createDatabase", table, ifExists, purge)
          })


# CreateTableBuilder class and methods implemented in S4 OO classes
CreateTableBuilder <- setClass("CreateTableBuilder",
                               slots = list(jsession = "jobj", jtablebuilder = "jobj"))

setMethod("initialize", "CreateTableBuilder", function(.Object, jsession, jtablebuilder) {
  .Object@jsession <- jsession
  .Object@jtablebuilder <- jtablebuilder
  .Object
})

setGeneric("ifNotExists", function(tablebuilder) { standardGeneric("ifNotExists") })
setGeneric("column", function(tablebuilder, name, type) { standardGeneric("column") })
setGeneric("partition", function(tablebuilder, name, type) { standardGeneric("partition") })
setGeneric("prop", function(tablebuilder, key, value) { standardGeneric("prop") })
setGeneric("clusterBy", function(tablebuilder, numBuckets, ...) { standardGeneric("clusterBy") })
setGeneric("create", function(tablebuilder) { standardGeneric("create") })
setGeneric("toString", function(tablebuilder) { standardGeneric("toString") })

setMethod("ifNotExists",
          signature(tablebuilder = "CreateTableBuilder"),
          function(tablebuilder) {
            sparkR.callJMethod(tablebuilder@jtablebuilder, "ifNotExists")
            tablebuilder
          })

setMethod("column",
          signature(tablebuilder = "CreateTableBuilder", name = "character", type = "character"),
          function(tablebuilder, name, type) {
            sparkR.callJMethod(tablebuilder@jtablebuilder, "column", name, type)
            tablebuilder
          })

setMethod("partition",
          signature(tablebuilder = "CreateTableBuilder", name = "character", type = "character"),
          function(tablebuilder, name, type) {
            sparkR.callJMethod(tablebuilder@jtablebuilder, "partition", name, type)
            tablebuilder
          })

setMethod("prop",
          signature(tablebuilder = "CreateTableBuilder", key = "character", value = "character"),
          function(tablebuilder, key, value) {
            sparkR.callJMethod(tablebuilder@jtablebuilder, "prop", key, value)
            tablebuilder
          })

setMethod("clusterBy",
          signature(tablebuilder = "CreateTableBuilder", numBuckets = "numeric"),
          function(tablebuilder, numBuckets, ...) {
            cols <- lapply(list(...),
                           function(col) {
                             stopifnot(is.character(col))
                             col
                           })
            sparkR.callJMethod(tablebuilder@jtablebuilder,
                              "clusterBy",
                              as.integer(numBuckets),
                              cols)
            tablebuilder
          })

setMethod("create",
          signature(tablebuilder = "CreateTableBuilder"),
          function(tablebuilder) {
            sparkR.callJMethod(tablebuilder@jtablebuilder, "create")
          })

setMethod("toString",
          signature(tablebuilder = "CreateTableBuilder"),
          function(tablebuilder) {
            sparkR.callJMethod(tablebuilder@jtablebuilder, "toString")
          })
