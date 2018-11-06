/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hortonworks.spark.sql.hive.llap.util;

import java.util.UUID;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;

import static java.lang.String.format;

public class HiveQlUtil {

  public static String projections(String[] columns) {
    return "`" + String.join("` , `", columns) + "`";
  }

  public static String selectStar(String database, String table) {
    return format("SELECT * FROM %s.%s", database, table);
  }

  public static String selectStar(String table) {
    return format("SELECT * FROM %s", table);
  }

  public static String selectProjectAliasFilter(String projections, String table, String alias, String whereClause) {
    return format("select %s from (%s) as %s %s", projections, table, alias, whereClause);
  }
    public static String useDatabase(String database) {
        return format("USE %s", database);
    }

    public static String showDatabases() {
        return "SHOW DATABASES";
    }

    public static String showTables(String database) {
        return format("SHOW TABLES IN %s", database);
    }

    public static String describeTable(String database, String sql) {
        return format("DESCRIBE %s.%s", database, sql);
    }

    public static String dropDatabase(String database, boolean ifExists, boolean cascade) {
        return format("DROP DATABASE %s %s %s",
                orBlank(ifExists, "IF EXISTS"),
                database,
                orBlank(cascade, "CASCADE"));
    }

    //Requires jdbc Connection attached to current database (see HiveWarehouseSessionImpl.dropTable)
    public static String dropTable(String table, boolean ifExists, boolean purge) {
        return format("DROP TABLE %s %s %s",
                orBlank(ifExists, "IF EXISTS"),
                table,
                orBlank(purge, "PURGE"));
    }

    public static String createDatabase(String database, boolean ifNotExists) {
        String x = format("CREATE DATABASE %s %s",
                orBlank(ifNotExists, "IF NOT EXISTS"),
                database);
        return x;
    }

    public static String columnSpec(String columnSpec) {
        return format(" (%s) ", columnSpec);
    }

    public static String partitionSpec(String partSpec) {
        return format(" PARTITIONED BY(%s) ", partSpec);
    }

    public static String bucketSpec(String bucketColumns, long numOfBuckets) {
        return format(" CLUSTERED BY (%s) INTO %s BUCKETS ", bucketColumns, numOfBuckets);
    }

    public static String tblProperties(String keyValuePairs) {
        return format(" TBLPROPERTIES (%s) ", keyValuePairs);
    }

    public static String createTablePrelude(String database, String table, boolean ifNotExists) {
        return format("CREATE TABLE %s %s.%s ",
                orBlank(ifNotExists, "IF NOT EXISTS"),
                database,
                table);
    }

    private static String orBlank(boolean useText, String text) {
        return (useText ? text : "");
    }

    public static String loadInto(String path, String database, String table) {
      return format("LOAD DATA INPATH '%s' INTO TABLE %s.%s", path, database, table);
    }

    public static String randomAlias() {
        return "q_" + UUID.randomUUID().toString().replaceAll("[^A-Za-z0-9 ]", "");
    }

    public static String escapeForQl(Object value, String escapeDelimiter) {
      return value == null ? null : StringUtils.replace(value.toString(), ",", escapeDelimiter + ",");
    }

  /**
   *
   * formats record for Ql
   *  1)Escapes strings if escapeDelimiter is non-null
   *  2)Wraps strings with quoteDelimiter(if non-null)
   *
   * @param schema Schema of the record
   * @param record record
   * @param escapeDelimiter Escape char/string
   * @param quoteDelimiter Quote char/string
   * @return formatted record
   */
    public static Object[] formatRecord(StructType schema, InternalRow record, String escapeDelimiter, String quoteDelimiter) {
      StructField[] schemaFields = schema.fields();
      Object[] arr = new Object[record.numFields()];
      for (int i = 0; i < record.numFields(); i++) {
        DataType dataType = schemaFields[i].dataType();
        Object obj = record.get(i, dataType);
        if (DataTypes.StringType.equals(dataType) && obj != null) {
          //escape comma with escapeDelimiter
          if (escapeDelimiter != null) {
            obj = StringUtils.replace(obj.toString(), ",", escapeDelimiter + ",");
          }
          //wrap with quoteDelimiter
          if (quoteDelimiter != null) {
            obj = quoteDelimiter + obj + quoteDelimiter;
          }
          //Internally spark needs UTF8String
          obj = UTF8String.fromString(obj.toString());
        }
        arr[i] = obj;
      }
      return arr;
    }

}
