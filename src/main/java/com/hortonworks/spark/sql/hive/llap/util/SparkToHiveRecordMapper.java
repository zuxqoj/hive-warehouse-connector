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

import java.io.Serializable;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.types.StructType;

/**
 * -Provides methods to map a sparkDF record({@link InternalRow}) to hive table(in the sequence of hive table columns)
 * -Ensures dataframe columns are isomorphic to hive columns, throws otherwise.
 */
public class SparkToHiveRecordMapper implements Serializable {

  private static final long serialVersionUID = -5213255591548912610L;

  private StructType schemaInHiveColumnsOrder;
  private int[] mapping;
  private boolean recordReorderingNeeded = false;
  private StructType sparkDFSchema;

  public SparkToHiveRecordMapper(StructType sparkDFSchema, String[] hiveTableColumns) {
    this.sparkDFSchema = sparkDFSchema;
    this.schemaInHiveColumnsOrder = sparkDFSchema;
    resolveBasedOnColumnNames(hiveTableColumns);
  }

  public SparkToHiveRecordMapper() {
  }

  public StructType getSchemaInHiveColumnsOrder() {
    return schemaInHiveColumnsOrder;
  }

  /**
   * Permutes fields of record according to hive column sequence if needed.
   *
   * @param record record
   * @return record having same sequence to that of hive column sequence
   */
  public InternalRow mapToHiveColumns(InternalRow record) {
    if (this.recordReorderingNeeded) {
      int numFields = record.numFields();
      Object[] reordered = new Object[numFields];
      for (int i = 0; i < numFields; i++) {
        reordered[mapping[i]] = record.get(i, sparkDFSchema.fields()[i].dataType());
      }
      record = new GenericInternalRow(reordered);
    }
    return record;
  }

  /**
   * Builds mapping between sparkDF columns to hive columns.
   * <br/>
   * Eg: If schemaInHiveColumnsOrder is [3,2,0,1], this implies that
   * <br/>Spark DF column ----corresponds to -----> Hive column
   * <br/>    0                                      3
   * <br/>    1                                      2
   * <br/>    2                                      0
   * <br/>    3                                      1
   */
  private void resolveBasedOnColumnNames(String[] hiveTableColumns) {
    //this will be the case, when table does not exist
    if (hiveTableColumns == null) {
      return;
    }

    String[] dataframeColumns = sparkDFSchema.fieldNames();
    if (hiveTableColumns.length != dataframeColumns.length) {
      throw new IllegalArgumentException("Number of columns in specified hive table and given dataframe do not" +
          " match, ensure that dataframe has all the columns same as to that of your hive table");
    }

    Map<String, Integer> hiveColumnIndexMapping = IntStream.range(0, hiveTableColumns.length)
        .boxed()
        .collect(Collectors.toMap(i -> hiveTableColumns[i], Function.identity()));

    this.mapping = new int[hiveTableColumns.length];
    Integer hiveColIndex;
    for (int i = 0; i < dataframeColumns.length; i++) {
      if ((hiveColIndex = hiveColumnIndexMapping.get(dataframeColumns[i])) == null) {
        throw new IllegalArgumentException("Dataframe column: " + dataframeColumns[i] +
            " does not exist in specified hive table");
      }
      this.mapping[i] = hiveColIndex;
      //i.e. sequence of columns in df is different to that of hive columns, we will need to reorder
      if (i != hiveColIndex) {
        this.recordReorderingNeeded = true;
      }
    }
    //if reordering is needed, reorder schema as well...as later it is passed to OrcOutputWriter
    if (this.recordReorderingNeeded) {
      this.schemaInHiveColumnsOrder = new StructType();
      for (String hiveTableColumn : hiveTableColumns) {
        this.schemaInHiveColumnsOrder = this.schemaInHiveColumnsOrder.add(sparkDFSchema.fields()[sparkDFSchema.fieldIndex(hiveTableColumn)]);
      }
    }
  }

}
