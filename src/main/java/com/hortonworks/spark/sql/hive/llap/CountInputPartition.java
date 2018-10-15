package com.hortonworks.spark.sql.hive.llap;

import org.apache.spark.sql.sources.v2.reader.InputPartition;
import org.apache.spark.sql.sources.v2.reader.InputPartitionReader;
import org.apache.spark.sql.vectorized.ColumnarBatch;

public class CountInputPartition implements InputPartition<ColumnarBatch> {
  private long numRows;

  public CountInputPartition(long numRows) {
    this.numRows = numRows;
  }

  @Override
  public InputPartitionReader<ColumnarBatch> createPartitionReader() {
    return new CountInputPartitionReader(numRows);
  }
}
