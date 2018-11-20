package com.hortonworks.spark.sql.hive.llap;

import com.hortonworks.spark.sql.hive.llap.util.SparkToHiveRecordMapper;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.types.StructType;
import org.junit.Test;
import static org.apache.spark.sql.types.DataTypes.*;
import static org.junit.Assert.assertEquals;

public class SparkToHiveRecordMapperTest extends SessionTestBase {

  @Test
  public void testMapToHiveColumns() {

    String[] hiveColumns = {"c1", "c2", "c3"};

    StructType dfSchema = new StructType()
        .add("c3", IntegerType)
        .add("c1", ByteType)
        .add("c2", ShortType);
    InternalRow record = new GenericInternalRow(new Object[]{3, 1, 2});

    GenericInternalRow expected = new GenericInternalRow(new Object[]{1, 2, 3});
    test(hiveColumns, dfSchema, record, expected);
  }

  @Test
  public void testMapToHiveColumnsWithRightSequence() {

    String[] hiveColumns = {"c1", "c2", "c3"};

    StructType dfSchema = new StructType()
        .add("c1", IntegerType)
        .add("c2", ByteType)
        .add("c3", ShortType);

    InternalRow record = new GenericInternalRow(new Object[]{1, 2, 3});
    test(hiveColumns, dfSchema, record, record);
  }

  @Test
  public void testMapToHiveColumnsWithHiveColumnsNull() {

    StructType dfSchema = new StructType()
        .add("c1", IntegerType)
        .add("c2", ByteType)
        .add("c3", ShortType);

    InternalRow record = new GenericInternalRow(new Object[]{1, 2, 3});
    test(null, dfSchema, record, record);
  }


  @Test
  public void testWithDifferentColumnsInDF() {

    String[] hiveColumns = {"c1", "c2", "c3"};

    StructType dfSchema = new StructType()
        .add("c1", IntegerType)
        .add("c2", ByteType)
        .add("c999", ShortType);

    InternalRow record = new GenericInternalRow(new Object[]{1, 2, 3});
    test(hiveColumns, dfSchema, record, record);
  }

  @Test
  public void testWithDifferentNumberOfColsInHiveAndDF() {

    String[] hiveColumns = {"c1", "c2", "c3"};

    StructType dfSchema = new StructType()
        .add("c1", IntegerType)
        .add("c2", ByteType)
        .add("c3", ShortType)
        .add("c999", ShortType);

    InternalRow record = new GenericInternalRow(new Object[]{1, 2, 3, 4});
    test(hiveColumns, dfSchema, record, record);
  }

  private void test(String[] hiveColumns, StructType dfSchema, InternalRow record, InternalRow expected) {
    SparkToHiveRecordMapper sparkToHiveRecordMapper = new SparkToHiveRecordMapper(dfSchema, hiveColumns);
    InternalRow newRecord = sparkToHiveRecordMapper.mapToHiveColumns(record);
    assertEquals(expected, newRecord);
  }

}