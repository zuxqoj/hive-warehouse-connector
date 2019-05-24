package com.hortonworks.spark.sql.hive.llap;

import java.nio.ByteBuffer;
import java.util.Arrays;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.catalyst.expressions.UnsafeRow;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;
import org.junit.Assert;
import org.junit.Test;

import static com.hortonworks.spark.sql.hive.llap.util.HiveQlUtil.formatRecord;
import static org.junit.Assert.assertTrue;

public class HiveQlUtilTest extends SessionTestBase {

  private final Object[] values = {0, "h,w,c", null};
  private final InternalRow record = new GenericInternalRow(values);
  private final StructType schema = new StructType()
      .add("c1", "int")
      .add("c2", "string")
      .add("c3", "string");

  @Test
  public void testFormatRecord() {
    Object[] formatted = formatRecord(schema, record, null, null);
    assertTrue(Arrays.equals(new Object[]{0, UTF8String.fromString("h,w,c"), null}, formatted));

    formatted = formatRecord(schema, record, "\\", null);
    assertTrue(Arrays.equals(new Object[]{0, UTF8String.fromString("h\\,w\\,c"), null}, formatted));

    formatted = formatRecord(schema, record, null, "'");
    assertTrue(Arrays.equals(new Object[]{0, UTF8String.fromString("'h,w,c'"), null}, formatted));

    formatted = formatRecord(schema, record, "\\", "'");
    assertTrue(Arrays.equals(new Object[]{0, UTF8String.fromString("'h\\,w\\,c'"), null}, formatted));
  }

  @Test
  public void testTimestampSchemaUnsafeRow() {
    byte[] buffer = new byte[100];
    UnsafeRow record = new UnsafeRow(2);
    record.pointTo(buffer, 100);
    record.setInt(0, 1);
    record.setLong(1, 1548811552012000L);

    StructType timeSchema = new StructType()
        .add("c1", "int")
        .add("c2", "timestamp");

    Object[] formatted = formatRecord(timeSchema, record, null, null);
    Assert.assertEquals(new Object[]{1, UTF8String.fromString("2019-01-30 01:25:52.012")}, formatted);
  }
}