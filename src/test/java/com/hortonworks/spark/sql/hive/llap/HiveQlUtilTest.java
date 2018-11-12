package com.hortonworks.spark.sql.hive.llap;

import java.util.Arrays;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;
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
}