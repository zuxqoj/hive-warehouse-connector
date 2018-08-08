package com.hortonworks.spark.sql.hive.llap.streaming;

import com.hortonworks.spark.sql.hive.llap.HiveWarehouseSession;
import org.apache.spark.sql.sources.v2.DataSourceOptions;
import org.apache.spark.sql.sources.v2.DataSourceV2;
import org.apache.spark.sql.sources.v2.StreamWriteSupport;
import org.apache.spark.sql.sources.v2.writer.streaming.StreamWriter;
import org.apache.spark.sql.sources.v2.SessionConfigSupport;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.types.StructType;

public class HiveStreamingDataSource implements DataSourceV2, StreamWriteSupport, SessionConfigSupport {

  @Override
  public StreamWriter createStreamWriter(final String queryId, final StructType schema, final OutputMode mode,
    final DataSourceOptions options) {
    return createDataSourceWriter(queryId, schema, options);
  }

  private HiveStreamingDataSourceWriter createDataSourceWriter(final String id, final StructType schema,
    final DataSourceOptions options) {
    return new HiveStreamingDataSourceWriter(id, schema, options);
  }

  @Override public String keyPrefix() {
    return HiveWarehouseSession.HIVE_WAREHOUSE_POSTFIX;
  }

}

