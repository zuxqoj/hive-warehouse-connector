package com.hortonworks.spark.sql.hive.llap;

import java.io.IOException;

import com.hortonworks.spark.sql.hive.llap.util.SerializableHadoopConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.sources.v2.writer.DataWriter;
import org.apache.spark.sql.sources.v2.writer.DataWriterFactory;
import org.apache.spark.sql.types.StructType;

public class HiveWarehouseDataWriterFactory implements DataWriterFactory<InternalRow> {

  protected String jobId;
  protected StructType schema;
  private Path path;
  private SerializableHadoopConfiguration conf;

  public HiveWarehouseDataWriterFactory(String jobId, StructType schema,
      Path path, SerializableHadoopConfiguration conf) {
    this.jobId = jobId;
    this.schema = schema;
    this.path = path;
    this.conf = conf;
  }

  @Override public DataWriter<InternalRow> createDataWriter(int partitionId, long taskId, long epochId) {
    Path filePath = new Path(this.path, String.format("%s_%s_%s_%s", jobId, partitionId, taskId, epochId));
    FileSystem fs = null;
    try {
      fs = filePath.getFileSystem(conf.get());
    } catch (IOException e) {
      e.printStackTrace();
    }
    return getDataWriter(conf.get(), jobId, schema, partitionId, taskId, epochId,
        fs, filePath);
  }

  protected DataWriter<InternalRow> getDataWriter(Configuration conf, String jobId,
      StructType schema, int partitionId, long taskId, long epochId,
      FileSystem fs, Path filePath) {
    return new HiveWarehouseDataWriter(conf, jobId, schema, partitionId, taskId, epochId, fs, filePath);
  }
}