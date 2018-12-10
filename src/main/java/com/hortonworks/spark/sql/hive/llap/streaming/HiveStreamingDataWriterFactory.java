package com.hortonworks.spark.sql.hive.llap.streaming;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;

import com.hortonworks.spark.sql.hive.llap.util.JobUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hive.streaming.HiveStreamingConnection;
import org.apache.hive.streaming.StreamingException;
import org.apache.hive.streaming.StrictDelimitedInputWriter;
import org.apache.spark.TaskContext;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.sources.v2.writer.DataWriter;
import org.apache.spark.sql.sources.v2.writer.DataWriterFactory;
import org.apache.spark.sql.types.StructType;

import com.hortonworks.spark.hive.utils.HiveIsolatedClassLoader;
import org.apache.spark.util.SerializableConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HiveStreamingDataWriterFactory implements DataWriterFactory<InternalRow> {

  private String jobId;
  private StructType schema;
  private String db;
  private Table table;
  private List<String> partition;
  private Long writeId;
  private final SerializableConfiguration conf;
  private static Logger LOG = LoggerFactory.getLogger(HiveStreamingDataWriterFactory.class);

  public HiveStreamingDataWriterFactory(HiveConf conf,
      String jobId, StructType schema, String db,
      Table table, List<String> partition, Long writeId) {
    this.jobId = jobId;
    this.schema = schema;
    this.db = db;
    this.table = table;
    this.partition = partition;
    this.writeId = writeId;
    this.conf = new SerializableConfiguration(conf);
  }

  @Override
  public DataWriter<InternalRow> createDataWriter(int partitionId, int attemptNumber) {
    if (TaskContext.get().attemptNumber() > 0) {
      try {
        deleteDirtyFiles(partitionId);
      } catch (IOException e) {
        throw new RuntimeException(
            "Unable delete if it exists directory from" + " previous runs", e);
      }
    }

    ClassLoader restoredClassloader = Thread.currentThread().getContextClassLoader();
    ClassLoader isolatedClassloader = HiveIsolatedClassLoader.isolatedClassLoader();
    try {
      Thread.currentThread().setContextClassLoader(isolatedClassloader);

      HiveStreamingConnection streamingConnection;
      try {
        streamingConnection = createStreamingConnection(partitionId);
        streamingConnection.beginTransaction();
      } catch (StreamingException e) {
        throw new RuntimeException("Unable to create hive streaming connection", e);
      }
      return new HiveStreamingDataWriter((HiveConf) conf.value(), jobId,
          schema, partitionId, attemptNumber, streamingConnection);
    } finally {
      Thread.currentThread().setContextClassLoader(restoredClassloader);
    }
  }

  private void deleteDirtyFiles(Integer statementId)
      throws IOException {
    // File name is partitionDirectory/delta_writeId_writeId/bucket_00000
    // root is the directory that contains all the partitions
    LOG.info("This task has been restarted. The files written by the previous run will be cleaned if any");

    Path root = new Path(table.getSd().getLocation());
    final FileSystem fs = root.getFileSystem(conf.value());

    String deltaDirName = AcidUtils.deltaSubdir(writeId, writeId, statementId);

    RemoteIterator<LocatedFileStatus> files = fs.listFiles(root, true);
    while(files.hasNext()){
      LocatedFileStatus fileStatus = files.next();
      if (fileStatus.getPath().getName().equals(deltaDirName)) {
        try {
          fs.delete(fileStatus.getPath(), true);
        } catch (FileNotFoundException e) {
          LOG.warn("File " + fileStatus.getPath() + " not found while deleting dirty files");
        }
      }
    }
  }

  private HiveStreamingConnection createStreamingConnection(int partitionId) throws StreamingException {

    final StrictDelimitedInputWriter strictDelimitedInputWriter = StrictDelimitedInputWriter.newBuilder()
        .withFieldDelimiter(',').build();

    LOG.info("Creating hive streaming connection..");
    HiveStreamingConnection streamingConnection = HiveStreamingConnection.newBuilder()
        .withDatabase(db)
        .withTableObject(table)
        .withWriteId(writeId)
        .withStaticPartitionValues(partition)
        .withRecordWriter(strictDelimitedInputWriter)
        .withHiveConf((HiveConf) conf.value())
        .withAgentInfo(JobUtil.createAgentInfo(jobId, partitionId))
        .withStatementId(partitionId)
        .connect();
    LOG.info("{} created hive streaming connection.", streamingConnection);
    return streamingConnection;
  }
}

