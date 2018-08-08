package com.hortonworks.spark.sql.hive.llap.streaming;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Random;

import com.hortonworks.spark.sql.hive.llap.StreamingWriterCommitMessage;
import com.hortonworks.spark.sql.hive.llap.util.JobUtil;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.streaming.HiveStreamingConnection;
import org.apache.hive.streaming.StreamingException;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.sources.v2.writer.DataWriter;
import org.apache.spark.sql.sources.v2.writer.WriterCommitMessage;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Joiner;

public class HiveStreamingDataWriter implements DataWriter<InternalRow> {
  private static Logger LOG = LoggerFactory.getLogger(HiveStreamingDataWriter.class);

  private final String jobId;
  private final StructType schema;
  private final int partitionId;
  private final int attemptNumber;
  private final HiveStreamingConnection streamingConnection;
  private final HiveConf hiveConf;

  private final double chanceExceptionBeforeCommit;
  private final double chanceExceptionAfterCommit;
  private final Random random;

  private long rowsWritten = 0;

  public HiveStreamingDataWriter(HiveConf hiveConf, String jobId, StructType schema, int partitionId, int
    attemptNumber, HiveStreamingConnection connection) {
    this.jobId = jobId;
    this.schema = schema;
    this.partitionId = partitionId;
    this.attemptNumber = attemptNumber;
    this.streamingConnection = connection;
    this.hiveConf = hiveConf;

    chanceExceptionBeforeCommit = hiveConf.getDouble(
        HiveStreamingDataSourceWriter.EXCEPTION_PROBABILITY_BEFORE_COMMIT,
        HiveStreamingDataSourceWriter.EXCEPTION_PROBABILITY_BEFORE_COMMIT_DEFAULT);

    chanceExceptionAfterCommit = hiveConf.getDouble(
        HiveStreamingDataSourceWriter.EXCEPTION_PROBABILITY_AFTER_COMMIT,
        HiveStreamingDataSourceWriter.EXCEPTION_PROBABILITY_AFTER_COMMIT_DEFAULT);

    if (chanceExceptionBeforeCommit > 0 || chanceExceptionAfterCommit > 0) {
      random = new Random();
    } else {
      random = null;
    }
  }

  @Override
  public void write(final InternalRow record) throws IOException {
    String delimitedRow = Joiner.on(",").useForNull("")
      .join(scala.collection.JavaConversions.seqAsJavaList(record.toSeq(schema)));
    try {
      streamingConnection.write(delimitedRow.getBytes(Charset.forName("UTF-8")));
      rowsWritten++;
    } catch (StreamingException e) {
      throw new IOException(e);
    }
  }

  @Override
  public WriterCommitMessage commit() throws IOException {
    if (chanceExceptionBeforeCommit > 0) {
      if (random.nextDouble() < chanceExceptionBeforeCommit) {
        throw new RuntimeException("Exception thrown for testing purposes. "
            + "This should never happen outside of a testing environment, "
            + "chanceExceptionBeforeCommit=" + chanceExceptionBeforeCommit);
      }
    }
    try {
      streamingConnection.commitTransaction();
    } catch (StreamingException e) {
      throw new IOException(e);
    }

    if (chanceExceptionAfterCommit > 0) {
      if (random.nextDouble() < chanceExceptionAfterCommit) {
        throw new RuntimeException("Exception thrown for testing purposes. "
            + "This should never happen outside of a testing environment, "
            + "chanceExceptionAfterCommit=" + chanceExceptionAfterCommit);
      }
    }

    String msg = "Committed jobId: " + jobId + " partitionId: " + partitionId + " attemptNumber: " + attemptNumber +
      " connectionStats: " + streamingConnection.getConnectionStats();
    streamingConnection.close();
    LOG.info("Closing streaming connection on commit. Msg: {} rowsWritten: {}", msg, rowsWritten);
    return new StreamingWriterCommitMessage(streamingConnection.getPartitions());
  }

  @Override
  public void abort() throws IOException {
    if (streamingConnection != null) {
      try {
        streamingConnection.abortTransaction();
      } catch (StreamingException e) {
        throw new IOException(e);
      }
      String msg = "Aborted jobId: " + jobId + " partitionId: " + partitionId + " attemptNumber: " + attemptNumber +
        " connectionStats: " + streamingConnection.getConnectionStats();
      streamingConnection.close();
      LOG.info("Closing streaming connection on abort. Msg: {} rowsWritten: {}", msg, rowsWritten);
    }
  }
}

