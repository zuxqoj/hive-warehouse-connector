package com.hortonworks.spark.sql.hive.llap.streaming;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import java.util.Optional;
import java.util.Random;

import com.hortonworks.spark.sql.hive.llap.transaction.TransactionException;
import com.hortonworks.spark.sql.hive.llap.transaction.TransactionManager;
import com.hortonworks.spark.sql.hive.llap.util.JobUtil;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.streaming.StreamingException;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.execution.streaming.StreamMetadata;
import org.apache.spark.sql.sources.v2.DataSourceOptions;
import org.apache.spark.sql.sources.v2.writer.DataWriterFactory;
import org.apache.spark.sql.sources.v2.writer.SupportsWriteInternalRow;
import org.apache.spark.sql.sources.v2.writer.WriterCommitMessage;
import org.apache.spark.sql.sources.v2.writer.streaming.StreamWriter;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HiveStreamingDataSourceWriter implements SupportsWriteInternalRow, StreamWriter {
  public static final String TASK_EXCEPTION_PROBABILITY_BEFORE_COMMIT = "test.task.exception.probability.beforecommit";
  public static final float TASK_EXCEPTION_PROBABILITY_BEFORE_COMMIT_DEFAULT = 0;
  public static final String TASK_EXCEPTION_PROBABILITY_AFTER_COMMIT = "test.task.exception.probability.aftercommit";
  public static final float TASK_EXCEPTION_PROBABILITY_AFTER_COMMIT_DEFAULT = 0;

  public static final String DRIVER_EXCEPTION_PROBABILITY_BEFORE_COMMIT = "test.driver.exception.probability.beforecommit";
  public static final float DRIVER_EXCEPTION_PROBABILITY_BEFORE_COMMIT_DEFAULT = 0;
  public static final String DRIVER_EXCEPTION_PROBABILITY_AFTER_COMMIT = "test.driver.exception.probability.aftercommit";
  public static final float DRIVER_EXCEPTION_PROBABILITY_AFTER_COMMIT_DEFAULT = 0;

  private static Logger LOG = LoggerFactory.getLogger(HiveStreamingDataSourceWriter.class);

  private String jobId;
  private StructType schema;
  private String db;
  private String tableName;
  private List<String> partition;
  private String metastoreUri;
  private String metastoreKerberosPrincipal;
  private HiveStreamingDataWriterFactory streamingDataWriterFactory;
  private TransactionManager transactionManager;
  private HiveConf conf;
  private final DataSourceOptions options;

  private final double chanceExceptionBeforeCommit;
  private final double chanceExceptionAfterCommit;
  private final Random random;

  public HiveStreamingDataSourceWriter(String jobId, StructType schema,
      final DataSourceOptions options) {
    this.jobId = jobId;
    this.schema = schema;
    this.db = JobUtil.dbNameFromOptions(options);
    this.tableName = options.get("table").orElse(null);;
    this.partition = JobUtil.partitionFromOptions(options);
    this.metastoreUri = JobUtil.metastoreUriFromOptions(options);
    this.metastoreKerberosPrincipal = options.get("metastoreKrbPrincipal").orElse(null);;

    LOG.info("OPTIONS - database: {} table: {} partition: {} metastoreUri: {} metastoreKerberosPrincipal: {} jobId: {} options: {}",
        db, tableName, partition, metastoreUri, metastoreKerberosPrincipal, jobId, options.asMap());

    conf = JobUtil.createJobHiveConfig(metastoreUri,
        metastoreKerberosPrincipal, options);

    this.transactionManager = new TransactionManager(jobId, conf, db,
        tableName, partition, options);
    this.options = options;

    chanceExceptionBeforeCommit = options.getDouble(
        HiveStreamingDataSourceWriter.DRIVER_EXCEPTION_PROBABILITY_BEFORE_COMMIT,
        HiveStreamingDataSourceWriter.DRIVER_EXCEPTION_PROBABILITY_BEFORE_COMMIT_DEFAULT);

    chanceExceptionAfterCommit = options.getDouble(
        HiveStreamingDataSourceWriter.DRIVER_EXCEPTION_PROBABILITY_AFTER_COMMIT,
        HiveStreamingDataSourceWriter.DRIVER_EXCEPTION_PROBABILITY_AFTER_COMMIT_DEFAULT);

    if (chanceExceptionBeforeCommit > 0 || chanceExceptionAfterCommit > 0) {
      random = new Random();
    } else {
      random = null;
    }
  }

  @Override
  public DataWriterFactory<InternalRow> createInternalRowWriterFactory() {
    try {
      this.transactionManager.createAndStartTransaction();
      LOG.info("Started new transaction with connection="
          + transactionManager.getConnectionString());
    } catch (StreamingException e) {
      throw new RuntimeException("Unable to start the transaction manager", e);
    }
    streamingDataWriterFactory = new HiveStreamingDataWriterFactory(conf, jobId,
        schema, db, transactionManager.getTable(), partition, transactionManager.getWriteId());
    // for the streaming case, commit transaction happens on task commit() (atleast-once), so interval is set to -1
    return streamingDataWriterFactory;
  }

  @Override
  public void commit(final long epochId, final WriterCommitMessage[] messages) {
    String id;
    try {
      id = getIdFromCheckpoint();
      if (id == null) {
        throw new RuntimeException("checkpointLocation"
            + " should be set in the options");
      }
      LOG.info("Commit id {}, jobId {}, epochId {}", id, jobId, epochId);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    if (chanceExceptionBeforeCommit > 0) {
      if (random.nextDouble() < chanceExceptionBeforeCommit) {
        throw new RuntimeException("Exception thrown in driver for testing purposes. "
            + "This should never happen outside of a testing environment, "
            + "chanceExceptionBeforeCommit=" + chanceExceptionBeforeCommit);
      }
    }
    try {
      transactionManager.checkForRunningJobs(id);
      if (!transactionManager.isEpochIdCommitted(id, epochId)) {
        //transactionManager.commitFromWorkerMessages(messages, id, epochId);
      } else {
        LOG.warn("A batch with parameters"
            + " job=" + id + ","
            + " epochId=" + epochId + ","
            + " databaseName=" + db
            + " tableName=" + tableName
            + " has already been committed. Skipping this micro batch and aborting transaction.");
        transactionManager.abortTransaction();
      }
    } catch ( SQLException | TransactionException | StreamingException e) {
      throw new RuntimeException(e);
    }

    if (chanceExceptionAfterCommit > 0) {
      if (random.nextDouble() < chanceExceptionAfterCommit) {
        throw new RuntimeException("Exception thrown in driver for testing purposes. "
            + "This should never happen outside of a testing environment, "
            + "chanceExceptionAfterCommit=" + chanceExceptionAfterCommit);
      }
    }

    transactionManager.close();
  }

  @Override
  public void abort(final long epochId, final WriterCommitMessage[] messages) {
    LOG.info("Abort job {}", jobId);
    try {
      transactionManager.abortTransaction();
    } catch (StreamingException e) {
      // throw new RuntimeException("Error aborting transaction", e);
      LOG.error("Error aborting transaction", e);
    } finally {
      transactionManager.close();
    }
  }

  private String getIdFromCheckpoint() throws IOException {
    Optional<String> optionalPath = options.get("checkpointLocation");
    if (!optionalPath.isPresent()) {
      return null;
    }
    Path checkpointPath = new Path(optionalPath.get());
    FileSystem fs = checkpointPath.getFileSystem(conf);
    Path qualifiedPath = checkpointPath.makeQualified(fs.getUri(), fs.getWorkingDirectory());
    Path checkpointFile = new Path(qualifiedPath, "metadata");

    StreamMetadata streamMetadata = StreamMetadata.read(checkpointFile, conf).get();
    return streamMetadata.id();
  }
}

