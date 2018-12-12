package com.hortonworks.spark.sql.hive.llap.transaction;

import com.hortonworks.spark.sql.hive.llap.DefaultJDBCWrapper;
import com.hortonworks.spark.sql.hive.llap.DriverResultSet;
import com.hortonworks.spark.sql.hive.llap.HWConf;
import com.hortonworks.spark.sql.hive.llap.StreamingWriterCommitMessage;
import com.hortonworks.spark.sql.hive.llap.util.HiveQlUtil;
import com.hortonworks.spark.sql.hive.llap.util.JobUtil;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.txn.TxnStore;
import org.apache.hive.streaming.HiveStreamingConnection;
import org.apache.hive.streaming.StreamingException;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hive.streaming.StrictDelimitedInputWriter;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.sources.v2.DataSourceOptions;
import org.apache.spark.sql.sources.v2.writer.WriterCommitMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

public class TransactionManager {
  private static final Logger
      LOG = LoggerFactory.getLogger(HiveStreamingConnection.class.getName());
  private static final String COMMIT_KEY = TxnStore.TXN_KEY_START + "_SPARK";
  private static final String COMMIT_KEY_FORMAT = COMMIT_KEY + "_%s";
  private static final String INITIAL_COMMIT_KEY = "-1";
  private static final int MAX_STREAMING_JOBS = 3;

  private final StrictDelimitedInputWriter strictDelimitedInputWriter = StrictDelimitedInputWriter.newBuilder()
      .withFieldDelimiter(',').build();

  private final HiveConf conf;
  private final String databaseName;
  private final String tableName;
  private final String agentInfo;
  private final List<String> partition;
  private final Map<String, String> optionsMap;
  private final DataSourceOptions options;
  private final AtomicBoolean committed = new AtomicBoolean(false);

  private Long writeId;
  private HiveStreamingConnection connection;
  private Table table;

  public TransactionManager(String jobId, HiveConf conf, String databaseName,
      String tableName, List<String> partition, final DataSourceOptions options) {
    this.conf = conf;
    this.databaseName = databaseName;
    this.tableName = tableName;
    this.agentInfo =
        JobUtil.createAgentInfo(jobId);
    this.partition = partition;
    this.optionsMap = Collections.unmodifiableMap(options.asMap());
    this.options = options;
  }

  /**
   * Creates and opens a transaction
   * @throws StreamingException if transaction cant be opened.
   */
  public void createAndStartTransaction() throws StreamingException {
    committed.set(false);
    createHiveStreamingConnection();
  }

  public void commitTransaction(Set<String> partitions)
      throws StreamingException{
    commitTransaction(partitions, null, null);
  }

  public void commitTransaction(Set<String> partitions, String key,
      String value) throws StreamingException{
    LOG.info("Committing transaction with"
        + " partition=" + partitions + ", "
        + " key=" + key + ", "
        + " value=" + value + ".");
    connection.commitTransaction(partitions, key, value);
  }

  public void abortTransaction() throws StreamingException{
    connection.abortTransaction();
  }

  public void commitFromWorkerMessages(WriterCommitMessage[] messages,
      String id, Long epochId) throws TransactionException, StreamingException {
    // Commit can be called several times and it should be idempotent
    if (!committed.compareAndSet(false, true)) {
      LOG.warn("Already committed epochId=" + epochId + ".");
      return;
    }

    Set<String> partitions = new HashSet<>();
    for (WriterCommitMessage m: messages) {
      if (m instanceof StreamingWriterCommitMessage) {
        partitions.addAll(((StreamingWriterCommitMessage)m).getCreatedPartitions());
      } else {
        String error = "Received unexpected WriterCommitMessage: " + m + ", "
            + "commit will be aborted.";
        LOG.error(error);
        throw new TransactionException(error);
      }
    }
    if (epochId != null) {
      String commitKey = getCommitKey(id);
      commitTransaction(partitions, commitKey, epochId.toString());
    } else {
      commitTransaction(partitions);
    }
  }

  public void checkForRunningJobs(String id)
      throws TransactionException, SQLException {
    String database = JobUtil.dbNameFromOptions(options);
    try (Connection conn = getConnection()) {
      DriverResultSet tblProperties = DefaultJDBCWrapper
          .executeStmt(conn, database, HiveQlUtil.getTableProperties(tableName),
              Long.parseLong(HWConf.MAX_EXEC_RESULTS.getFromOptionsMap(optionsMap)));
      int ongoingStreamingJobs = 0;
      String commitKey = getCommitKey(id);
      for (Row r : tblProperties.getData()) {
        // Second condition is because this can only be check at commit time(just before) because
        // that's the only moment when we have the id. Therefore it could happen that
        // in the first commit there are only MAX_STREAMING_JOBS but after it when we check
        // again in the second commit there are MAX_STREAMING_JOBS + 1 and an exception is thrown
        // This could be solved if there was a place to check the number of ongoing jobs that
        // would be called once before the whole streaming starts
        if (r.getString(0).startsWith(COMMIT_KEY)
            && !(r.getString(0).equals(commitKey))) {
          ongoingStreamingJobs += 1;
        }
      }
      if (ongoingStreamingJobs >= TransactionManager.MAX_STREAMING_JOBS) {
        throw new TransactionException(String.format("%d jobs are running on table %s, max allowed jobs is %d",
            ongoingStreamingJobs, tableName, TransactionManager.MAX_STREAMING_JOBS));
      }
    }
  }

  public void commitFromWorkerMessages(WriterCommitMessage[] messages)
      throws TransactionException, StreamingException {
    commitFromWorkerMessages(messages, null, null);
  }

  public void close() {
    connection.close();
  }

  public Table getTable() {
    return table;
  }

  public Long getWriteId() {
    return writeId;
  }

  /**
   * Checks if a prior transaction
   * with the the epochId has been committed. This may happen in scenarios
   * where for example a transaction is committed and just after that but
   * before spark can persist the batch has ended successfully spark crashes.
   * It will also update the table with the appropriate property if it
   * doesn't have it.
   * @param id id from the job. This is not the job id. What's called in this
   *           class jobId is the runId or the queryId.
   * @param epochId to check if a batch with this epochId has already
   *                been committed.
   */
  public boolean isEpochIdCommitted(String id, long epochId) throws SQLException {
    String database = JobUtil.dbNameFromOptions(options);
    String commitKey = getCommitKey(id);
    try (Connection conn = getConnection()) {
      DriverResultSet rs = DefaultJDBCWrapper.executeStmt(conn, database,
          HiveQlUtil.getTableProperty(tableName, commitKey), 1);
      String value = rs.getData().get(0).getString(1);
      if ((value == null) || (value.contains("does not have property"))) {
        DefaultJDBCWrapper.executeUpdate(conn, database,
            HiveQlUtil.updateTableProperty(tableName, commitKey, INITIAL_COMMIT_KEY));
        return  false;
      } else {
        LOG.info("Long.parseLong(value)=" + Long.parseLong(value) + ", epochId=" + epochId);
        return Long.parseLong(value) >= epochId;
      }
    }
  }

  private void createHiveStreamingConnection() throws StreamingException {
    connection = HiveStreamingConnection.newBuilder()
        .withAgentInfo(agentInfo)
        .withDatabase(databaseName)
        .withTable(tableName)
        .withHiveConf(conf)
        .withStaticPartitionValues(partition)
        .withRecordWriter(strictDelimitedInputWriter)
        .connect();
    connection.beginTransaction();
    writeId = connection.getCurrentWriteId();
    table = connection.getTable();
  }

  private Connection getConnection() {
    String url = HWConf.RESOLVED_HS2_URL.getFromOptionsMap(optionsMap);
    String user = HWConf.USER.getFromOptionsMap(optionsMap);
    String dbcp2Configs = HWConf.DBCP2_CONF.getFromOptionsMap(optionsMap);
    return DefaultJDBCWrapper
        .getConnector(Option.empty(), url, user, dbcp2Configs);
  }

  public String getConnectionString() {
    return connection.toString();
  }

  private String getCommitKey(String id) {
    return String.format(COMMIT_KEY_FORMAT, id);
  }
}
