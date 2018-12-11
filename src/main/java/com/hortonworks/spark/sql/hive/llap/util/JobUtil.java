package com.hortonworks.spark.sql.hive.llap.util;

import com.hortonworks.spark.sql.hive.llap.HWConf;
import com.hortonworks.spark.sql.hive.llap.streaming.HiveStreamingDataSourceWriter;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.mapred.JobConf;
import org.apache.spark.SparkContext;

import java.sql.Driver;
import java.sql.DriverManager;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.spark.sql.sources.v2.DataSourceOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

public class JobUtil {

  private static Logger LOG = LoggerFactory.getLogger(JobUtil.class);
  private static final long DEFAULT_COMMIT_INTERVAL_ROWS = 10000;

  public static JobConf createJobConf(Map<String, String> options, String queryString) {
    JobConf jobConf = new JobConf(SparkContext.getOrCreate().hadoopConfiguration());
    jobConf.set("hive.llap.zk.registry.user", "hive");
    jobConf.set("llap.if.hs2.connection", HWConf.RESOLVED_HS2_URL.getFromOptionsMap(options));
    if (queryString != null) {
      jobConf.set("llap.if.query", queryString);
    }
    jobConf.set("llap.if.user", HWConf.USER.getFromOptionsMap(options));
    jobConf.set("llap.if.pwd", HWConf.PASSWORD.getFromOptionsMap(options));
    if (options.containsKey("default.db")) {
      jobConf.set("llap.if.database", HWConf.DEFAULT_DB.getFromOptionsMap(options));
    }
    if (!options.containsKey("handleid")) {
      String handleId = UUID.randomUUID().toString();
      options.put("handleid", handleId);
    }
    jobConf.set("llap.if.handleid", options.get("handleid"));
    return jobConf;
  }

  public static void replaceSparkHiveDriver() throws Exception {
    Enumeration<Driver> drivers = DriverManager.getDrivers();
    while(drivers.hasMoreElements()) {
      Driver driver = drivers.nextElement();
      String driverName = driver.getClass().getName();
      LOG.debug("Found a registered JDBC driver {}", driverName);
      if(driverName.endsWith("HiveDriver")) {
        LOG.debug("Deregistering {}", driverName);
        DriverManager.deregisterDriver(driver);
      } else {
        LOG.debug("Not deregistering the {}", driverName);
      }
    }
    DriverManager.registerDriver((Driver) Class.forName("shadehive.org.apache.hive.jdbc.HiveDriver").newInstance());
  }

  public static String createAgentInfo(String jobId, int partitionId) {
    return jobId + "(" + partitionId + ")";
  }

  public static List<String> partitionFromOptions(final DataSourceOptions options) {
    String partitionS = options.get("partition").orElse(null);
    return partitionS == null ? null : Arrays.asList(partitionS.split(","));
  }

  public static long commitIntervalFromOptions(final DataSourceOptions options) {
    String commitIntervalRows = options.get("commitIntervalRows").orElse("" + DEFAULT_COMMIT_INTERVAL_ROWS);
    return Long.parseLong(commitIntervalRows);
  }

  public static String metastoreUriFromOptions(final DataSourceOptions options) {
    return options.get("metastoreUri").orElse("thrift://localhost:9083");
  }

  public static String dbNameFromOptions(final DataSourceOptions options) {
    String dbName;
    if(options.get("default.db").isPresent()) {
      dbName = options.get("default.db").get();
    } else {
      dbName = options.get("database").orElse("default");
    }
    return dbName;
  }

  public static String createAgentInfo(String jobId) {
    return jobId + "()";
  }

  public static HiveConf createJobHiveConfig(String metastoreUri,
      @Nullable String metastoreKrbPrincipal, DataSourceOptions options) {
    HiveConf hiveConf = new HiveConf();
    hiveConf.set(MetastoreConf.ConfVars.THRIFT_URIS.getHiveName(), metastoreUri);
    // isolated classloader and shadeprefix are required for reflective instantiation of outputformat class when
    // creating hive streaming connection (isolated classloader to load different hive versions and shadeprefix for
    // HIVE-19494)
    hiveConf.setVar(HiveConf.ConfVars.HIVE_CLASSLOADER_SHADE_PREFIX, "shadehive");
    hiveConf.set(MetastoreConf.ConfVars.CATALOG_DEFAULT.getHiveName(), "hive");

    // HiveStreamingCredentialProvider requires metastore uri and kerberos principal to obtain delegation token to
    // talk to secure metastore. When HiveConf is created above, hive-site.xml is used from classpath. This option
    // is just an override in case if kerberos principal cannot be obtained from hive-site.xml.
    if (metastoreKrbPrincipal != null) {
      hiveConf.set(MetastoreConf.ConfVars.KERBEROS_PRINCIPAL.getHiveName(), metastoreKrbPrincipal);
    }
    hiveConf.setDouble(HiveStreamingDataSourceWriter.TASK_EXCEPTION_PROBABILITY_BEFORE_COMMIT,
        options.getDouble(HiveStreamingDataSourceWriter.TASK_EXCEPTION_PROBABILITY_BEFORE_COMMIT,
            HiveStreamingDataSourceWriter.TASK_EXCEPTION_PROBABILITY_BEFORE_COMMIT_DEFAULT));
    hiveConf.setDouble(HiveStreamingDataSourceWriter.TASK_EXCEPTION_PROBABILITY_AFTER_COMMIT,
        options.getDouble(HiveStreamingDataSourceWriter.TASK_EXCEPTION_PROBABILITY_AFTER_COMMIT,
            HiveStreamingDataSourceWriter.TASK_EXCEPTION_PROBABILITY_AFTER_COMMIT_DEFAULT));
    return hiveConf;
  }
}
