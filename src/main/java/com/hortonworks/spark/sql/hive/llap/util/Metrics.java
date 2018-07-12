package com.hortonworks.spark.sql.hive.llap.util;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import org.elasticsearch.metrics.*;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

public class Metrics {

  private static MetricRegistry registry = null;
  private static boolean isRegistered = false;

  public static MetricRegistry registry() {
    synchronized(Metrics.class) {
      if(!isRegistered) {
        registry = new MetricRegistry();
        try {
          ElasticsearchReporter reporter =
              ElasticsearchReporter.forRegistry(registry).hosts("cn105-10.l42scl.hortonworks.com:9200").index("hwc").build();
          reporter.start(1, TimeUnit.SECONDS);
          isRegistered = true;
        } catch(IOException ioe) {
          throw new RuntimeException(ioe);
        }
      }
    }
    return registry;
  }

}

