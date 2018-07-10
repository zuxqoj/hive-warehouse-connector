package com.hortonworks.spark.sql.hive.llap.util;

import org.apache.spark.metrics.source.Source;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;

import java.util.function.Supplier;

public class Metrics implements Source {

  MetricRegistry registry;

  public Metrics() {
    registry = new MetricRegistry();
  }

  public Timer getTimer(String name) {
    return registry.timer(name);
  }

  public <T> T withTimer(String name, Supplier<T> supplier) {
    Timer timer = registry.timer(name);
    Timer.Context context = timer.time();
    T value;
    try {
      value = supplier.get();
    } finally {
      context.stop();
    }
    return value;
  }

  @Override
  public String sourceName() { return "hwc"; }

  @Override
  public MetricRegistry metricRegistry() { return registry; }

}

