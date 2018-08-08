package com.hortonworks.spark.sql.hive.llap;

import org.apache.spark.sql.sources.v2.writer.WriterCommitMessage;

import java.util.Set;

public class StreamingWriterCommitMessage implements WriterCommitMessage {
  private final Set<String> createdPartitions;
  public StreamingWriterCommitMessage(Set<String> createdPartitions) {
    this.createdPartitions = createdPartitions;
  }

  public Set<String> getCreatedPartitions() {
    return createdPartitions;
  }
}
