package com.hortonworks.spark.sql.hive.llap;

import org.apache.spark.sql.sources.v2.writer.WriterCommitMessage;

import java.util.Set;

public class StreamingWriterCommitMessage implements WriterCommitMessage {
  private final Set<String> createdPartitions;
  public String note;

  public StreamingWriterCommitMessage(Set<String> createdPartitions, String note) {
    this.createdPartitions = createdPartitions;
    this.note = note;
  }

  public Set<String> getCreatedPartitions() {
    return createdPartitions;
  }
}
