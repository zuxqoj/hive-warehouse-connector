package com.hortonworks.spark.sql.hive.llap.transaction;

public class TransactionException extends Exception {
  public TransactionException(Exception e) {
    super(e);
  }

  public TransactionException(String message) {
    super(message);
  }
}
