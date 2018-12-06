package com.hortonworks.hwc;

/**
 * Provides APIs to construct merge query of form:
 * <pre>
 * {@code
 * MERGE INTO <target table> AS T USING <source expression/table> AS S
 * ON <boolean expression1>
 * WHEN MATCHED [AND <boolean expression2>] THEN UPDATE SET <set clause list>
 * WHEN MATCHED [AND <boolean expression3>] THEN DELETE
 * WHEN NOT MATCHED [AND <boolean expression4>] THEN INSERT VALUES<value list>
 *}
 *</pre>
 * <br/>
 * For more, refer:
 * <br/><a href="https://cwiki.apache.org/confluence/display/Hive/LanguageManual+DML#LanguageManualDML-Merge">https://cwiki.apache.org/confluence/display/Hive/LanguageManual+DML#LanguageManualDML-Merge</a>
 */
public interface MergeBuilder {
  /**
   * Used to specify target table.
   *
   * @param targetTable target table for merge
   * @param alias target table alias
   * @return {@link MergeBuilder}
   */
  MergeBuilder mergeInto(String targetTable, String alias);

  /**
   * Used to specify source table or source expression (e.g. (select * from some_table)). If expression is specified, it must be enclosed in braces.
   *
   * @param sourceTableOrExpr source table or expression
   * @param alias source alias
   * @return {@link MergeBuilder}
   */
  MergeBuilder using(String sourceTableOrExpr, String alias);

  /**
   * Used to specify condition expression for merge.
   *
   * @param expr merge condition
   * @return {@link MergeBuilder}
   */
  MergeBuilder on(String expr);

  /**
   * Used to specify fields to update for rows for which merge condition and matchExpr hold.
   *
   * @param matchExpr Condition to evaluate in addition to merge condition, before applying update. Can be null.
   * @param nameValuePairs pairs of columns and values (e.g. col1=val1)
   * @return {@link MergeBuilder}
   */
  MergeBuilder whenMatchedThenUpdate(String matchExpr, String... nameValuePairs);

  /**
   * Instructs to delete rows for which merge condition and matchExpr hold.
   *
   * @param matchExpr Condition to evaluate in addition to merge condition, before applying delete. Can be null.
   * @return {@link MergeBuilder}
   */
  MergeBuilder whenMatchedThenDelete(String matchExpr);

  /**
   *Instructs to insert rows to target table, for which merge condition and matchExpr hold.
   *
   * @param matchExpr Condition to evaluate in addition to merge condition, before applying insert. Can be null.
   * @param values values to insert
   * @return {@link MergeBuilder}
   */
  MergeBuilder whenNotMatchedInsert(String matchExpr, String... values);

  /**
   * Executes the merge operation
   */
  void merge();

}
