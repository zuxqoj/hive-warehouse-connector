/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hortonworks.spark.sql.hive.llap.streaming.examples

import java.io.{File, FileInputStream}
import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import scala.collection.JavaConverters.propertiesAsScalaMapConverter

/*
 * A Hive Streaming example to ingest data from socket and push into hive table.
 *
 * Assumed HIVE table Schema:
 * CREATE TABLE `streaming.web_sales`(
 * `ws_sold_date_sk` int,
 * `ws_sold_time_sk` int,
 * `ws_ship_date_sk` int,
 * `ws_item_sk` int,
 * `ws_bill_customer_sk` int,
 * `ws_bill_cdemo_sk` int,
 * `ws_bill_hdemo_sk` int,
 * `ws_bill_addr_sk` int,
 * `ws_ship_customer_sk` int,
 * `ws_ship_cdemo_sk` int,
 * `ws_ship_hdemo_sk` int,
 * `ws_ship_addr_sk` int,
 * `ws_web_page_sk` int,
 * `ws_web_site_sk` int,
 * `ws_ship_mode_sk` int,
 * `ws_warehouse_sk` int,
 * `ws_promo_sk` int,
 * `ws_order_number` int,
 * `ws_quantity` int,
 * `ws_wholesale_cost` float,
 * `ws_list_price` float,
 * `ws_sales_price` float,
 * `ws_ext_discount_amt` float,
 * `ws_ext_sales_price` float,
 * `ws_ext_wholesale_cost` float,
 * `ws_ext_list_price` float,
 * `ws_ext_tax` float,
 * `ws_coupon_amt` float,
 * `ws_ext_ship_cost` float,
 * `ws_net_paid` float,
 * `ws_net_paid_inc_tax` float,
 * `ws_net_paid_inc_ship` float,
 * `ws_net_paid_inc_ship_tax` float,
 * `ws_net_profit` float)
 *  PARTITIONED BY (
 *  `ws_sold_date` string)
 *  STORED AS ORC
 *  TBLPROPERTIES ('transactional'='true')
 */
case class Schema(ws_sold_date_sk: Int, ws_sold_time_sk: Int, ws_ship_date_sk: Int,
                  ws_item_sk: Int, ws_bill_customer_sk: Int, ws_bill_cdemo_sk: Int,
                  ws_bill_hdemo_sk: Int, ws_bill_addr_sk: Int, ws_ship_customer_sk: Int,
                  ws_ship_cdemo_sk: Int, ws_ship_hdemo_sk: Int, ws_ship_addr_sk: Int,
                  ws_web_page_sk: Int, ws_web_site_sk: Int, ws_ship_mode_sk: Int,
                  ws_warehouse_sk: Int, ws_promo_sk: Int, ws_order_number: Int,
                  ws_quantity: Int, ws_wholesale_cost: Float, ws_list_price: Float,
                  ws_sales_price: Float, ws_ext_discount_amt: Float, ws_ext_sales_price: Float,
                  ws_ext_wholesale_cost: Float, ws_ext_list_price: Float, ws_ext_tax: Float,
                  ws_coupon_amt: Float, ws_ext_ship_cost: Float, ws_net_paid: Float,
                  ws_net_paid_inc_tax: Float, ws_net_paid_inc_ship: Float,
                  ws_net_paid_inc_ship_tax: Float, ws_net_profit: Float, ws_sold_date: String)

object HiveStreamingExample {

  def main(args: Array[String]): Unit = {
    if (args.length != 4 && args.length != 6) {
      // scalastyle:off println
      System.err.println("Received args: " + args.toList)
      System.err.println(s"Usage: HiveStreamingExample " +
        s"<bootstrap-servers> <properties-file> <topic-name>")
      System.err.println(s"Usage: HiveStreamingExample " +
        s"<bootstrap-servers> <properties-file> <topic-name> <database> <table>")
      System.err.println(s"Kafka consumer properties such as security.protocol, " +
        s"ssl.truststore.location can be set in properties-file." +
        s" Value 'none' can be supplied if there are no properties to set.")
      // scalastyle:on println
      System.exit(1)
    }

    val bootstrapServers = args(0)
    var propertiesFileName = args(1)
    val topicName = args(2)
    val metastoreUri = args(3)


    val sparkConf = new SparkConf()
      .set("spark.sql.streaming.checkpointLocation", "./checkpoint")
    val sparkSession = SparkSession.builder()
      .appName("HiveStreamingExample")
      .config(sparkConf)
      .enableHiveSupport()
      .getOrCreate()

    val options = new scala.collection.mutable.HashMap[String, String]()
    import sparkSession.implicits._
    if (!Set("none", "null", "empty", "undefined").contains(propertiesFileName.toLowerCase.trim)) {
      val props = new Properties
      val fileStream = new FileInputStream(new File(propertiesFileName))
      props.load(fileStream)
      fileStream.close()
      props.asScala.toMap.foreach { kv =>
        options(s"kafka.${kv._1}") = kv._2
      }
    }

    val df = sparkSession
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", bootstrapServers)
      .option("startingOffsets", "earliest")
      .option("subscribe", topicName)
      .options(options)
      .load()
    val dataStream = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .as[(String, String)].map(_._2)

    val (dbName, tableName) = if (args.length == 6) {
      (args(4), args(5))
    } else {
      (sparkConf.get("spark.datasource.hive.warehouse.dbname"),
        sparkConf.get("spark.datasource.hive.warehouse.tablename"))
    }
    val writer =
      dataStream.map { s =>
        val x = s.split(",")
        Schema(x(0).toInt, x(1).toInt, x(2).toInt, x(3).toInt, x(4).toInt, x(5).toInt,
          x(6).toInt, x(7).toInt, x(8).toInt, x(9).toInt, x(10).toInt, x(11).toInt,
          x(12).toInt, x(13).toInt, x(14).toInt, x(15).toInt, x(16).toInt, x(17).toInt,
          x(18).toInt, x(19).toFloat, x(20).toFloat, x(21).toFloat, x(22).toFloat,
          x(23).toFloat, x(24).toFloat, x(25).toFloat, x(26).toFloat, x(27).toFloat,
          x(28).toFloat, x(29).toFloat, x(30).toFloat, x(31).toFloat, x(32).toFloat,
          x(33).toFloat, x(34))
      }
        .writeStream
        .format("com.hortonworks.spark.sql.hive.llap.streaming.HiveStreamingDataSource")
        .option("metastoreUri", metastoreUri)
        .option("database", dbName)
        .option("table", tableName)

    // before this, a new terminal that runs 'nc -l <port>' has to be started and
    // csv records for web_sales table has to be pasted so that spark streaming
    // can read the rows from nc and pass it on to hive data source
    val query = writer.start()
    query.awaitTermination()

    query.stop()
    sparkSession.stop()
  }
}