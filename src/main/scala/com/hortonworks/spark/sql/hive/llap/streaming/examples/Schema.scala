package com.hortonworks.spark.sql.hive.llap.streaming.examples


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
