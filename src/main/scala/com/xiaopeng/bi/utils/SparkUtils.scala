package com.xiaopeng.bi.utils

import java.sql.Connection
import java.util.Properties

import com.xiaopeng.bi.constant.Constants
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, RowFactory}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by kequan on 2/22/17.
  */
object SparkUtils {


  def setMaster(conf: SparkConf) {
    val local = ConfigurationUtil.getBoolean(Constants.SPARK_LOCAL)
    if (local) conf.setMaster("local[2]")
  }

  /**
    * 读取bi 表 并注册相应的表
    *
    * @param tabName
    * @param hiveContext
    * 注意： 该方法读取的是整张表，所以只能用于读取小表，表的数据数据过大会导致内存溢出
    */
  def readBiTable(tabName: String, hiveContext: HiveContext) = {
    val properties = new Properties()
    properties.put("driver", ConfigurationUtil.getProperty(Constants.JDBC_DRIVER))
    properties.put("user", ConfigurationUtil.getProperty(Constants.JDBC_USER))
    properties.put("password", ConfigurationUtil.getProperty(Constants.JDBC_PWD))
    val deptDF = hiveContext.read.jdbc(ConfigurationUtil.getProperty(Constants.JDBC_URL), tabName, properties).persist()
    deptDF.registerTempTable(tabName)
  }

  /**
    * 读取 xiaopeng2 表 并注册相应的表
    *
    * @param tabName
    * @param hiveContext
    * 注意： 该方法读取的是整张表，所以只能用于读取小表，表的数据数据过大会导致内存溢出
    */
  def readXiaopeng2Table(tabName: String, hiveContext: HiveContext) = {
    val properties = new Properties()
    properties.put("driver", ConfigurationUtil.getProperty(Constants.JDBC_DRIVER))
    properties.put("user", ConfigurationUtil.getProperty(Constants.JDBC_XIAOPENG2_USER))
    properties.put("password", ConfigurationUtil.getProperty(Constants.JDBC_XIAOPENG2_PWD))
    val deptDF = hiveContext.read.jdbc(ConfigurationUtil.getProperty(Constants.JDBC_XIAOPENG2_URL), tabName, properties).persist()
    deptDF.registerTempTable(tabName)
  }

  /**
    * 读取 xiaopeng2_faxing 表 并注册相应的表
    *
    * @param tabName
    * @param hiveContext
    * 注意： 该方法读取的是整张表，所以只能用于读取小表，表的数据数据过大会导致内存溢出
    */
  def readXiaopeng2FxTable(tabName: String, hiveContext: HiveContext) = {
    val properties = new Properties()
    properties.put("driver", ConfigurationUtil.getProperty(Constants.JDBC_DRIVER))
    properties.put("user", ConfigurationUtil.getProperty(Constants.JDBC_XIAOPENG2FX_USER))
    properties.put("password", ConfigurationUtil.getProperty(Constants.JDBC_XIAOPENG2FX_PWD))
    val deptDF = hiveContext.read.jdbc(ConfigurationUtil.getProperty(Constants.JDBC_XIAOPENG2FX_URL), tabName, properties).persist()
    deptDF.persist().registerTempTable(tabName)
  }

  /**
    * 读取 xiaopeng2_faxing 表 并注册相应的表
    *
    * @param tabName
    * @param hiveContext
    * 注意： 该方法读取的是整张表，所以只能用于读取小表，表的数据数据过大会导致内存溢出
    */
  def readXiaopeng2FxTableCache(tabName: String, hiveContext: HiveContext) = {
    val properties = new Properties()
    properties.put("driver", ConfigurationUtil.getProperty(Constants.JDBC_DRIVER))
    properties.put("user", ConfigurationUtil.getProperty(Constants.JDBC_XIAOPENG2FX_USER))
    properties.put("password", ConfigurationUtil.getProperty(Constants.JDBC_XIAOPENG2FX_PWD))
    val deptDF = hiveContext.read.jdbc(ConfigurationUtil.getProperty(Constants.JDBC_XIAOPENG2FX_URL), tabName, properties)
    deptDF.registerTempTable(tabName)
    hiveContext.cacheTable(tabName)
  }


  /**
    * 读取  xiaopeng2_faxing fx_cost_advertisement 表 并注册相应的表
    *
    * @param sc
    * @param hiveContext
    * @param startTime
    * @param endTime
    *
    * 可以带时间参数，在时间范围小的情况下，不会造成内出溢出
    */
  def readXiaopeng2FxCostAdvTable(sc: SparkContext, hiveContext: HiveContext, startTime: String, endTime: String) = {
    val conn: Connection = JdbcUtil.getXiaopeng2FXConn();
    val sql = "select * from fx_cost_advertisement where (cost_date>='startTime' and cost_date<='endTime') or (date(create_time)>='startTime' and date(create_time)<='endTime') or (date(update_time)>='startTime' and date(update_time)<='endTime')"
      .replace("startTime", startTime).replace("endTime", endTime)
    val stmt = conn.createStatement();
    val results = stmt.executeQuery(sql);
    var rows = new ArrayBuffer[Row]()
    while (results.next()) {
      try {
        rows.+=(RowFactory.create(Integer.valueOf(results.getString(1)), Integer.valueOf(results.getString(2)), Integer.valueOf(results.getString(3)), results.getString(4), results.getString(5), results.getString(6), results.getString(7), results.getString(8), Integer.valueOf(results.getString(9)), Integer.valueOf(results.getString(10)), Integer.valueOf(results.getString(11)), results.getString(12)));
      } catch {
        case e: Exception => {
          println("fx_cost_advertisement add data error:" + e)
        }
      }
    }
    val rowsRDD = sc.parallelize(rows);
    val schema = (new StructType).add("id", IntegerType).add("game_parent_id", IntegerType).add("game_sub_id", IntegerType).add("pkg_id", StringType).add("spid", StringType).add("cost_date", StringType).add("create_time", StringType).add("update_time", StringType).add("consume_sum", IntegerType).add("actual_cost", IntegerType).add("status", IntegerType).add("topup_wallet_ids", StringType)
    hiveContext.createDataFrame(rowsRDD, schema).persist().registerTempTable("fx_cost_advertisement");

  }

  /**
    * 读取  xiaopeng2_faxing fx_cost_advertisement 表 并注册相应的表
    *
    * @param sc
    * @param hiveContext
    * @param startTime
    * @param endTime
    *
    * 可以带时间参数，在时间范围小的情况下，不会造成内出溢出
    */
  def readXiaopeng2FxCostAgentTable(sc: SparkContext, hiveContext: HiveContext, startTime: String, endTime: String) = {
    val conn: Connection = JdbcUtil.getXiaopeng2FXConn();
    val sql = "select  * from fx_cost_agent where (cost_date>='startTime' and cost_date<='endTime') or (date(create_time)>='startTime' and date(create_time)<='endTime') or (date(update_time)>='startTime' and date(update_time)<='endTime')"
      .replace("startTime", startTime).replace("endTime", endTime)
    val stmt = conn.createStatement();
    val results = stmt.executeQuery(sql);
    var rows = new ArrayBuffer[Row]()
    while (results.next()) {
      try {
        rows.+=(RowFactory.create(Integer.valueOf(results.getString(1)), Integer.valueOf(results.getString(2)), Integer.valueOf(results.getString(3)), results.getString(4), results.getString(5), results.getString(6), results.getString(7), results.getString(8), Integer.valueOf(results.getString(9)), Integer.valueOf(results.getString(10)), Integer.valueOf(results.getString(11))));
      } catch {
        case e: Exception => {
          println("fx_cost_agent add data error:" + e)
        }
      }
    }
    val rowsRDD = sc.parallelize(rows);
    val schema = (new StructType).add("id", IntegerType).add("game_parent_id", IntegerType).add("game_sub_id", IntegerType).add("pkg_id", StringType).add("spid", StringType).add("cost_date", StringType).add("create_time", StringType).add("update_time", StringType).add("cost_modify", IntegerType).add("actual_cost", IntegerType).add("status", IntegerType)
    hiveContext.createDataFrame(rowsRDD, schema).persist().registerTempTable("fx_cost_agent");

  }

  /**
    * 读取 bi 的bi_gamepublic_active_detail 表 并注册相应的表
    *
    * @param sc
    * @param hiveContext
    * @param startTime
    * @param endTime
    *
    * 可以带时间参数，在时间范围小的情况下，不会造成内出溢出
    */
  def readActiveDeatials(sc: SparkContext, hiveContext: HiveContext, startTime: String, endTime: String) = {

    val conn: Connection = JdbcUtil.getConn();
    val sql = "select game_id,parent_channel,child_channel,ad_label,imei,active_hour from bi_gamepublic_active_detail where date(active_hour)>='startTime' and  date(active_hour)<='endTime'"
      .replace("startTime", startTime).replace("endTime", endTime)
    val stmt = conn.createStatement();
    val results = stmt.executeQuery(sql);
    var rows = new ArrayBuffer[Row]()
    while (results.next()) {
      rows.+=(RowFactory.create(Integer.valueOf(results.getString(1)), results.getString(2), results.getString(3), results.getString(4), results.getString(5), results.getString(6)));
    }
    val rowsRDD = sc.parallelize(rows);
    val schema = (new StructType).add("game_id", IntegerType).add("parent_channel", StringType).add("child_channel", StringType).add("ad_label", StringType).add("imei", StringType).add("active_hour", StringType)
    hiveContext.createDataFrame(rowsRDD, schema).persist().registerTempTable("bi_gamepublic_active_detail");

  }

  /**
    * 读取 bi 的 bi_gamepublic_regi_detail 表 并注册相应的表
    *
    * @param sc
    * @param hiveContext
    * @param startTime
    * @param endTime
    *
    * 可以带时间参数，在时间范围小的情况下，不会造成内出溢出
    */
  def readRegiDeatials(sc: SparkContext, hiveContext: HiveContext, startTime: String, endTime: String) = {

    val conn: Connection = JdbcUtil.getConn();
    val sql = "select game_id,parent_channel,child_channel,ad_label,imei,regi_hour from bi_gamepublic_regi_detail where date(regi_hour)>='startTime' and  date(regi_hour)<='endTime'"
      .replace("startTime", startTime).replace("endTime", endTime)
    val stmt = conn.createStatement();
    val results = stmt.executeQuery(sql);
    var rows = new ArrayBuffer[Row]()
    while (results.next()) {
      rows.+=(RowFactory.create(Integer.valueOf(results.getString(1)), results.getString(2), results.getString(3), results.getString(4), results.getString(5), results.getString(6)));
    }
    val rowsRDD = sc.parallelize(rows);
    val schema = (new StructType).add("game_id", IntegerType).add("parent_channel", StringType).add("child_channel", StringType).add("ad_label", StringType).add("imei", StringType).add("regi_hour", StringType)
    hiveContext.createDataFrame(rowsRDD, schema).persist().registerTempTable("bi_gamepublic_regi_detail");
  }

  /**
    * 读取 bi 的 bi_publish_back_performance 表 并注册相应的表
    *
    * @param sc
    * @param hiveContext
    * @param startTime
    * @param endTime
    *
    * 可以带时间参数，在时间范围小的情况下，不会造成内出溢出
    */
  def readPerformance(sc: SparkContext, hiveContext: HiveContext, startTime: String, endTime: String) = {

    val conn: Connection = JdbcUtil.getConn();
    val sql = "select performance_time,medium_channel,ad_site_channel,pkg_code,parent_game_id,child_game_id,pay_water,box_water,main_man,regi_num,pay_type from bi_publish_back_performance where performance_time >='startTime' and  performance_time <='endTime'"
      .replace("startTime", startTime).replace("endTime", endTime)
    val stmt = conn.createStatement();
    val results = stmt.executeQuery(sql);
    var rows = new ArrayBuffer[Row]()
    while (results.next()) {
      rows.+=(RowFactory.create(results.getDate(1).toString, results.getString(2), results.getString(3), results.getString(4), Integer.valueOf(results.getString(5)), Integer.valueOf(results.getString(6)), Integer.valueOf(results.getString(7)), Integer.valueOf(results.getString(8)), results.getString(9), Integer.valueOf(results.getString(10)), results.getString(11)));
    }
    val rowsRDD = sc.parallelize(rows);
    val schema = (new StructType).add("performance_time", StringType).add("medium_channel", StringType).add("ad_site_channel", StringType).add("pkg_code", StringType).add("parent_game_id", IntegerType).add("child_game_id", IntegerType).add("pay_water", IntegerType).add("box_water", IntegerType).add("main_man", StringType).add("regi_num", IntegerType).add("pay_type", StringType)
    hiveContext.createDataFrame(rowsRDD, schema).persist().registerTempTable("bi_publish_back_performance");
  }

  /**
    * 读取 发行联运管理 激活表明细
    *
    * @param sc
    * @param hiveContext
    * @param startTime
    * @param endTime
    */
  def readAllianceActiveDeatials(sc: SparkContext, hiveContext: HiveContext, startTime: String, endTime: String) = {
    val conn: Connection = JdbcUtil.getConn();
    val sql = "select game_id,alliance_bag_id,imei,active_time from bi_gamepublic_alliance_active_detail where date(active_time)>='startTime' and  date(active_time)<='endTime'"
      .replace("startTime", startTime).replace("endTime", endTime)
    val stmt = conn.createStatement();
    val results = stmt.executeQuery(sql);
    var rows = new ArrayBuffer[Row]()
    while (results.next()) {
      rows.+=(RowFactory.create(Integer.valueOf(results.getString(1)), results.getString(2), results.getString(3), results.getString(4)));
    }
    val rowsRDD = sc.parallelize(rows);
    val schema = (new StructType).add("game_id", IntegerType).add("alliance_bag_id", StringType).add("imei", StringType).add("active_time", StringType)
    hiveContext.createDataFrame(rowsRDD, schema).persist().registerTempTable("bi_gamepublic_alliance_active_detail");

  }

  /**
    * 读取聚合SDK订单交换表
    *
    * @param sc
    * @param hiveContext
    * @param startTime
    * @param endTime
    */
  def readCommonSdkOrderTran(sc: SparkContext, hiveContext: HiveContext, startTime: String, endTime: String) = {
    val connXP2 = JdbcUtil.getXiaopeng2Conn()
    val sql = "select channel_order_sn,pyw_order_sn from common_sdk_order_tran where date(create_time)>=date_add('startTime',interval -1 day) and date(create_time)<='endTime'"
      .replace("startTime", startTime).replace("endTime", endTime)
    val stmt = connXP2.createStatement()
    val rs = stmt.executeQuery(sql)
    val rows = new ArrayBuffer[Row]()
    while (rs.next()) {
      rows.+=(Row(rs.getString("channel_order_sn"), rs.getString("pyw_order_sn")))
    }
    rs.close()
    stmt.close()
    connXP2.close()
    val rowsRDD = sc.parallelize(rows)
    val struct = new StructType().add("channel_order_sn", StringType).add("pyw_order_sn", StringType)
    hiveContext.createDataFrame(rowsRDD, struct).persist().registerTempTable("common_sdk_order_tran")
  }

  /**
    * 读取聚合SDK订单付款表
    *
    * @param sc
    * @param hiveContext
    * @param startTime
    * @param endTime
    */
  def readCommonSdkOrderPay(sc: SparkContext, hiveContext: HiveContext, startTime: String, endTime: String) = {
    val connXP2 = JdbcUtil.getXiaopeng2Conn()
    val sql = "select channel_order_sn,is_sandbox_pay from common_sdk_order_pay where date(create_time)>=date_add('startTime',interval -1 day) and date(create_time)<='endTime'"
      .replace("startTime", startTime).replace("endTime", endTime)
    val stmt = connXP2.createStatement()
    val rs = stmt.executeQuery(sql)
    val rows = new ArrayBuffer[Row]()
    while (rs.next()) {
      rows.+=(Row(rs.getString("channel_order_sn"), rs.getInt("is_sandbox_pay")))
    }
    rs.close()
    stmt.close()
    connXP2.close()
    val rowsRDD = sc.parallelize(rows)
    val struct = new StructType().add("channel_order_sn", StringType).add("is_sandbox_pay", IntegerType)
    hiveContext.createDataFrame(rowsRDD, struct).persist().registerTempTable("common_sdk_order_pay")
  }
}
