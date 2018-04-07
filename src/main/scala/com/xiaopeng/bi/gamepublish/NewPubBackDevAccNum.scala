package com.xiaopeng.bi.gamepublish

import com.xiaopeng.bi.udf.ChannelUDF
import com.xiaopeng.bi.utils.{JdbcUtil, SparkUtils}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by bigdata on 17-7-25.
  * 注册设备统计 ，统计维度到game_id  -- 运营报表  2017-7-25
  * 注册帐号数统计，第 n 日充值帐号数统计  --运营报表  2017-8-26
  */
object NewPubBackDevAccNum {

  var startDay = "";
  var endDay = ""

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
    if (args.length == 2) {
      startDay = args(0)
      endDay = args(1)
    } else if (args.length == 1) {
      startDay = args(0)
      endDay = startDay
    }
    // 创建上下文
    val sparkConf = new SparkConf().setAppName(this.getClass.getName.replace("$", ""))
      .set("spark.default.parallelism", "60")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.shuffle.consolidateFiles", "true")
      .set("spark.storage.memoryFraction", "0.4")
      .set("spark.streaming.stopGracefullyOnShutdown", "true")
    SparkUtils.setMaster(sparkConf)

    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)

    //注册设备统计 ，统计维度到game_id  -- 运营报表
    sqlContext.sql("use yyft")

    val devAccSqloper = "select  rz2.reg_time, \nrz2.game_id, \nsum(case when imei = '' or imei is null then rz2.acc_count else 0 end) as amount_no_dev_num,   \nsum(case when imei != '' and imei is not null and rz2.acc_count=1 then 1 else 0 end ) as dev_acc_1, \nsum(case when imei != '' and imei is not null and rz2.acc_count>=2 and rz2.acc_count<=5 then 1 else 0 end ) as dev_acc_2_5, \nsum(case when imei != '' and imei is not null and rz2.acc_count>=6 and rz2.acc_count<=10 then 1 else 0 end ) as dev_acc_6_10, \nsum(case when imei != '' and imei is not null and rz2.acc_count>=11 and rz2.acc_count<=20 then 1 else 0 end ) as dev_acc_11_20, \nsum(case when imei != '' and imei is not null and rz2.acc_count>=21 and rz2.acc_count<=50 then 1 else 0 end ) as dev_acc_21_50, \nsum(case when imei != '' and imei is not null and rz2.acc_count>=51 and rz2.acc_count<=100 then 1 else 0 end ) as dev_acc_51_100, \nsum(case when imei != '' and imei is not null and rz2.acc_count>=101 and rz2.acc_count<=200 then 1 else 0 end ) as dev_acc_101_200, \nsum(case when imei != '' and imei is not null and rz2.acc_count>=201 then 1 else 0 end ) as dev_acc_201 \nfrom \n(select  rz.reg_time reg_time, rz.game_id game_id, rz.imei imei, count(rz.game_account) acc_count \nfrom ( select distinct to_date(reg_time) reg_time,game_id,if(imei='00000000000000000000000000000000','',imei) imei,lower(trim(game_account)) game_account \nfrom ods_regi_rz where game_id is not null and game_account is not null and reg_time is not null  and to_date(reg_time)>='startDay' and to_date(reg_time)<='endDay') rz  \njoin (select distinct game_id as parent_game_id,old_game_id,system_type,group_id from game_sdk where state=0) gs on rz.game_id = gs.old_game_id group by rz.reg_time,rz.game_id,rz.imei) rz2  \ngroup by rz2.reg_time,rz2.game_id"
    val devAccexecSqloper = devAccSqloper.replace("startDay", startDay).replace("endDay", endDay);
    val deviceRetainedDfoper = sqlContext.sql(devAccexecSqloper).registerTempTable("opera_device_acc")
    val devAccSql = "select oda.*,gs.game_id parent_game_id,gs.system_type os,gs.group_id from opera_device_acc oda join game_sdk gs on oda.game_id = gs.old_game_id where gs.state=0"
    val operaDeviceRetainedDf = sqlContext.sql(devAccSql)
    operaDeviceAccNumForeachPartition(operaDeviceRetainedDf)




    sc.stop()
  }

  private def operaDeviceAccNumForeachPartition(deviceRetainedDf: DataFrame) = {
    deviceRetainedDf.foreachPartition(rows => {

      val conn = JdbcUtil.getConn()
      val statement = conn.createStatement

      val sqlText = "insert into bi_gamepublic_opera_actions(publish_date,child_game_id,amount_no_dev_num,dev_acc_1,dev_acc_2_5,dev_acc_6_10,dev_acc_11_20,dev_acc_21_50,dev_acc_51_100,dev_acc_101_200,dev_acc_201,parent_game_id,os,group_id)\nvalues(?,?,?,?,?,?,?,?,?,?,?,?,?,?)\non duplicate key update \namount_no_dev_num=VALUES(amount_no_dev_num),\ndev_acc_1=VALUES(dev_acc_1),\ndev_acc_2_5=VALUES(dev_acc_2_5),\ndev_acc_6_10=VALUES(dev_acc_6_10),\ndev_acc_11_20=VALUES(dev_acc_11_20),\ndev_acc_21_50=VALUES(dev_acc_21_50),\ndev_acc_51_100=VALUES(dev_acc_51_100),\ndev_acc_101_200=VALUES(dev_acc_101_200),\ndev_acc_201=VALUES(dev_acc_201)"
      val params = new ArrayBuffer[Array[Any]]()
      for (insertedRow <- rows) {
        params.+=(Array[Any](insertedRow.get(0), insertedRow.get(1), insertedRow.get(2), insertedRow.get(3),
          insertedRow.get(4), insertedRow.get(5), insertedRow.get(6), insertedRow.get(7), insertedRow.get(8),
          insertedRow.get(9), insertedRow(10), insertedRow.get(11), insertedRow.get(12), insertedRow(13)))
      }
      try {
        JdbcUtil.doBatch(sqlText, params, conn)
      } finally {
        statement.close()
        conn.close
      }
    })
  }




}
