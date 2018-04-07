package com.xiaopeng.test

import com.xiaopeng.bi.utils._
import org.apache.spark.sql.Row
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}


/**
  * Created by kequan on 3/27/17
  */
object TestMode {

  var startDay = "";
  var endDay = "";
  var tomorrow = "";

  def main(args: Array[String]): Unit = {

    startDay = args(0)
    endDay = args(1)
    tomorrow = DateUtils.addDay(endDay, 1)

    //创建各种上下文
    val sparkConf = new SparkConf().setAppName(this.getClass.getName.replace("$", ""))
      .set("spark.default.parallelism", "60")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.shuffle.consolidateFiles", "true")
      .set("spark.sql.shuffle.partitions", "60")
    SparkUtils.setMaster(sparkConf);
    val sc = new SparkContext(sparkConf);
    val hiveContext: HiveContext = HiveContextSingleton.getInstance(sc)

    hiveContext.sql("use yyft")

    val rdd = sc.textFile("file:///home/kequan/workspace/xiaopeng_bi/src/test/scala/com/xiaopeng/test/mode.txt")
    val row = rdd.filter(t => {
      t.contains("|");
    }).map(row => {
      val t = row.split("\\|", -1)
      val mode_2 = "晓霞公会"
      val modifytime_2 = "2017-01-01"
      val mode_3 = "付费榜"
      val modifytime_3 = "2017-06-01"

      val saqq = "insert into modes_of_statistic(game_id,mode,modifytime_2,mode_2,modifytime_3,mode_3,modifytime_4,mode_4,modifytime_5,status) values('" + t(0).toInt + "','" + t(1) + "','" + modifytime_2 + "','" + mode_2 + "','" + modifytime_3 + "','" + mode_3 + "','" + "" + "','" + "" + "','" + "" + "','" + 0 + "');"
      FileUtil.apppendTofile("aaa.txt", saqq)

      Row(t(0).toInt, t(1), t(2), t(3), t(4), t(5), "", "", "", "", 0)
    }).collect()

//    val schema = (new StructType).add("game_id", IntegerType).
//      add("mode", StringType)
//      .add("modifytime_2", StringType)
//      .add("mode_2", StringType)
//      .add("modifytime_3", StringType)
//      .add("mode_3", StringType)
//      .add("modifytime_4", StringType)
//      .add("mode_4", StringType)
//      .add("modifytime_5", StringType)
//      .add("mode_5", StringType)
//      .add("status", IntegerType)
//
//
//    val df = hiveContext.createDataFrame(row, schema).persist()
//    df.registerTempTable("modes_of_statistic");
//
//    val sql1 = "select game_id,mode,mode_2,to_date(if(to_date(modifytime_2)>'2016-01-01',modifytime_2,'tomorrow')) modifytime_2,mode_3,to_date(if(to_date(modifytime_3)>'2016-01-01',modifytime_3,'tomorrow')) modifytime_3,mode_4,to_date(if(to_date(modifytime_4)>'2016-01-01',modifytime_4,'tomorrow')) modifytime_4,mode_5, to_date(if(to_date(modifytime_5)>'2016-01-01',modifytime_5,'tomorrow')) modifytime_5 from  modes_of_statistic where status=0"
//      .replace("tomorrow", tomorrow).replace("startDay", startDay).replace("endDay", endDay)
//    val df1 = hiveContext.sql(sql1)
//
//    val sql2 = "select rz.parent_game_id parent_game_id,\nrz.child_game_id child_game_id,\nrz.publish_mode publish_mode,\nrz.os os,\nto_date(oz.order_time) publish_date,\nsum(if(oz.ori_price_all is null,0,oz.ori_price_all)) pay_water,\nsum(if(ver.sandbox is null,0,oz.ori_price_all)) box_water\nfrom \n(\nselect gs.parent_game_id parent_game_id,rz.game_id child_game_id,gs.system_type os, rz.game_account game_account,\ncase\nwhen  rz.reg_time>=ms.modifytime_2  and rz.reg_time<ms.modifytime_3  then  ms.mode_2\nwhen  rz.reg_time>=ms.modifytime_3  and rz.reg_time<ms.modifytime_4  then  ms.mode_3\nwhen  rz.reg_time>=ms.modifytime_4  and rz.reg_time<ms.modifytime_5  then  ms.mode_4\nwhen  rz.reg_time>=ms.modifytime_5  then  ms.mode_5\nelse  ms.mode end as publish_mode\nfrom \n(select  distinct game_id,lower(trim(game_account)) game_account, to_date(reg_time) reg_time from ods_regi_rz where game_id is not null and game_account is not null and reg_time is not null)  rz\njoin (select  distinct game_id as parent_game_id,old_game_id ,system_type from game_sdk  where state=0) gs on rz.game_id=gs.old_game_id\njoin (select game_id,mode,mode_2,to_date(if(to_date(modifytime_2)>'2016-01-01',modifytime_2,'tomorrow')) modifytime_2,mode_3,to_date(if(to_date(modifytime_3)>'2016-01-01',modifytime_3,'tomorrow')) modifytime_3,mode_4,to_date(if(to_date(modifytime_4)>'2016-01-01',modifytime_4,'tomorrow')) modifytime_4,mode_5, to_date(if(to_date(modifytime_5)>'2016-01-01',modifytime_5,'tomorrow')) modifytime_5 from  modes_of_statistic where status=0) ms on ms.game_id=rz.game_id\n) rz\njoin (select distinct order_no,order_time, lower(trim(game_account)) game_account,game_id,(if(ori_price is null,0,ori_price)+if(total_amt is null,0,total_amt)) as ori_price_all,payment_type,order_status from ods_order where order_status=4 and  to_date(order_time)>='startDay' and   to_date(order_time)<='endDay' and prod_type=6)oz on  oz.game_account=rz.game_account\nleft join  (select order_sn,sandbox from pywsdk_apple_receipt_verify where sandbox=1 and state=3) ver on ver.order_sn=oz.order_no\ngroup by to_date(oz.order_time),rz.parent_game_id,rz.child_game_id,rz.publish_mode,rz.os"
//      .replace("tomorrow", tomorrow).replace("startDay", startDay).replace("endDay", endDay)
//    val df2 = hiveContext.sql(sql2).count()

    //    hiveContext.sql("create table rz_mode as select * from rz order  by  parent_game_id,child_game_id,publish_mode,os,publish_date ASC")

  }
}
