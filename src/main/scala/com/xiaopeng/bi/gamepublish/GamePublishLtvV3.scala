package com.xiaopeng.bi.gamepublish

import com.xiaopeng.bi.utils._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

/**
  * ltv: 用户价值分析，注册和支付
  */
object GamePublishLtvV3 {
  val logger = Logger.getLogger(GamePublishLtvV3.getClass)

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
    val yesterday = args(0)
    // 创建各种上下文
    val sparkConf = new SparkConf().setAppName(this.getClass.getName.replace("$", ""))
      .set("spark.default.parallelism", "60")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.shuffle.consolidateFiles", "true")
      .set("spark.storage.memoryFraction", "0.4")
      .set("spark.streaming.stopGracefullyOnShutdown", "true")
    SparkUtils.setMaster(sparkConf)
    val sc = new SparkContext(sparkConf)
    val hiveContext = new HiveContext(sc)

    //一.分析数据
    hiveContext.sql("use yyft")
    val resultSql = "select  \nrz.reg_time as publish_date, \ngs.parent_game_id, \nrz.game_id, \nif(rz.expand_channel is null or rz.expand_channel='','21',rz.expand_channel) as expand_channel, \ngs.os, gs.group_id, \nsum(case when datediff(to_date(oz.order_time),rz.reg_time)=0 and oz.ori_price_all  is not null then oz.ori_price_all else 0 end)*100 as ltv1_amount, \nsum(case when datediff(to_date(oz.order_time),rz.reg_time)<=1 and oz.ori_price_all is not null then oz.ori_price_all else 0 end)*100 as ltv2_amount, \nsum(case when datediff(to_date(oz.order_time),rz.reg_time)<=2 and oz.ori_price_all is not null then oz.ori_price_all else 0 end)*100 as ltv3_amount, \nsum(case when datediff(to_date(oz.order_time),rz.reg_time)<=3 and oz.ori_price_all is not null then oz.ori_price_all else 0 end)*100 as ltv4_amount, \nsum(case when datediff(to_date(oz.order_time),rz.reg_time)<=4 and oz.ori_price_all is not null then oz.ori_price_all else 0 end)*100 as ltv5_amount, \nsum(case when datediff(to_date(oz.order_time),rz.reg_time)<=5 and oz.ori_price_all is not null then oz.ori_price_all else 0 end)*100 as ltv6_amount, \nsum(case when datediff(to_date(oz.order_time),rz.reg_time)<=6 and oz.ori_price_all is not null then oz.ori_price_all else 0 end)*100 as ltv7_amount, \nsum(case when datediff(to_date(oz.order_time),rz.reg_time)<=7 and oz.ori_price_all is not null then oz.ori_price_all else 0 end)*100 as ltv8_amount, \nsum(case when datediff(to_date(oz.order_time),rz.reg_time)<=8 and oz.ori_price_all is not null then oz.ori_price_all else 0 end)*100 as ltv9_amount, \nsum(case when datediff(to_date(oz.order_time),rz.reg_time)<=9 and oz.ori_price_all is not null then oz.ori_price_all else 0 end)*100 as ltv10_amount, \nsum(case when datediff(to_date(oz.order_time),rz.reg_time)<=10 and oz.ori_price_all is not null then oz.ori_price_all else 0 end)*100 as ltv11_amount, \nsum(case when datediff(to_date(oz.order_time),rz.reg_time)<=11 and oz.ori_price_all is not null then oz.ori_price_all else 0 end)*100 as ltv12_amount, \nsum(case when datediff(to_date(oz.order_time),rz.reg_time)<=12 and oz.ori_price_all is not null then oz.ori_price_all else 0 end)*100 as ltv13_amount, \nsum(case when datediff(to_date(oz.order_time),rz.reg_time)<=13 and oz.ori_price_all is not null then oz.ori_price_all else 0 end)*100 as ltv14_amount, \nsum(case when datediff(to_date(oz.order_time),rz.reg_time)<=14 and oz.ori_price_all is not null then oz.ori_price_all else 0 end)*100 as ltv15_amount, \nsum(case when datediff(to_date(oz.order_time),rz.reg_time)<=29 and oz.ori_price_all is not null then oz.ori_price_all else 0 end)*100 as ltv30_amount, \nsum(case when datediff(to_date(oz.order_time),rz.reg_time)<=44 and oz.ori_price_all is not null then oz.ori_price_all else 0 end)*100 as ltv45_amount, \nsum(case when datediff(to_date(oz.order_time),rz.reg_time)<=59 and oz.ori_price_all is not null then oz.ori_price_all else 0 end)*100 as ltv60_amount, \nsum(case when datediff(to_date(oz.order_time),rz.reg_time)<=89 and oz.ori_price_all is not null then oz.ori_price_all else 0 end)*100 as ltv90_amount, \nsum(case when datediff(to_date(oz.order_time),rz.reg_time)<=179 and oz.ori_price_all is not null then oz.ori_price_all else 0 end)*100 as ltv180_amount, \nsum(case when oz.ori_price_all is not null then oz.ori_price_all else 0 end)*100 as ltvall_amount\nfrom \n(select  distinct game_id,lower(trim(game_account)) game_account,to_date(reg_time) reg_time,expand_channel from ods_regi_rz where game_id is not null and game_account is not null and reg_time is not null and  to_date(reg_time)<='yesterday' and to_date (reg_time) >= date_add('yesterday',-180))  \nrz \njoin (select distinct game_id  from ods_publish_game) ods_publish_game on rz.game_id=ods_publish_game.game_id  --过滤发行游戏 \njoin  (select  distinct game_id as parent_game_id , old_game_id as child_game_id,system_type as os,group_id from game_sdk) gs on rz.game_id=gs.child_game_id --补全 main_id \nleft  join  (select distinct order_no,order_time,lower(trim(game_account)) as game_account,game_id,(if(ori_price is null,0,ori_price)+if(total_amt is null,0,total_amt)) as ori_price_all,total_amt,payment_type,order_status from ods_order where order_status=4 and prod_type=6 and to_date(order_time)>=date_add('yesterday',-180) and to_date(order_time)<='yesterday') oz on  oz.game_account=rz.game_account--支付信息  \ngroup by rz.reg_time,gs.parent_game_id,rz.game_id,if(rz.expand_channel is null or rz.expand_channel='','21',rz.expand_channel),gs.os,gs.group_id"
      .replace("yesterday", yesterday)
    val resultDf = hiveContext.sql(resultSql)

    //二.把结果存入mysql
    resultDf.foreachPartition(rows => {
      val sqlText = "insert into bi_gamepublic_actions(publish_date,parent_game_id,child_game_id,medium_channel,ad_site_channel,pkg_code,os,group_id,recharge_lj_1,recharge_lj_2,recharge_lj_3,recharge_lj_4,recharge_lj_5,recharge_lj_6,recharge_lj_7,recharge_lj_8,recharge_lj_9,recharge_lj_10,recharge_lj_11,recharge_lj_12,recharge_lj_13,recharge_lj_14,recharge_lj_15,recharge_lj_30,recharge_lj_45,recharge_lj_60,recharge_lj_90,recharge_lj_180,recharge_price) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)\non duplicate key update recharge_lj_1=VALUES(recharge_lj_1),\nrecharge_lj_2=VALUES(recharge_lj_2),\nrecharge_lj_3=VALUES(recharge_lj_3),\nrecharge_lj_4=VALUES(recharge_lj_4),\nrecharge_lj_5=VALUES(recharge_lj_5),\nrecharge_lj_6=VALUES(recharge_lj_6),\nrecharge_lj_7=VALUES(recharge_lj_7),\nrecharge_lj_8=VALUES(recharge_lj_8),\nrecharge_lj_9=VALUES(recharge_lj_9),\nrecharge_lj_10=VALUES(recharge_lj_10),\nrecharge_lj_11=VALUES(recharge_lj_11),\nrecharge_lj_12=VALUES(recharge_lj_12),\nrecharge_lj_13=VALUES(recharge_lj_13),\nrecharge_lj_14=VALUES(recharge_lj_14),\nrecharge_lj_15=VALUES(recharge_lj_15),\nrecharge_lj_30=VALUES(recharge_lj_30),\nrecharge_lj_45=VALUES(recharge_lj_45),\nrecharge_lj_60=VALUES(recharge_lj_60),\nrecharge_lj_90=VALUES(recharge_lj_90),\nrecharge_lj_180=VALUES(recharge_lj_180),\nrecharge_price=VALUES(recharge_price)"
      val params = new ArrayBuffer[Array[Any]]()
      for (insertedRow <- rows) {
        val channelArray = StringUtils.getArrayChannel(insertedRow.get(3).toString)
        if (channelArray(0).length <= 10 && channelArray(1).length <= 10 && channelArray(2).length <= 12) {
          params.+=(Array[Any](insertedRow.get(0), insertedRow.get(1), insertedRow.get(2),
            channelArray(0), channelArray(1), channelArray(2),
            insertedRow.get(4), insertedRow.get(5), insertedRow.get(6),
            insertedRow.get(7), insertedRow.get(8), insertedRow.get(9),
            insertedRow.get(10), insertedRow.get(11), insertedRow.get(12),
            insertedRow.get(13), insertedRow.get(14), insertedRow.get(15),
            insertedRow.get(16), insertedRow.get(17), insertedRow.get(18),
            insertedRow.get(19), insertedRow.get(20), insertedRow.get(21),
            insertedRow.get(22), insertedRow.get(23), insertedRow.get(24),
            insertedRow.get(25), insertedRow.get(26)))
        } else {
          logger.error("expand_channel format error: " + insertedRow.get(3) + " - " + insertedRow.get(1))
        }
      }
      JdbcUtil.executeBatch(sqlText, params)
    })
    sc.stop()
  }
}
