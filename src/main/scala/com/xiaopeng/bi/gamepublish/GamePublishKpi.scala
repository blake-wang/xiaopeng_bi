package com.xiaopeng.bi.gamepublish

import java.sql.Connection

import com.xiaopeng.bi.utils._
import com.xiaopeng.bi.utils.action.GamePublicActs2
import kafka.serializer.StringDecoder
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by kequan on 5/8/17.
  * 注册 激活 登录  支付 相关
  */
object GamePublishKpi {
  var arg = "60"

  def main(args: Array[String]): Unit = {
    //    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    //    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
    if (arg.length > 0) {
      arg = args(0)
    }
    val ssc = StreamingContext.getOrCreate(ConfigurationUtil.getProperty("spark.checkpoint.kpi"), getStreamingContext _);
    ssc.start();

    ssc.awaitTermination();
  }

  def getStreamingContext(): StreamingContext = {
    //创建各种上下文
    val sparkConf = new SparkConf().setAppName(this.getClass.getName.replace("$", ""))
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.shuffle.consolidateFiles", "true")
      .set("spark.sql.shuffle.partitions", "60")
      .set("spark.default.parallelism", "60")

    SparkUtils.setMaster(sparkConf);
    val sc = new SparkContext(sparkConf);
    val ssc = new StreamingContext(sc, Seconds(arg.toInt));
    // 获取kafka的数据
    val Array(brokers, topics) = Array(ConfigurationUtil.getProperty("kafka.metadata.broker.list"), ConfigurationUtil.getProperty("kafka.topics.kpi"));
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers);
    val topicsSet = topics.split(",").toSet;
    val dslogs: DStream[String] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet).map(_._2)
    dslogs.foreachRDD(rdd => {

      val sc = rdd.sparkContext
      val hiveContext: HiveContext = HiveContextSingleton.getInstance(sc)

      // 基本维度信息
      publicFxGgameTbPush2Redis.publicGgameTbPush2Redis();
      // 把 以前的日志 bi_pubgame 和 本次实时的日志 bi_pubgame 相加
      StreamingUtils.convertPubGameLogsToDfTmpTable(rdd, hiveContext)
      // 处理激活日志
      GamePublicActiveUtil.loadActiveInfo(rdd, hiveContext)
      // 处理注册日志
      GamePublicRegiUtil.loadRegiInfo(rdd, hiveContext)
      // 处理登录数据
      GamePublicActs2.loadLoginInfo(rdd);
      // 投放报表支付数据
      GamePublicActs2.loadOrderInfo(rdd);
      // 运营报表支付数据
      GamePublicPayUtil.loadPayInfo(rdd, hiveContext);
      // 处理点击数据
      GamePublicClickUtil.loadClickInfo(rdd, hiveContext);
      // 广告检测钱大师
      AdMoneyMasterUtil.loadMoneyMaterInfo(rdd, hiveContext)
      // 刷榜信息导入
      DimensionUtil.updateRankDayDim(hiveContext, DateUtils.getYesterDayDate(), DateUtils.getTodayDate())
      // 把投放的数据实时汇总到运营; 把 投放和运营的数据汇总到刷榜
      migrationData()
    })

    ssc.checkpoint(ConfigurationUtil.getProperty("spark.checkpoint.kpi"));
    ssc;
  }

  /**
    * 数据迁移
    */
  def migrationData() = {
    val conn: Connection = JdbcUtil.getConn();
    val stmt = conn.createStatement();
    val startHour = DateUtils.getCriticalHour();
    val endHour = DateUtils.getDateHour();

    // 1.把投放数据实时汇总到运营
    val sql_pkg2oper = "insert into bi_gamepublic_base_opera_hour_kpi(publish_time,parent_game_id,child_game_id,pay_money,pay_account_num,regi_account_num,os,group_id)\nselect publish_time,parent_game_id,child_game_id,pay_money,pay_account_num,regi_account_num,os,group_id  from \n(\nselect publish_time,parent_game_id,game_id child_game_id,sum(pay_money_new) pay_money,sum(pay_account_num) pay_account_num,sum(regi_account_num) regi_account_num,os,group_id from  bi_gamepublic_basekpi \nwhere  publish_time>='startHour' and publish_time<='endHour' group by parent_game_id,game_id,publish_time,os,group_id\n) rs\non DUPLICATE key update pay_money= rs.pay_money,pay_account_num=rs.pay_account_num,regi_account_num= rs.regi_account_num,parent_game_id=rs.parent_game_id,os=rs.os,group_id=rs.group_id"
      .replace("startHour", startHour + ":00:00").replace("endHour", endHour + ":00:00");
    stmt.execute(sql_pkg2oper)

    // 2.把投放数据实时汇总到刷榜 由于录入刷帮信息 不会延迟太久，所以，只取最近 30 天的数据导入
    val endDay = DateUtils.getTodayDate()
    val startDay = DateUtils.LessDay(endDay, 30)

    val sql_pkg2rank = "update  bi_gamepublic_opera_rank_day bgord inner join\n(\nselect parent_game_id,publish_date, \nchild_game_id,\nsum(regi_account_num) regi_account_num\nfrom bi_gamepublic_base_day_kpi where publish_date>='startDay' and publish_date<='endDay' group by parent_game_id,publish_date,child_game_id\n)rs on bgord.publish_date =rs.publish_date and bgord.child_game_id =rs.child_game_id\nset  \nbgord.regi_account_num=rs.regi_account_num"
      .replace("startDay", startDay).replace("endDay", endDay);
    stmt.execute(sql_pkg2rank)

    // 3.把运营数据实时汇总到刷榜 由于 是 累计，所以数据会一直更新，这个必须全量更新 2017-06-01 到 今天
    val sql_oper2rank = "update  bi_gamepublic_opera_rank_day bgord inner join\n(\nselect\npublish_date,\nchild_game_id,\nregi_device_num,\nrecharge_price,\nrecharge_accounts,\nactive_num,\nltv_1day_b,\nltv_2day_b,\nltv_3day_b,\nltv_7day_b,\nltv_30day_b,\nretained_1day acc_retained_2day,\nretained_3day acc_retained_3day,\nretained_7day acc_retained_7day,\nretained_30day acc_retained_30day\nfrom bi_gamepublic_base_opera_kpi where publish_date>='startDay' and publish_date<='endDay'\n)rs on bgord.publish_date =rs.publish_date and bgord.child_game_id =rs.child_game_id\nset  \nbgord.regi_device_num=rs.regi_device_num,\nbgord.recharge_price=rs.recharge_price,\nbgord.recharge_accounts=rs.recharge_accounts,\nbgord.active_num=rs.active_num,\nbgord.ltv_1day_b=rs.ltv_1day_b,\nbgord.ltv_2day_b=rs.ltv_2day_b,\nbgord.ltv_3day_b=rs.ltv_3day_b,\nbgord.ltv_7day_b=rs.ltv_7day_b,\nbgord.ltv_30day_b=rs.ltv_30day_b,\nbgord.acc_retained_2day=rs.acc_retained_2day,\nbgord.acc_retained_3day=rs.acc_retained_3day,\nbgord.acc_retained_7day=rs.acc_retained_7day,\nbgord.acc_retained_30day=rs.acc_retained_30day"
      .replace("startDay", "2017-06-01").replace("endDay", endDay);
    stmt.execute(sql_oper2rank)

    stmt.close()
    conn.close();
  }

}
