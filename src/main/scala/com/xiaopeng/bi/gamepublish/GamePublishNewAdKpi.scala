package com.xiaopeng.bi.gamepublish

import java.sql.Connection

import com.xiaopeng.bi.utils._
import kafka.serializer.StringDecoder
import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by bigdata on 18-3-7.
  */
object GamePublishNewAdKpi {
  var arg = "120"

  def main(args: Array[String]): Unit = {
    //测试环境，不打印日志
    val environment = ConfigurationUtil.getEnvProperty("env.conf")
    if (!environment.contains("product")) {
      Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
      Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
    }

    if (args.length > 0) {
      arg = args(0)
    }

    val ssc = StreamingContext.getOrCreate(ConfigurationUtil.getProperty("spark.checkpoint.newad.kpi"), getStreamingContext _)
    ssc.start()
    ssc.awaitTermination()
  }

  def getStreamingContext: StreamingContext = {
    val sparkConf = new SparkConf().setAppName(this.getClass.getName.replace("$", ""))
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.shuffle.consolidateFiles", "true")
      .set("spark.sql.shuffle.partitions", "60")
      .set("spark.default.parallelism", "60")
    SparkUtils.setMaster(sparkConf)
    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc, Seconds(arg.toInt))
    val Array(brokers, topics) = Array(ConfigurationUtil.getProperty("kafka.metadata.broker.list"), ConfigurationUtil.getProperty("kafka.topics.newad.kpi"));
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers);
    val topicsSet = topics.split(",").toSet;
    val dslogs: DStream[String] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet).map(_._2)
    dslogs.foreachRDD(rdd => {

      val sc = rdd.sparkContext
      val hiveContext = HiveContextSingleton.getInstance(sc)

      //处理广告平台来的日志
      GamePublicNewAdThirddataUtils.loadThirddataInfo(rdd)

      //实时从运营天表和投放天表导自然量数据
      copyDataFromOpkpiToAdkpi()

    })

    ssc.checkpoint(ConfigurationUtil.getProperty("spark.checkpoint.newad.kpi"))
    ssc

  }

  /**
    * 下面指标，可以直接从运营天表,投放天表导数据
    * 激活数 active_num                 运营天表
    * 注册设备数 regi_dev_num           运营天表
    * 新增注册设备数 new_regi_dev_num    运营天表
    * 充值设备数 pay_dev_num            运营天表
    * 新增充值设备数 new_pay_dev_num    运营天表
    * 新增充值金额 new_pay_price       运营天表
    * 充值金额 pay_price                      投放天表
    * 落地页点击次数 request_click_num    运营天表
    * 落地页点击IP去重数 request_click_uv 运营天表
    * 落地页下载次数 download_num        运营天表
    * 落地页下载IP去重数 download_uv     运营天表
    * 激活注册设备数 active_regi_dev_num 运营天表
    * 新增活跃设备数 new_active_dev_num 运营天表
    */
  def copyDataFromOpkpiToAdkpi(): Unit = {
    val conn: Connection = JdbcUtil.getConn
    val stmt = conn.createStatement()
    val currentday = DateUtils.getTodayDate()
    val yesterDay = DateUtils.addDay(currentday, -1)

    //1:从投放基础表导request_click_num,download_num到运营天表
    val update_sql = "update bi_gamepublic_base_opera_kpi opkpi \njoin (select publish_date,child_game_id,sum(request_click_num) request_click_num,sum(download_num) download_num from bi_gamepublic_base_day_kpi where publish_date='currentday' group by publish_date,child_game_id) basekpi on opkpi.publish_date=basekpi.publish_date and opkpi.child_game_id=basekpi.child_game_id \nset opkpi.request_click_num=basekpi.request_click_num,opkpi.download_num=basekpi.download_num"
      .replace("currentday", currentday)
    stmt.executeUpdate(update_sql)

    //2:从运营天表导自然量数据到广告监测
    //获取广告监测游戏id的集合
    val game_id_set = getThirddataGameId(conn, currentday)

    val op_kpi_sql = "select\nopkpi.publish_date,\nopkpi.child_game_id,\nopkpi.parent_game_id,\nopkpi.os,\nopkpi.group_id,\nopkpi.request_click_num - adkpi.request_click_num as request_click_num,\nopkpi.request_click_uv - adkpi.request_click_uv as request_click_uv,\nopkpi.download_num - adkpi.download_num as download_num,\nopkpi.download_uv - adkpi.download_uv as download_uv,\nopkpi.active_num - adkpi.active_num as active_num,\nopkpi.active_regi_dev_num - adkpi.active_regi_dev_num as active_regi_dev_num,\nopkpi.new_active_dev_num - adkpi.new_active_dev_num as new_active_dev_num,\nopkpi.regi_device_num - adkpi.regi_dev_num as regi_dev_num,\nopkpi.new_regi_device_num - adkpi.new_regi_dev_num as new_regi_dev_num,\nopkpi.pay_people_num - adkpi.pay_dev_num as pay_dev_num,\nopkpi.new_pay_people_num - adkpi.new_pay_dev_num as new_pay_dev_num,\nopkpi.new_pay_money - adkpi.new_pay_price as new_pay_price,\nbasekpi.pay_money - adkpi.pay_price as pay_price\nfrom \n(select publish_date,parent_game_id,child_game_id,os,group_id,sum(request_click_num) request_click_num,sum(request_click_uv) request_click_uv,sum(download_num) download_num,sum(download_uv) download_uv,sum(active_num) active_num,sum(active_regi_dev_num) active_regi_dev_num,sum(new_active_dev_num) new_active_dev_num,sum(regi_device_num) regi_device_num,sum(new_regi_device_num) new_regi_device_num,sum(pay_people_num) pay_people_num,sum(new_pay_people_num) new_pay_people_num,sum(new_pay_money) new_pay_money from bi_gamepublic_base_opera_kpi okpi where okpi.publish_date='currentday' and child_game_id in (game_id_set) group by publish_date,child_game_id,os,group_id ) opkpi\njoin \n(select publish_date,child_game_id,request_click_num,request_click_uv,download_num,download_uv,active_num,active_regi_dev_num,new_active_dev_num,regi_dev_num,new_regi_dev_num,pay_dev_num,new_pay_dev_num,pay_price,new_pay_price  from bi_new_merge_ad_kpi where publish_date='currentday' and child_game_id in (game_id_set) group by publish_date,child_game_id) adkpi on opkpi.publish_date=adkpi.publish_date and opkpi.child_game_id=adkpi.child_game_id\njoin \n(select publish_date,child_game_id,sum(pay_money) pay_money from bi_gamepublic_base_day_kpi where publish_date='currentday' and child_game_id in (game_id_set) group by publish_date,child_game_id ) basekpi on opkpi.publish_date=basekpi.publish_date and opkpi.child_game_id=basekpi.child_game_id"
      .replace("currentday", currentday).replace("game_id_set", game_id_set)
    val ad_kpi_update_sql = "insert into bi_new_merge_ad_kpi (publish_date,child_game_id,parent_game_id,os,group_id,request_click_num,request_click_uv,download_num,download_uv,active_num,active_regi_dev_num,new_active_dev_num,regi_dev_num,new_regi_dev_num,pay_dev_num,new_pay_dev_num,new_pay_price,pay_price) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)\non duplicate key update \nparent_game_id=values(parent_game_id),\nos=values(os),\ngroup_id=values(group_id),\nrequest_click_num=values(request_click_num),\nrequest_click_uv=values(request_click_uv),\ndownload_num=values(download_num),\ndownload_uv=values(download_uv),\nactive_num=values(active_num),\nactive_regi_dev_num=values(active_regi_dev_num),\nnew_active_dev_num=values(new_active_dev_num),\nregi_dev_num=values(regi_dev_num),\nnew_regi_dev_num=values(new_regi_dev_num),\npay_dev_num=values(pay_dev_num),\nnew_pay_dev_num=values(new_pay_dev_num),\nnew_pay_price=values(new_pay_price),\npay_price=values(pay_price)"
    val pstmt = conn.prepareStatement(ad_kpi_update_sql)

    val res = stmt.executeQuery(op_kpi_sql)
    while (res.next()) {
      pstmt.setObject(1, res.getString("publish_date"))
      pstmt.setObject(2, res.getString("child_game_id"))
      pstmt.setObject(3, res.getString("parent_game_id"))
      pstmt.setObject(4, res.getInt("os"))
      pstmt.setObject(5, res.getInt("group_id"))
      pstmt.setObject(6, res.getInt("request_click_num"))
      pstmt.setObject(7, res.getInt("request_click_uv"))
      pstmt.setObject(8, res.getInt("download_num"))
      pstmt.setObject(9, res.getInt("download_uv"))
      pstmt.setObject(10, res.getInt("active_num"))
      pstmt.setObject(11, res.getInt("active_regi_dev_num"))
      pstmt.setObject(12, res.getInt("new_active_dev_num"))
      pstmt.setObject(13, res.getInt("regi_dev_num"))
      pstmt.setObject(14, res.getInt("new_regi_dev_num"))
      pstmt.setObject(15, res.getInt("pay_dev_num"))
      pstmt.setObject(16, res.getInt("new_pay_dev_num"))
      pstmt.setObject(17, res.getInt("new_pay_price"))
      pstmt.setObject(18, res.getInt("pay_price"))
      pstmt.executeUpdate()
    }
    res.close()
    pstmt.close()
    stmt.close()
    conn.close()

  }

  def getThirddataGameId(conn: Connection, currentday: String): String = {
    val sql = "select group_concat(distinct child_game_id) game_id_set from bi_new_merge_ad_kpi where publish_date='" + currentday + "'"
    val stmt = conn.createStatement()
    val res = stmt.executeQuery(sql)
    var game_id_set = ""
    if (res.next()) {
      game_id_set = res.getString("game_id_set")
    }
    res.close()
    stmt.close()
    game_id_set
  }

}
