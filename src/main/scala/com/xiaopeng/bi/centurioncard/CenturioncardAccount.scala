package com.xiaopeng.bi.centurioncard

import com.xiaopeng.bi.utils._
import kafka.serializer.StringDecoder
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by kequan on 2/13/17.
  *
  * 保存 每个游戏（游戏id,游戏名称），每个账号的 账号类型，注册时间，最近登录时间，是否交易
  */
object CenturioncardAccount {
  var arg = "10"

  def main(args: Array[String]) {
//    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
//    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
    arg = args(0)
    val ssc = StreamingContext.getOrCreate(ConfigurationUtil.getProperty("spark.checkpoint.centurioncardaccount"), recently _)
    ssc.start()
    ssc.awaitTermination()
  }

  /**
    * @return StreamingContext
    */
  def recently(): StreamingContext = {
    //创建各种上下文
    val sparkConf = new SparkConf().setAppName("CenturioncardAccount");
    SparkUtils.setMaster(sparkConf);
    val sc = new SparkContext(sparkConf);
    val ssc = new StreamingContext(sc, Seconds(arg.toInt));
    //val hiveContext = new HiveContext(sc);
    val sqlContext = new SQLContext(sc);

    // 获取kafka的数据
    val Array(brokers, topics) = Array(ConfigurationUtil.getProperty("kafka.metadata.broker.list"), ConfigurationUtil.getProperty("kafka.topics.centurioncardaccount"));
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers);
    val topicsSet = topics.split(",").toSet;
    val valuesDstream: DStream[(String)] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet).map(x => {
      x._2
    }).cache();

    //注册日志  header 游戏帐号 游戏id 注册时间  user_type 绑定的返利人
    val regiLogs: DStream[(String, String, String, String, String, String)] = valuesDstream.filter(x => {
      x.split("\\|")(0).contains("bi_regi")
    }).filter(x => {
      val splitlog = x.split("\\|");
      val iscorrectformat = splitlog.length > 8 && (!splitlog(3).equals("")) && StringUtils.isTime(splitlog(5))
      if (!iscorrectformat) {
        FileUtil.apppendTofile("/home/hduser/projs/logs/centaccouterror.log", "formaterror=="+x)
      }
      iscorrectformat;
    }).map(x => {
      val splitlog = x.split("\\|");
      (splitlog(0), splitlog(3), splitlog(4), splitlog(5), splitlog(6), splitlog(8));
    })
    dealInfo(regiLogs);

    //登录日志   header  游戏帐号 游戏id 登录时间
    val loginLogs: DStream[(String, String, String, String, String, String)] = valuesDstream.filter(x => {
      x.split("\\|")(0).contains("bi_login")
    }).filter(x => {
      val splitlog = x.split("\\|");
      val iscorrectformat = splitlog.length > 8 && (!splitlog(3).equals("")) && (!splitlog(8).equals("") && StringUtils.isTime(splitlog(4)))
      if (!iscorrectformat) {
        FileUtil.apppendTofile("/home/hduser/projs/logs/centaccouterror.log", "formaterror=="+x)
      }
      iscorrectformat;
    }).map(x => {
      val splitlog = x.split("\\|");
      (splitlog(0), splitlog(3), splitlog(8), splitlog(4), "", "");
    });
    dealInfo(loginLogs);

    //支付日志    header  游戏帐号 游戏id 登录时间
    val orderLogs: DStream[(String, String, String, String, String, String)] = valuesDstream.filter(x => {
      x.split("\\|")(0).contains("bi_order")
    }).filter(x => {
      val splitlog = x.split("\\|");
      val iscorrectformat = splitlog.length > 7 && (!splitlog(5).equals("")) && (!splitlog(7).equals(""))
      if (splitlog.length <= 7) {
        FileUtil.apppendTofile("/home/hduser/projs/logs/centaccouterror.log", "formaterror=="+x)
      }
      iscorrectformat;
    }).map(x => {
      val splitlog = x.split("\\|");
      (splitlog(0), splitlog(5), splitlog(7), "", "", "");
    });
    dealInfo(orderLogs);

    ssc.checkpoint(ConfigurationUtil.getProperty("spark.checkpoint.centurioncardaccount"))
    ssc;
  }

  def dealInfo(valuesDstream: DStream[(String, String, String, String, String, String)]) = {
    valuesDstream.foreachRDD(rdd => {
      AccountUtil.deallogs(rdd);
    })

  }


}