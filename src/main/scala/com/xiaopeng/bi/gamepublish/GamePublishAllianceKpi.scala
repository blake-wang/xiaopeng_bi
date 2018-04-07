package com.xiaopeng.bi.gamepublish

import com.xiaopeng.bi.utils._
import kafka.serializer.StringDecoder
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by kequan on 2/26/18.
  * 发行 联运 业务逻辑
  */
object GamePublishAllianceKpi {
  var arg = "60"

  def main(args: Array[String]): Unit = {
    //    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    //    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
    if (args.length > 0) {
      arg = args(0)
    }
    val ssc = StreamingContext.getOrCreate(ConfigurationUtil.getProperty("spark.checkpoint.alliance.kpi"), getStreamingContext _);
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
    val Array(brokers, topics) = Array(ConfigurationUtil.getProperty("kafka.metadata.broker.list"), ConfigurationUtil.getProperty("kafka.topics.alliance.kpi"));
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers);
    val topicsSet = topics.split(",").toSet;
    val dslogs: DStream[String] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet).map(_._2)
    dslogs.foreachRDD(rdd => {

      val sc = rdd.sparkContext
      val hiveContext: HiveContext = HiveContextSingleton.getInstance(sc)

      // 联运管理维度信息表
      DimensionUtil.createAllianceDim(hiveContext)
      // 处理激活日志
      GamePublicAllianceActiveUtil.loadActiveInfo(rdd, hiveContext)
      // 处理注册日志
      GamePublicAllianceRegiUtil.loadRegiInfo(rdd, hiveContext)
      // 处理登录数据
      GamePublicAllianceLoginUtil.loadLoginInfo(rdd, hiveContext);
      // 处理支付数据
      GamePublicAlliancePayUtil.loadPayInfo(rdd, hiveContext);
      // 联运管理维度信息表 内存消除
      DimensionUtil.uncacheAllianceDimTable(hiveContext)
    })

    ssc.checkpoint(ConfigurationUtil.getProperty("spark.checkpoint.alliance.kpi"));
    ssc;
  }

}
