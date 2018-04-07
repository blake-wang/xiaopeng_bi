package com.xiaopeng.bi.backend

import com.xiaopeng.bi.utils._
import kafka.serializer.StringDecoder
import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by bigdata on 17-8-7.
  * 会员id，手机号，注册时间，来源，最后消费时间，帐号数，订单数,充值金额，实付金额
  */
object GameMemberDetail {
  var arg = "60"

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    if (args.length > 0) {
      arg = args(0)
    }

    val ssc = StreamingContext.getOrCreate(ConfigurationUtil.getProperty("spark.checkpoint.member"), getStreamingContext _)
    ssc.start()
    ssc.awaitTermination()
  }


  def getStreamingContext(): StreamingContext = {

    val sparkConf = new SparkConf().setAppName(this.getClass.getName.replace("$", ""))
      .set("spark.default.parallelism", "60")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.shuffle.consolidateFiles", "true")
      .set("spark.sql.shuffle.partitions", "60")
    SparkUtils.setMaster(sparkConf)
    val sparkContext = new SparkContext(sparkConf)
    val streamingContext = new StreamingContext(sparkContext, Seconds(arg.toInt))
    val brokers = ConfigurationUtil.getProperty("kafka.metadata.broker.list")
    val kafkaparams = Map[String, String]("metadata.broker.list" -> brokers)
    val topics = ConfigurationUtil.getProperty("kafka.topics.member")
    val topicSet = topics.split(",").toSet
    val dsMember: DStream[String] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](streamingContext, kafkaparams, topicSet).map(_._2)

    dsMember.foreachRDD(rdd => {
      val sc = rdd.sparkContext
      val hiveContext = HiveContextSingleton.getInstance(sc)

      //处理member日志,更新会员明细表基础信息
      GameMemberUtil.loadMemberInfo(rdd, hiveContext)

      //处理regi日志，从redis中获取game_account,对应的bind_member_id 的值 ,更新会员对应的accounts
      GameMemberWithRegiUtil2.loadRegiInfo(rdd, hiveContext)

      //处理binduid日志，更新会员对应的帐号数accounts
      GameBinduidUtil2.loadBinduidInfo(rdd, hiveContext)

      //处理order日志，更新会员对应的订单数
      GameMemberWithOrderUtil.loadOrderInfo(rdd, hiveContext)
    })
    streamingContext.checkpoint(ConfigurationUtil.getProperty("spark.checkpoint.member"))
    return streamingContext
  }
}
