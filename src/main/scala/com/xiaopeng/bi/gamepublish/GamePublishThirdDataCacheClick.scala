package com.xiaopeng.bi.gamepublish

import com.xiaopeng.bi.utils.ConfigurationUtil
import com.xiaopeng.bi.utils.action.ThirdDataCacheActs
import kafka.serializer.StringDecoder
import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by bigdata on 17-12-11.
  * 广告监联调点击数据明细表
  */
object GamePublishThirdDataCacheClick {
  val checkpointdir = "file:///home/hduser/spark/spark-1.6.1/checkpointdir/thirddata_cache"
  val topic = "thirddata"
  var batch_time = 20

  def main(args: Array[String]): Unit = {
    //测试环境，不打印日志
    val environment = ConfigurationUtil.getEnvProperty("env.conf")
    if (!environment.contains("product")) {
      Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
      Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
    }

    if (args.length > 0) {
      batch_time = args(0).toInt
    }
    val ssc: StreamingContext = StreamingContext.getOrCreate(checkpointdir, startActions _)
    ssc.start()
    ssc.awaitTermination()
  }

  def startActions(): StreamingContext = {
    val Array(brokers, topics) = Array(ConfigurationUtil.getProperty("kafka.metadata.broker.list"), topic)
    val sparkConf = new SparkConf().setAppName(this.getClass.getName.replace("$", "")).setMaster("local[1]")
    val sparkContext = new SparkContext(sparkConf)
    val ssc: StreamingContext = new StreamingContext(sparkContext, Seconds(batch_time))
    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet)
    val valuesDstream: DStream[String] = messages.map(_._2)
    val thirdData = valuesDstream
      .filter(x => x.contains("bi_thirddata")).filter(x => (!x.contains("bi_adv_money"))) //排除钱大师
      .map(line => line.substring(line.indexOf("{"), line.length))


    //缓存数据到数据表
    ThirdDataCacheActs.cacheClickData(thirdData)

    ssc.checkpoint(checkpointdir)
    ssc
  }
}
