package com.xiaopeng.bi.app

import com.xiaopeng.bi.utils.ConfigurationUtil
import kafka.serializer.StringDecoder
import org.apache.log4j.Logger
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{Logging, SparkConf, SparkContext}


/**
  * Created by JSJSB-0071 on 2016/12/13.
  */
object AppPointAndQuota extends Logging {

  val logger = Logger.getLogger(this.getClass)
  var arg = "60"

  val apppointqcheckpointdir="file:///home/hduser/spark/spark-1.6.1/checkpointdir/checkpointdirapppointq"

  val apppointtopic="order,points,appdownload,login,appinstall"

  def main(args: Array[String]) {
    if (arg.length > 0) {
      arg = args(0)
    }
    val ssc = StreamingContext.getOrCreate(apppointqcheckpointdir, getStreamingContext _);
    ssc.start();
    ssc.awaitTermination();

  }

  def getStreamingContext(): StreamingContext = {

      val sparkConf = new SparkConf().setAppName("AppPointAndQuota")
      .set("spark.sql.shuffle.partitions", "60")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.locality.wait", "1000")
    val sparkContext = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sparkContext, Seconds(arg.toInt))

    // Create direct kafka stream with brokers and topics
    val Array(brokers, topics) = Array(ConfigurationUtil.getProperty("kafka.metadata.broker.list"), apppointtopic)
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers);
    val topicsSet = topics.split(",").toSet

    val messages: DStream[String] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet).map(_._2)

    messages.foreachRDD(rdd => {
      if (!rdd.isEmpty()) {
        try {
          AppActions.appPointQuota(rdd)
          AppActions.appPhoneApps(rdd);
          AppActions.appDownloads(rdd);
          AppActions.orderLtdPrice(rdd);
          AppActions.gameRecently(rdd);
        } catch {
          case ex: Exception => {
            logger.error("patition error： " + ex)
          }
        }
      }
    })
    ssc.checkpoint(apppointqcheckpointdir);
    ssc
  }
}
