package com.xiaopeng.bi.gamepublish


import com.xiaopeng.bi.utils._
import com.xiaopeng.bi.utils.action.ThirdDataActs
import kafka.serializer.StringDecoder
import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}


/** * Created by denglh on 2017/09/10 */
object GamePublishThirdData {
  /*检查点目录*/
  val checkdir = "file:///home/hduser/spark/spark-1.6.1/checkpointdir/thirddata_7_medium"
  var batch: Int = 6
  val topic = "thirddata,active,regi,order"

  def batchInt(bt: Int): Unit = {
    batch = bt
  }

  def main(args: Array[String]): Unit = {

    //测试环境，不打印日志
    val environment = ConfigurationUtil.getEnvProperty("env.conf")
    if (!environment.contains("product")) {
      Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
      Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
    }


    Hadoop.hd
    batchInt(args(0).toInt)
    //val ssc: StreamingContext=statActions
    val ssc: StreamingContext = StreamingContext.getOrCreate(checkdir, statActions _)
    ssc.start()
    ssc.awaitTermination()
  }

  /**
    * 游戏内部数据
    *
    * @return StreamingContext
    */
  def statActions(): StreamingContext = {

    val Array(brokers, topics) = Array(ConfigurationUtil.getProperty("kafka.metadata.broker.list"), topic)
    val sparkConf = new SparkConf().setAppName(this.getClass.getName.replace("$", ""))
    //<下面4个参数暂时取消，删除检查点生效>
    //    sparkConf.set("spark.streaming.backpressure.enabled", "true") //开启后spark自动根据系统负载
    //    sparkConf.set("spark.streaming.backpressure.initialRate", "1000")
    //    sparkConf.set("spark.streaming.stopGracefullyOnShutdown", "true") //启动优雅关闭
    //    sparkConf.set("spark.streaming.kafka.maxRatePerPartition", "1000")
    sparkConf.set("spark.streaming.unpersist", "true")
    sparkConf.set("spark.locality.wait", "500") //数据本地化模式转换等待时间
    sparkConf.set("spark.default.parallelism", "60")  //设置读取数据的并发数
    SparkUtils.setMaster(sparkConf)
    val sparkContext = new SparkContext(sparkConf)
    val ssc: StreamingContext = new StreamingContext(sparkContext, Seconds(batch))
    val topicsSet = topics.split(",").toSet
    //    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers, "zookeeper.session.timeout.ms" -> "12000", "socket.timeout.ms" -> "60000")
    //从kafka中获取所有游戏日志数据
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet)
    //获取人tuple的values
    val valuesDstream: DStream[String] = messages.map(_._2)
    //角色数据
    val thirdData = valuesDstream
      .filter(x => x.contains("bi_thirddata")).filter(x => (!x.contains("bi_adv_money"))) //排除钱大师
      .map(line => line.substring(line.indexOf("{"), line.length))
    val ownerData = valuesDstream
      .filter(x => (!x.contains("bi_thirddata")))
    //加载广告日志
    ThirdDataActs.adClick(thirdData);
    //加载自家日志
    ThirdDataActs.theOwnerData(ownerData)
    ssc.checkpoint(checkdir)
    ssc
  }
}
