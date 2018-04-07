package com.xiaopeng.bi.gameinside

import com.xiaopeng.bi.bean.GameInsideRoleData
import com.xiaopeng.bi.utils.action.GameInsideActs
import com.xiaopeng.bi.utils._
import kafka.serializer.StringDecoder
import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/** * Created by denglh on 2016/12/28. */
object GameInsideRole {
  /*检查点目录*/
  val checkdir="file:///home/hduser/spark/spark-1.6.1/checkpointdir/gameinner"
  var batch: Int = 50
  val topic="gameinner,accountonline"
  def batchInt(bt:Int):Unit={batch=bt}

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
    Hadoop.hd
    batchInt(args(0).toInt)
    //val ssc: StreamingContext=statActions
    val ssc: StreamingContext = StreamingContext.getOrCreate(checkdir, statActions _)
    ssc.start()
    ssc.awaitTermination()
  }
  /**
    * 游戏内部数据
    * @return StreamingContext
    */
  def statActions(): StreamingContext = {

    val Array(brokers, topics) = Array(ConfigurationUtil.getProperty("kafka.metadata.broker.list"),topic)
    val sparkConf = new SparkConf().setAppName(this.getClass.getName.replace("$", ""))
    sparkConf.set("spark.streaming.backpressure.enabled","true")               //开启后spark自动根据系统负载选择最优消费速率
    sparkConf.set("spark.streaming.backpressure.initialRate","1000")
    sparkConf.set("spark.streaming.stopGracefullyOnShutdown","true")          //启动优雅关闭
    sparkConf.set("spark.streaming.kafka.maxRatePerPartition","1000")
    sparkConf.set("spark.streaming.unpersist","true")
    sparkConf.set("spark.locality.wait","1000")   //数据本地化模式转换等待时间
    val sparkContext=new SparkContext(sparkConf)
    val ssc: StreamingContext = new StreamingContext(sparkContext, Seconds(batch))

    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
    //从kafka中获取所有游戏日志数据
    val messages: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet)
    //获取人tuple的values
    val valuesDstream: DStream[String] = messages.map(_._2)
    //角色数据
    val roleInfo=valuesDstream
                   .filter(x=>x.contains("BI_ROLE"))
                   .map(x => {
                     val entry: GameInsideRoleData = AnalysisJsonUtil.AnalysisGameInsideData(x)
                     (entry.getContent.getRoleid, entry.getContent.getRolename,entry.getContent.getRoletype,entry.getTitle.getRolelevel,
                       entry.getContent.getOperatime,entry.getTitle.getServarea, entry.getTitle.getUserid,entry.getTitle.getServareaname)
                   })

    //gameaccount(3) gamekey(1) channelid(2) starttime(4) endtime(5)
    val accountOnlineInfo=valuesDstream
      .filter(x=>x.contains("bi_accountonline"))
      .filter(x => x.split("\\|",-1).length >= 6)        //排除截断日志
      .map(x => { val odInfo=x.split("\\|",-1)
      (odInfo(3).trim.toLowerCase, odInfo(1), odInfo(2),odInfo(4),odInfo(5))
    })
    //加载juese数据
    GameInsideActs.loadAccountOnlinenfo(accountOnlineInfo);
    GameInsideActs.loadRoleInfo(roleInfo);
    ssc.checkpoint(checkdir)
    ssc
  }






}
