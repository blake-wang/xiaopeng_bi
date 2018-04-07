package com.xiaopeng.bi.centurioncard

import com.xiaopeng.bi.utils._
import com.xiaopeng.bi.utils.action.{CenturionCommonSdkActs, SubCenturionActs}
import kafka.serializer.StringDecoder
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Administrator on 2017/5/19.
  */
object CenturionCommonSdk {

  /*检查点目录*/
  val checkdir="file:///home/hduser/spark/spark-1.6.1/checkpointdir/CenturionCommonSdk"
  /*topic*/
  val topic: String ="order,login,regi"

  def main(args: Array[String]): Unit = {

    Hadoop.hd
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
    //val ssc: StreamingContext=statCenturionCommonSdk
    val ssc: StreamingContext = StreamingContext.getOrCreate(checkdir, statCenturionCommonSdk _)
    ssc.start()
    ssc.awaitTermination()
  }


  def statCenturionCommonSdk(): StreamingContext = {
    val Array(brokers, topics) = Array(ConfigurationUtil.getProperty("kafka.metadata.broker.list"),topic)
    val sparkConf = new SparkConf().setAppName(this.getClass.getName.replace("$", ""))
    sparkConf.set("spark.streaming.backpressure.enabled","true")               //开启后spark自动根据系统负载选择最优消费速率
    sparkConf.set("spark.streaming.backpressure.initialRate","200")            //
    sparkConf.set("spark.streaming.stopGracefullyOnShutdown","true")          //启动优雅关闭
    sparkConf.set("spark.streaming.unpersist","true")
    sparkConf.set("spark.cleaner.ttl","3600")
    sparkConf.set("spark.locality.wait","500")
    val sparkContext=new SparkContext(sparkConf)
    val ssc: StreamingContext = new StreamingContext(sparkContext, Seconds(40))
    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
    //从kafka中获取所有游戏日志数据

    val messages: InputDStream[(String, String)]= KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet)
    //获取人tuple的values
    val valuesDstream: DStream[String]= messages.map(_._2)
    valuesDstream.foreachRDD(rdd=>{
      if(rdd.count()>0)
        {
          loadRegiInfo(rdd);
          loadLoginInfo(rdd);
          loadOrderInfo(rdd);
        }

    })
    ssc.checkpoint(checkdir)
    ssc
  }

   /**加载注册数据action
   * 游戏帐号  游戏id  注册时间  渠道ID
   *
   **/
  def loadRegiInfo(rdd: RDD[String]) ={
    val regInfo: RDD[(String, String, String, String)] = rdd.filter(x=>x.contains("bi_common")).filter(x => {
       val splitlog = x.split("\\|",-1);
     splitlog(0).contains("bi_regi") && splitlog.length >=8 && (!splitlog(3).equals("")) &&(!splitlog(4).equals(""))&&(!splitlog(6).equals(""))
     }).map(x => {
       val splitlog = x.split("\\|",-1);
       (splitlog(3).toLowerCase.trim, splitlog(4), splitlog(5),splitlog(7));
     })
     //无注册数据不操作
     if(regInfo.count()>0)
       {
         regInfo.coalesce(20)
         regInfo.foreachPartition(fp=>{
         CenturionCommonSdkActs.regiInfoActions(fp)
         })
       }

  }

  /**
    *  订单信息：游戏账号（5），订单日期（6），游戏id（7），渠道id（8）,充值流水(10)+代金券(13),区服
    *
    * @param rdd
    */

  def loadOrderInfo(rdd: RDD[String]) ={
    val orderInfo =rdd
      .filter(_.split("\\|",-1)(0).contains("bi_order"))
      .filter(_.split("\\|",-1).length>=28)
      .filter(_.split("\\|",-1)(19).toInt==4)        //只取状态为4的数据
      .map(x=>{val splitlog=x.split("\\|",-1);
      (splitlog(5).trim.toLowerCase,splitlog(6),splitlog(7),splitlog(8),Commons.getNullTo0(splitlog(10))+Commons.getNullTo0(splitlog(13)),splitlog(27))})
      //无数据则不操作
      if(rdd.count()>0)
        {
          orderInfo.coalesce(2)
          orderInfo.foreachPartition(fp=>{
//            CenturionCommonSdkActs.orderInfoActions(fp)
          })
        }

  }


  /** 加载登录数据action
   * game_account,logintime,gameid
   **/
  def loadLoginInfo(rdd: RDD[String]) ={
      val loginInfo: RDD[(String, String, String)] =rdd.filter(x=>x.contains("bi_common")).filter(x=>x.contains("bi_login"))
        .filter(_.split("\\|",-1).length>=9)
        .map(x=>{
          val splitlog=x.split("\\|",-1);
          (splitlog(3).trim.toLowerCase,splitlog(4),splitlog(8))
        })
    loginInfo.coalesce(10)
    loginInfo.foreachPartition(fp=>{
      CenturionCommonSdkActs.loginInfoActions(fp)
      })
  }
}

