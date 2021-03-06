package com.xiaopeng.bi.centurioncard

import com.xiaopeng.bi.utils._
import com.xiaopeng.bi.utils.action.SubCenturionActs
import kafka.serializer.StringDecoder
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext, rdd}

/**
  * Created by Administrator on 2017/5/19.
  */
object SubCenturionCard {

  /*检查点目录*/
  val checkdir="file:///home/hduser/spark/spark-1.6.1/checkpointdir/subcenturion"
  /*topic*/
  val topic: String ="order,login,active,regi"

  def main(args: Array[String]): Unit = {

    Hadoop.hd
//    val ssc: StreamingContext=statSubCenturion
    val ssc: StreamingContext = StreamingContext.getOrCreate(checkdir, statSubCenturion _)
    ssc.start()
    ssc.awaitTermination()
  }

  def statSubCenturion(): StreamingContext = {
    val Array(brokers, topics) = Array(ConfigurationUtil.getProperty("kafka.metadata.broker.list"),topic)
    val sparkConf = new SparkConf().setAppName(this.getClass.getName.replace("$", ""))
    sparkConf.set("spark.streaming.backpressure.enabled","true")               //开启后spark自动根据系统负载选择最优消费速率
    sparkConf.set("spark.streaming.backpressure.initialRate","200")            //
    sparkConf.set("spark.streaming.stopGracefullyOnShutdown","true")          //启动优雅关闭
    sparkConf.set("spark.streaming.unpersist","true")
    val sparkContext=new SparkContext(sparkConf)
    val ssc: StreamingContext = new StreamingContext(sparkContext, Seconds(5))
    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
    //从kafka中获取所有游戏日志数据

    val messages: InputDStream[(String, String)]= KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet)
    //获取人tuple的values
    val valuesDstream: DStream[String]= messages.map(_._2)
    valuesDstream.foreachRDD(rdd=>{
      if(rdd.count()>0)
        {
          //黑专服数据统计
          try {loadActiveInfo(rdd); }catch {case e: Exception => e.printStackTrace}
          try {loadRegiInfo(rdd); }catch {case e: Exception => e.printStackTrace}
          Thread.sleep(1000)         //两秒钟，等注册数据加载进redis
          try {loadLoginInfo(rdd); }catch {case e: Exception => e.printStackTrace}
          try {loadOrderInfo(rdd); }catch {case e: Exception => e.printStackTrace}
        }

    })
    ssc.checkpoint(checkdir)
    ssc
  }

   /**加载注册数据action
   * 游戏帐号  游戏id  注册时间  imei1 ip  promo_code user_code expand_code expand_channel
   *
   **/
  def loadRegiInfo(rdd: RDD[String]) ={
    val regInfo: RDD[(String, String, String, String, String, String, String, String, String)] = rdd.filter(x => {
       val splitlog = x.split("\\|",-1);
      //
       splitlog(0).contains("bi_regi") && splitlog.length >=16 && (!splitlog(3).equals("")) && StringUtils.isTime(splitlog(5))&&(!splitlog(4).equals(""))&&(!splitlog(6).equals(""))
     }).map(x => {
       val splitlog = x.split("\\|",-1);
       (splitlog(3).toLowerCase.trim, splitlog(4), splitlog(5),Commons.getImei(splitlog(14)),splitlog(15),splitlog(12).split("~")(0),
         Commons.getUserCode(splitlog(12)),splitlog(12),splitlog(13)
         );
     })
     //无注册数据不操作
     if(regInfo.count()>0)
       {
         regInfo.coalesce(1)
         regInfo.foreachPartition(fp=>{
           SubCenturionActs.regiInfoActions(fp)
         })
       }

  }

  /**
    *  订单信息：游戏账号（5），订单号（2），订单日期（6），游戏id（7），渠道id（8），返利人id（18），产品类型（22），订单状态（19）,充值流水(10)+代金券(13)
    ，返利金额（16),imei(24),下单时身份（23）,违规状态（26）,区服27，角色28
    * @param rdd
    */

  def loadOrderInfo(rdd: RDD[String]) ={
    val orderInfo =rdd
      .filter(_.split("\\|",-1)(0).contains("bi_order"))
      .filter(_.split("\\|",-1).length>=29)
      .filter(!_.split("\\|",-1)(19).contains("0"))    //不为0，0为下单日志
      .filter(_.split("\\|",-1)(19).toInt==4)        //只取状态为4或者8的数据
      .filter(_.split("\\|",-1)(22).contains("6"))     //只取直充
      .map(x=>{val splitlog=x.split("\\|",-1);
      (splitlog(5).trim.toLowerCase,if(splitlog(2).equals("")) splitlog(21) else splitlog(2),splitlog(6),splitlog(7),splitlog(8),splitlog(18),splitlog(22).toInt,
        // modify old:Commons.getNullTo0(splitlog(10))+Commons.getNullTo0(splitlog(13))
        splitlog(19).toInt,Commons.getNullTo0(splitlog(10)),Commons.getNullTo0(splitlog(16)),Commons.getImei(splitlog(24)),splitlog(23),splitlog(26),splitlog(27),splitlog(28))})
      //无数据则不操作
      if(rdd.count()>0)
        {
          orderInfo.coalesce(1)
          orderInfo.foreachPartition(fp=>{
            SubCenturionActs.orderInfoActions(fp)
          })
        }

  }

  /**加载激活数据action
   *激活设备: gameid1 推广码 下黑码 激活时间5 imei8 IP地址6
   **/
  def loadActiveInfo(rdd: RDD[String]) ={
    val activeInfo= rdd.filter(x => {
      val splitlog = x.split("\\|",-1);
      splitlog(0).contains("bi_active") && splitlog.length >=9 && (splitlog(3).contains("~"))
    }).map(x => {
      val splitlog = x.split("\\|",-1);
      (splitlog(1),Commons.getPromoCode(splitlog(3)),Commons.getUserCode(splitlog(3)),splitlog(5),Commons.getImei(splitlog(8)),splitlog(6),splitlog(3));
    })
    activeInfo.coalesce(2)
    activeInfo.foreachPartition(fp=>{
       SubCenturionActs.activeInfoActions(fp)
      })
  }

  /** 加载登录数据action
   * game_account,logintime,imei,expand_code,gameid,expand_channel
   **/
  def loadLoginInfo(rdd: RDD[String]) ={
      val loginInfo=rdd.filter(x=>x.split("\\|",-1)(0).contains("bi_login"))
        .filter(_.split("\\|",-1).length>=9)
        .map(x=>{
          val splitlog=x.split("\\|",-1);
          (splitlog(3).trim.toLowerCase,splitlog(4),Commons.getImei(splitlog(7)),splitlog(5),splitlog(8),splitlog(6))
        })
    loginInfo.coalesce(2)
    loginInfo.foreachPartition(fp=>{
        SubCenturionActs.loginInfoActions(fp)
      })
  }
}

