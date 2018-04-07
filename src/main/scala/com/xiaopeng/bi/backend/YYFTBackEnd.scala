package com.xiaopeng.bi.backend

import com.xiaopeng.bi.app.AppActions
import com.xiaopeng.bi.bean.GameInsideRoleData
import com.xiaopeng.bi.utils.action.GameInsideActs
import com.xiaopeng.bi.utils._
import com.xiaopeng.bi.utils.action.CenturionCommonSdkActs
import kafka.serializer.StringDecoder
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by bigdata on 17-8-7.
  * 《为了减少应用数量，故对一下不重要并且可以延时的数据应用合在一起》
  * 管理后台数据+聚合SDK数据+游戏内角色数据+游戏登录时长数据+移动APP
  */
object YYFTBackEnd {
  var arg = "600"

  def main(args: Array[String]): Unit = {
//    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
//    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    if (args.length > 0) {
      arg = args(0)
    }

    val ssc = StreamingContext.getOrCreate(ConfigurationUtil.getProperty("spark.checkpoint.backend"), getStreamingContext _)
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
    val topics = ConfigurationUtil.getProperty("kafka.topics.backend")
    val topicSet = topics.split(",").toSet
    val dsValues: DStream[String] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](streamingContext, kafkaparams, topicSet).map(_._2)
    //获取人tuple的values
    /**游戏内角色数据*/
    YYFTBackEnd.loadRoleInfo(dsValues);

    /**登录时长数据*/
    YYFTBackEnd.loadAccountOnlinenfo(dsValues);

   /**其他指标过程*/
    dsValues.foreachRDD(rdd => {
      if(rdd.count()>0) {
        val sc = rdd.sparkContext
        val hiveContext = HiveContextSingleton.getInstance(sc)

        /** ***********管理后台会员 ***********/
        //处理member日志,更新会员明细表基础信息
        GameMemberUtil.loadMemberInfo(rdd, hiveContext)
        //处理regi日志，从redis中获取game_account,对应的bind_member_id 的值 ,更新会员对应的accounts
        GameMemberWithRegiUtil2.loadRegiInfo(rdd, hiveContext)
        //处理binduid日志，更新会员对应的帐号数accounts
        GameBinduidUtil2.loadBinduidInfo(rdd, hiveContext)
        //处理order日志，更新会员对应的订单数
        GameMemberWithOrderUtil.loadOrderInfo(rdd, hiveContext)

        /** 管理后台，聚合SDK */
        YYFTBackEnd.loadRegiInfo(rdd);
        YYFTBackEnd.loadLoginInfo(rdd);
        YYFTBackEnd.loadOrderInfo(rdd);

        /**移动端 app2.0*/
        AppActions.appPointQuota(rdd)
        AppActions.appPhoneApps(rdd);
        AppActions.appDownloads(rdd);
        AppActions.orderLtdPrice(rdd);
        AppActions.gameRecently(rdd);

      }
    })
    streamingContext.checkpoint(ConfigurationUtil.getProperty("spark.checkpoint.backend"))
    return streamingContext
  }


  /**
    * 处理角色数据
    *
    * @param dsValues
    */
  def loadRoleInfo(dsValues: DStream[String]) ={
    val roleInfo=dsValues
      .filter(x=>x.contains("BI_ROLE"))
      .map(x => {
        val entry: GameInsideRoleData = AnalysisJsonUtil.AnalysisGameInsideData(x)
        (entry.getContent.getRoleid, entry.getContent.getRolename,entry.getContent.getRoletype,entry.getTitle.getRolelevel,
          entry.getContent.getOperatime,entry.getTitle.getServarea, entry.getTitle.getUserid,entry.getTitle.getServareaname)
      })
    GameInsideActs.loadRoleInfo(roleInfo);
  }

  /**
    * 登录时长
    * @param dsValues
    */
  def loadAccountOnlinenfo(dsValues: DStream[String]) = {
    //gameaccount(3) gamekey(1) channelid(2) starttime(4) endtime(5)
    val accountOnlineInfo=dsValues
      .filter(x=>x.contains("bi_accountonline"))
      .filter(x => x.split("\\|",-1).length >= 6)        //排除截断日志
      .map(x => { val odInfo=x.split("\\|",-1)
      (odInfo(3).trim.toLowerCase, odInfo(1), odInfo(2),odInfo(4),odInfo(5))
    })
    //加载juese数据
    GameInsideActs.loadAccountOnlinenfo(accountOnlineInfo);

  }


  /**加载注册数据action
    * 游戏帐号  游戏id  注册时间  渠道ID
    **/
  def loadRegiInfo(rdd: RDD[String]) ={
    val regInfo: RDD[(String, String, String, String)] = rdd.filter(x=>x.contains("bi_common")).filter(x => {
      val splitlog = x.split("\\|",-1);
      splitlog(0).contains("bi_regi") && splitlog.length >=16 && (!splitlog(3).equals("")) &&(!splitlog(4).equals(""))&&(!splitlog(6).equals(""))
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
    *  订单信息：游戏账号（5），订单日期（6），游戏id（7），渠道id（8）,充值流水(10)+代金券(13)，区服
    *
    * @param rdd
    */

  def loadOrderInfo(rdd: RDD[String]) ={
    val orderInfo =rdd
      .filter(_.split("\\|",-1)(0).contains("bi_order"))
      .filter(_.split("\\|",-1).length>=28)
      .filter(_.split("\\|",-1)(19).toInt==4)        //只取状态为4或者8的数据
      .map(x=>{val splitlog=x.split("\\|",-1);
      (splitlog(5).trim.toLowerCase,splitlog(6),splitlog(7),splitlog(8),Commons.getNullTo0(splitlog(10))+Commons.getNullTo0(splitlog(13)),splitlog(27))})
    //无数据则不操作
    if(rdd.count()>0)
    {
      orderInfo.coalesce(2)
      orderInfo.foreachPartition(fp=>{
        CenturionCommonSdkActs.orderInfoActions(fp)
      })
    }

  }


  /** 加载登录数据action
    * game_account,logintime,gameid
    **/
  def loadLoginInfo(rdd: RDD[String]) ={
    val loginInfo=rdd.filter(x=>x.contains("bi_common")).filter(x=>x.contains("bi_login"))
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
