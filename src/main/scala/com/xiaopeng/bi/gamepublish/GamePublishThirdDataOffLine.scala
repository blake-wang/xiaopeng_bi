package com.xiaopeng.bi.gamepublish

import java.text.SimpleDateFormat
import java.util.Calendar

import com.xiaopeng.bi.utils._
import com.xiaopeng.bi.utils.action.ThirdDataOffLineActs
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat
import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


/**
  * Created by bigdata on 17-10-10.
  * 广告监测的  点击-激活-注册-订单  离线数据修复
  *
  */
object GamePublishThirdDataOffLine {

  var startday = ""
  //参数pd，用来控制click，active，regi，order的执行，默认只执行click，active，regi
  var params = ""
  val logger = Logger.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    if (args.length == 1) {
      startday = args(0)
    } else if (args.length == 2) {
      startday = args(0)
      params = args(1)
    }

    //<线上环境>
    val thirddata = "hdfs://hadoopmaster:9000/user/hive/warehouse/yyft.db/thirddata/"
    val active = "hdfs://hadoopmaster:9000/user/hive/warehouse/ad/active/*"
    val regi = "hdfs://hadoopmaster:9000/user/hive/warehouse/ad/regi/*"
    val order = "hdfs://hadoopmaster:9000/user/hive/warehouse/ad/order/*"

    val sparkConf = new SparkConf().setAppName(this.getClass.getName.replace("$", ""))
      .set("spark.sql.shuffle.partitions", ConfigurationUtil.getProperty("spark.sql.shuffle.partitions"))
    SparkUtils.setMaster(sparkConf)
    val sparkContext = new SparkContext(sparkConf)

    //<日期判断>
    //根据参数传入日期
    val startdayNoBash = startday.replace("-", "")
    //    //获取参数传入日期的第二天
    //    val secondday = getDayByParams(simpleDateFormat, currentday, 1)
    //    //获取程序执行当前日期
    //    val today = simpleDateFormat.format(now)
    //    //获取程序执行日期的前一天
    //    val yesterday = getDayByParams(simpleDateFormat, today, -1)
    //    println("startday : " + startday)

    //点击
    if (params.equals("all")) {
      doClickAll(thirddata, sparkContext, startdayNoBash)
    } else if (params.equals("part")) {
      doClickPart(thirddata, sparkContext, startdayNoBash)
    } else if (params.equals("active")) {
      doActive(active, sparkContext)
      doRegi(regi, sparkContext)
      doOrder(order, sparkContext)
      //更新group_id,head_people,medium_account,remark
      ThirdDataOffLineActs.updateMediumAccountInfo2(startday)
    } else if (params.equals("regi")) {
      doRegi(regi, sparkContext)
      doOrder(order, sparkContext)
      //更新group_id,head_people,medium_account,remark
      ThirdDataOffLineActs.updateMediumAccountInfo2(startday)
    } else if (params.equals("order")) {
      doOrder(order, sparkContext)
      //更新group_id,head_people,medium_account,remark
      ThirdDataOffLineActs.updateMediumAccountInfo2(startday)
    }

    sparkContext.stop()
  }

  /**
    * 处理当天指定的点击日志，需要在linux上将日志文件处理后放入offlineData文件夹
    * @param thirddata
    * @param sparkContext
    * @param currentday
    */
  private def doClickPart(thirddata: String, sparkContext: SparkContext, currentday: String) = {
    //取需要补充的某个游戏的数据
    val clickLogRDD = sparkContext.newAPIHadoopFile(thirddata + "offlineData/*" + currentday + "*", classOf[CombineTextInputFormat], classOf[LongWritable], classOf[Text]).map(line => line._2.toString)
    //1：点击
    //    整理广告点击日志的格式,将所有媒介的原始日志统一整理成 tuple14
    val formatThirddataRDD = ThirdDataOffLineActs.formatThirdData(clickLogRDD, startday)
    //    println("formatThirddataRDD : " + formatThirddataRDD.count())
    //将tuple14转换成Row17
    val clickDataRDD = ThirdDataOffLineActs.convertFormatThirdDataRDD(formatThirddataRDD, startday)
    //    println("clickDataRDD : " + clickDataRDD.count())
    ThirdDataOffLineActs.clickData(formatThirddataRDD, clickDataRDD, sparkContext, startday)
  }

  /**
    * 处理当天全部的点击日志
    * @param thirddata
    * @param sparkContext
    * @param currentday
    */
  private def doClickAll(thirddata: String, sparkContext: SparkContext, currentday: String) = {
    //取补数当天全部点击数据
    val clickLogRDD = sparkContext.newAPIHadoopFile(thirddata + "*" + currentday + "*", classOf[CombineTextInputFormat], classOf[LongWritable], classOf[Text]).map(line => line._2.toString)
    //1：点击
    //    整理广告点击日志的格式,将所有媒介的原始日志统一整理成 tuple14
    val formatThirddataRDD = ThirdDataOffLineActs.formatThirdData(clickLogRDD, startday)
    //    println("formatThirddataRDD : " + formatThirddataRDD.count())
    //将tuple14转换成Row17
    val clickDataRDD = ThirdDataOffLineActs.convertFormatThirdDataRDD(formatThirddataRDD, startday)
    //    println("clickDataRDD : " + clickDataRDD.count())
    ThirdDataOffLineActs.clickData(formatThirddataRDD, clickDataRDD, sparkContext, startday)
  }

  /**
    * 处理订单日志
    * @param order
    * @param sparkContext
    */
  private def doOrder(order: String, sparkContext: SparkContext) = {
    //订单
    val orderLogRDD = sparkContext.newAPIHadoopFile(order, classOf[CombineTextInputFormat], classOf[LongWritable], classOf[Text]).map(line => line._2.toString)
    orderLogRDD.coalesce(3)
    //先查询bi_ad_momo_click,看哪些游戏是做了广告的
    val game_id_set = getGameId

    //4：订单   注意：订单去重
    val dataOrder = orderLogRDD.filter(ats => ats.contains("bi_order")).filter(line => {
      val arr = line.split("\\|", -1)
      //排除截断日志   只取存在订单号的数据，不存在订单号的为代金券消费  只取直充  只取状态为4的数据
      arr.length >= 25 && arr(2).trim.length > 0 && arr(22).contains("6") && arr(19).toInt == 4 && arr(6).length >= 13 && arr(6).substring(0, 10).equals(startday) && game_id_set.contains(arr(7))
    }).map(line => {
      val odInfo = line.split("\\|", -1)
      //游戏账号（5），订单号（2），订单时间（6），游戏id（7）,充值流水（10），imei(24)
      (odInfo(5).trim.toLowerCase, odInfo(2), odInfo(6), odInfo(7), Commons.getNullTo0(odInfo(10)) + Commons.getNullTo0(odInfo(13)), odInfo(24))
    })
    //    println("dataOrder : " + dataOrder.count())
    ThirdDataOffLineActs.orderMatchRegi(dataOrder)
  }

  /**
    * 处理注册日志
    * @param regi
    * @param sparkContext
    * @return
    */
  private def doRegi(regi: String, sparkContext: SparkContext) = {
    //注册
    val regiLogRDD = sparkContext.newAPIHadoopFile(regi, classOf[CombineTextInputFormat], classOf[LongWritable], classOf[Text]).map(line => line._2.toString)
    regiLogRDD.coalesce(3)
    //先查询bi_ad_momo_click,看哪些游戏是做了广告的
    val game_id_set = getGameId
    //3：注册
    val dataRegi = regiLogRDD.filter(ats => {
      ats.contains("bi_regi")
    }).filter(line => {
      val fields = line.split("\\|", -1)
      fields.length >= 15 && StringUtils.isNumber(fields(4)) && fields(5).length >= 13 && fields(5).substring(0, 10).equals(startday) && game_id_set.contains(fields(4)) //只取补数当天的数据
    }).map(regi => {
      val arr = regi.split("\\|", -1)
      //game_account  game_id  expand_channel._3 reg_time  imei,os
      (arr(3), arr(4).toInt, StringUtils.getArrayChannel(arr(13))(2), arr(5), arr(14), arr(11).toLowerCase)
    })
    //    println("dataRegi : " + dataRegi.count())
    ThirdDataOffLineActs.regiMatchActive(dataRegi)
  }

  /**
    * 处理激活日志
    * @param active
    * @param sparkContext
    */
  private def doActive(active: String, sparkContext: SparkContext) = {
    //激活
    val activeLogRDD = sparkContext.newAPIHadoopFile(active, classOf[CombineTextInputFormat], classOf[LongWritable], classOf[Text]).map(line => line._2.toString)
    activeLogRDD.coalesce(3)
    //先查询bi_ad_momo_click,看哪些游戏是做了广告的
    val game_id_set = getGameId
    //2：激活
    val dataActive = activeLogRDD.filter(line => {
      val fields = line.split("\\|", -1)
      fields(0).contains("bi_active") && fields.length >= 12 && fields(5).length >= 13 && fields(5).substring(0, 10).equals(startday) && game_id_set.contains(fields(1))
    }).map(line => {
      val splitd = line.split("\\|", -1)
      //gameid channelid expand_channel,imei,date,os,ip,deviceType,systemVersion
      //bi: bi_active|5202|21|djqy_hj_1|djqy_hj_1|2017-11-08 11:32:02|117.22.103.195|IOS|0CD6416392D14CD9B21109DCCCFC3DE9||11.0.3|iPhone 6s
      (splitd(1), splitd(2), splitd(4), splitd(8), splitd(5), splitd(7), splitd(6), splitd(11), splitd(10))
    })
    //println("dataActive : " + dataActive.count())
    ThirdDataOffLineActs.activeMatchClick(dataActive)
  }

  private def ExecuteClick(sparkContext: SparkContext, clickLogRDD: RDD[String]) = {
    //1：点击
    //    整理广告点击日志的格式,将所有媒介的原始日志统一整理成 tuple14
    val formatThirddataRDD = ThirdDataOffLineActs.formatThirdData(clickLogRDD, startday)
    //    println("formatThirddataRDD : " + formatThirddataRDD.count())
    //将tuple14转换成Row17
    val clickDataRDD = ThirdDataOffLineActs.convertFormatThirdDataRDD(formatThirddataRDD, startday)
    //    println("clickDataRDD : " + clickDataRDD.count())
    ThirdDataOffLineActs.clickData(formatThirddataRDD, clickDataRDD, sparkContext, startday)
  }

  private def ExecuteActive(activeLogRDD: RDD[String]) = {
    //先查询bi_ad_momo_click,看哪些游戏是做了广告的
    val game_id_set = getGameId

    //2：激活
    val dataActive = activeLogRDD.filter(line => {
      val fields = line.split("\\|", -1)
      fields(0).contains("bi_active") && fields.length >= 12 && fields(5).length >= 13 && fields(5).substring(0, 10).equals(startday) && game_id_set.contains(fields(1))
    }).map(line => {
      val splitd = line.split("\\|", -1)
      //gameid channelid expand_channel,imei,date,os,ip,deviceType,systemVersion
      //bi: bi_active|5202|21|djqy_hj_1|djqy_hj_1|2017-11-08 11:32:02|117.22.103.195|IOS|0CD6416392D14CD9B21109DCCCFC3DE9||11.0.3|iPhone 6s
      (splitd(1), splitd(2), splitd(4), splitd(8), splitd(5), splitd(7), splitd(6), splitd(11), splitd(10))
    })
    //println("dataActive : " + dataActive.count())
    ThirdDataOffLineActs.activeMatchClick(dataActive)
  }

  private def ExecuteRegi(regiLogRDD: RDD[String]) = {
    //先查询bi_ad_momo_click,看哪些游戏是做了广告的
    val game_id_set = getGameId
    //3：注册
    val dataRegi = regiLogRDD.filter(ats => {
      ats.contains("bi_regi")
    }).filter(line => {
      val fields = line.split("\\|", -1)
      fields.length >= 15 && StringUtils.isNumber(fields(4)) && fields(5).length >= 13 && fields(5).substring(0, 10).equals(startday) && game_id_set.contains(fields(4)) //只取补数当天的数据
    }).map(regi => {
      val arr = regi.split("\\|", -1)
      //game_account  game_id  expand_channel._3 reg_time  imei,os
      (arr(3), arr(4).toInt, StringUtils.getArrayChannel(arr(13))(2), arr(5), arr(14), arr(11).toLowerCase)
    })
    //    println("dataRegi : " + dataRegi.count())
    ThirdDataOffLineActs.regiMatchActive(dataRegi)
  }

  private def ExecuteOrder(orderLogRDD: RDD[String]) = {
    //先查询bi_ad_momo_click,看哪些游戏是做了广告的
    val game_id_set = getGameId

    //4：订单   注意：订单去重
    val dataOrder = orderLogRDD.filter(ats => ats.contains("bi_order")).filter(line => {
      val arr = line.split("\\|", -1)
      //排除截断日志   只取存在订单号的数据，不存在订单号的为代金券消费  只取直充  只取状态为4的数据
      arr.length >= 25 && arr(2).trim.length > 0 && arr(22).contains("6") && arr(19).toInt == 4 && arr(6).length >= 13 && arr(6).substring(0, 10).equals(startday) && game_id_set.contains(arr(7))
    }).map(line => {
      val odInfo = line.split("\\|", -1)
      //游戏账号（5），订单号（2），订单时间（6），游戏id（7）,充值流水（10），imei(24)
      (odInfo(5).trim.toLowerCase, odInfo(2), odInfo(6), odInfo(7), Commons.getNullTo0(odInfo(10)) + Commons.getNullTo0(odInfo(13)), odInfo(24))
    })
    //    println("dataOrder : " + dataOrder.count())
    ThirdDataOffLineActs.orderMatchRegi(dataOrder)
  }


  /**
    * 根据传入的日期以及时间差，获取其他的日期
    *
    * @param simpleDateFormat
    * @param currentday
    * @param params
    * @return
    */
  def getDayByParams(simpleDateFormat: SimpleDateFormat, currentday: String, params: Int): String = {
    val cal = Calendar.getInstance()
    val currentDate = simpleDateFormat.parse(currentday)
    cal.setTime(currentDate)
    cal.add(Calendar.DATE, params)
    val otherDay = simpleDateFormat.format(cal.getTime)
    return otherDay
  }


  /**
    * 查询点击明细表，获取做广告的 game_id
    *
    * @return
    */
  def getGameId(): String = {
    val conn = JdbcUtil.getConn()
    val stmt = conn.createStatement();
    val sql = "select group_concat(distinct game_id) game_id_set from bi_ad_momo_click"
    val resultSet = stmt.executeQuery(sql)
    var game_id_set = ""
    if (resultSet.next) {
      game_id_set = resultSet.getString("game_id_set")
    }
    resultSet.close()
    stmt.close()
    conn.close()

    return game_id_set
  }
}
