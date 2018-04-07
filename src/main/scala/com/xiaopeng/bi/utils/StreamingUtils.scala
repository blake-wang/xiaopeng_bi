package com.xiaopeng.bi.utils

import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext}
import redis.clients.jedis.{Jedis, JedisPool}

/**
  * Created by 苏孟虎 on 2016/8/24.
  * Spark Streaming数据转换等工具对象类
  */
object StreamingUtils {


  /**
    * 游戏消费数据临时表-自有，平台：当日充值账号数，充值金额，充值笔数
    *
    * @param sqlContext
    */
  def gameConsumer(sqlContext: SQLContext): Unit = {
    //投放渠道：展示数，下载数,广告页点击数
    val showNum = sqlContext.sql("select publish_time,game_id,expand_channel,count(1) show_num,0 download_num,0 active_num, " +
      "0 pay_account_num,0 pay_money,0 regi_account_num,0 request_num,0 adpage_click_num " +
      "from channel where type = 1 group by game_id,expand_channel ,publish_time")
    val downloadNum = sqlContext.sql("select publish_time, game_id,expand_channel,0 show_num,count(1) download_num,0 active_num," +
      "0 pay_account_num,0 pay_money,0 regi_account_num,0 request_num,0 adpage_click_num " +
      " from channel where type = 2 group by game_id,expand_channel,publish_time")
    val adpageClickNum = sqlContext.sql("select publish_time,game_id,expand_channel,0 show_num,0 download_num,0 active_num, " +
      "0 pay_account_num,0 pay_money,0 regi_account_num,0 request_num,count(1) adpage_click_num " +
      "from channel where type = 3 group by game_id,expand_channel ,publish_time")

    //激活
    val activeNum = sqlContext.sql("select publish_time, game_id,expand_channel,0 show_num,0 download_num,count(1) active_num," +
      "0 pay_account_num,0 pay_money,0 regi_account_num,0 request_num,0 adpage_click_num " +
      " from active  group by game_id,expand_channel,publish_time")

    //请求数
    val requestNum = sqlContext.sql("select publish_time,game_id,expand_channel,0 show_num,0 download_num,0 active_num," +
      "0 pay_account_num,0 pay_money, " +
      "0 regi_account_num,count(1) request_num,0 adpage_click_num " +
      "from request group by game_id,expand_channel,publish_time")

    //游戏活跃：新增注册账号数regi_account_num
    val regiAccountNum = sqlContext.sql("select r.publish_time, r.game_id,r.expand_channel,0 show_num,0 download_num,0 active_num," +
      "0 pay_account_num,0 pay_money, " +
      "count(1) regi_account_num,0 request_num,0 adpage_click_num " +
      "from regi r" +
      " group by r.game_id,r.expand_channel,r.publish_time")

    //所有账号：
    val lastRegi = sqlContext.sql("select * from full_regi union select * from regi")
    lastRegi.registerTempTable("lastRegi")
    //lastRegi.show(100)
    //概况：充值金额,充值账号数pay_money pay_account_num
    val payMoney = sqlContext.sql("select o.publish_time,lr.game_id,lr.expand_channel ,0 show_num,0 download_num,0 active_num," +
      "0 pay_account_num,sum(if(o.order_state = 4,o.paymoney,-o.paymoney)) pay_money, " +
      "0 regi_account_num,0 request_num,0 adpage_click_num " +
      "from ods_order o " +
      "join lastRegi lr on o.game_account=lr.game_account and o.order_state in(4,8) group by lr.game_id,lr.expand_channel,o.publish_time")

    //所有订单
    sqlContext.sql("select * from full_order union select * from ods_order").registerTempTable("lastOrder")
    val payAccountNum = sqlContext.sql("select o.publish_time,lr.game_id,lr.expand_channel ,0 show_num,0 download_num,0 active_num," +
      "count(distinct o.game_account) pay_account_num, " +
      "0 pay_money, " +
      "0 regi_account_num,0 request_num,0 adpage_click_num " +
      "from lastOrder o " +
      "join lastRegi lr on o.game_account=lr.game_account and o.order_state=4 group by lr.game_id,lr.expand_channel,o.publish_time")


    requestNum.unionAll(showNum).unionAll(downloadNum).unionAll(adpageClickNum).unionAll(activeNum)
      .unionAll(regiAccountNum).unionAll(payMoney).unionAll(payAccountNum)
      .coalesce(Integer.parseInt(ConfigurationUtil.getProperty("coalesce.partitioin.num"))).registerTempTable("gameConsumer")
  }

  /**
    * 把 全量+实时日志 深度联运转化-临时表
    *
    * @param rdd
    * @param hiveContext
    */
  def convertPubGameLogsToDfTmpTable(rdd: RDD[String], hiveContext: HiveContext): Unit = {
    //深度联运日志 ，Struct，DF,temp_table
    val pubGameRdd = rdd.filter(line => line.contains("bi_pubgame")).map(line => {
      try {
        val splited = line.split("\\|", -1)
        Row(Integer.parseInt(splited(1)))
      } catch {
        case ex: Exception => {
          Row(-1)
        }
      }
    })

    //      hiveContext.sql("use yyft");
    //      val lastPubGame = hiveContext.sql("select distinct game_id from ods_publish_game").cache()
    //      lastPubGame.registerTempTable("lastPubGame")

    val pubGameStruct = (new StructType).add("game_id", IntegerType)
    val pubGameDF = hiveContext.createDataFrame(pubGameRdd, pubGameStruct);
    pubGameDF.registerTempTable("pubgame")
    hiveContext.sql("use yyft");
    val lastPubGame = hiveContext.sql("select distinct t.* from (select * from pubgame union select distinct game_id from ods_publish_game) t").cache()
    lastPubGame.registerTempTable("lastPubGame")
  }

  /**
    * 把各种日志数据转换为临时表
    *
    * @param rdd
    * @param sqlContext
    */
  def convertLogsToDfTmpTable(rdd: RDD[String], sqlContext: SQLContext): Unit = {
    //渠道投放日志，Struct，DF,temp_table
    val channelRdd = rdd.filter(line => line.contains("bi_channel")).map(line => {
      try {
        val splited = line.split("\\|", -1)
        Row(Integer.parseInt(splited(1)), StringUtils.defaultEmptyTo21(splited(2)), Integer.parseInt(splited(5)), splited(4).split(":")(0))
      } catch {
        case ex: Exception => {
          Row(0, "pyw", 0, "0000-00-00 00")
        }
      }
    })
    val channelStruct = (new StructType).add("type", IntegerType)
      .add("expand_channel", StringType).add("game_id", IntegerType).add("publish_time", StringType)
    val channelDF = sqlContext.createDataFrame(channelRdd, channelStruct);
    channelDF.registerTempTable("tmp_channel")
    sqlContext.sql("select a.* from tmp_channel a join lastPubGame b on a.game_id=b.game_id").registerTempTable("channel")

    //激活日志，Struct，DF,temp_table
    val activeRdd = rdd.filter(line => line.contains("bi_active")).map(line => {
      try {
        val splited = line.split("\\|", -1)
        Row(Integer.parseInt(splited(1)), StringUtils.defaultEmptyTo21(splited(4)), splited(5).split(":")(0))
      } catch {
        case ex: Exception => {
          Row(0, "pyw", "")
        }
      }
    })
    val activeStruct = (new StructType).add("game_id", IntegerType).add("expand_channel", StringType).add("publish_time", StringType)
    val activeDF = sqlContext.createDataFrame(activeRdd, activeStruct);
    activeDF.registerTempTable("tmp_active")
    sqlContext.sql("select a.* from tmp_active a join lastPubGame b on a.game_id=b.game_id").registerTempTable("active")

    //订单日志，Struct，DF,temp_table
    val orderRdd = rdd.filter(line => line.contains("bi_order")).map(line => {
      try {
        val splited = line.split("\\|", -1)
        Row(splited(5), java.lang.Double.parseDouble(splited(10)), java.lang.Integer.parseInt(splited(19)), splited(6).split(":")(0))
      } catch {
        case ex: Exception => {
          Row("pyww", 0, 0, "00-00-00 00")
        }
      }
    })
    val orderStruct = (new StructType).add("game_account", StringType).add("paymoney", DoubleType).add("order_state", IntegerType).add("publish_time", StringType)
    val orderDF = sqlContext.createDataFrame(orderRdd, orderStruct);
    orderDF.registerTempTable("ods_order")

    //请求数日志，Struct，DF,temp_table
    //192.168.20.97 - - [01/Sep/2016:16:46:39 +0800] "GET /h5/sdxl5/index.html?p=gdt_lm_00020&g=132 HTTP/1.1" 200 1681 "-""Mozilla/5.0 (Windows NT 6.1; WOW64; rv:48.0) Gecko/20100101 Firefox/48.0" - tg.pyw.cn test4 - 0.000
    //192.168.20.146 - - [12/Oct/2016:10:17:54 +0800] "GET /Ssxy/loadComplete?p=bd_bd2_zy2&g=1296 HTTP/1.1" 200 43 "http://tg.pyw.cn/h5/bdfxgn/index.html?p=bd_bd2_zy2&g=129 6""Mozilla/5.0 (Windows NT 6.1; WOW64; rv:49.0) Gecko/20100101 Firefox/49.0" - tg.pyw.cn otter 0.011 0.011
    //不包含 404 500
    //.*GET /h5/.*/index[0-9]*.html[?]p=.*---->.*GET /Ssxy/loadComplete[?]p=.*
    val requestRdd = rdd.filter(line => !line.contains("HTTP/1.0\" 404")).filter(line => StringUtils.isRequestLog(line, ".*GET /Ssxy/loadComplete[?]p=.*")).map(line => {
      try {
        val sp = line.split(" ")(6).split("p=", -1)(1).split("\"\"", -1)(0)
        val sp1 = sp.split("&g=", -1)
        val requestDate = DateUtils.getDateForRequest(line)
        if (sp1(1) == "") {
          Row("pyw", 0, requestDate)
        } else {
          Row(sp1(0), Integer.parseInt(sp1(1).split("&", -1)(0)), requestDate)
        }
      } catch {
        case ex: Exception => {
          Row("pyw", 0, "0000-00-00")
        }
      }
    })
    val requestStruct = (new StructType).add("expand_channel", StringType)
      .add("game_id", IntegerType).add("publish_time", StringType)
    val requestDF = sqlContext.createDataFrame(requestRdd, requestStruct);
    requestDF.registerTempTable("tmp_request")
    sqlContext.sql("select a.* from tmp_request a join lastPubGame b on a.game_id=b.game_id").registerTempTable("request")

    //注册日志，Struct，DF,temp_table
    val regiRdd = rdd.filter(line => line.contains("bi_regi")).map(line => {
      val splited = line.split("\\|", -1)
      try {
        Row(splited(3), StringUtils.defaultEmptyTo21(splited(13)), Integer.parseInt(splited(4)), Integer.parseInt(splited(6)), splited(5).split(":")(0))
      } catch {
        case ex: Exception => {
          Row("", "pyw", -1, 0, "0000-00-00")
        }
      }
    })
    val regiStruct = (new StructType).add("game_account", StringType).add("expand_channel", StringType).add("game_id", IntegerType).add("resource_id", IntegerType).add("publish_time", StringType)
    val regiDF = sqlContext.createDataFrame(regiRdd, regiStruct)
    regiDF.registerTempTable("tmp_regi")

    sqlContext.sql("select a.* from tmp_regi a join lastPubGame b on a.game_id=b.game_id where a.game_id <> -1").registerTempTable("regi")
  }

  /**
    * 获取全量注册数据
    *
    * @param sc
    * @param sqlContext
    */
  def getRegiFullData(sc: SparkContext, sqlContext: SQLContext): Unit = {
    val regiRdd = sc.newAPIHadoopFile(ConfigurationUtil.getProperty("gamepublish.offline.regi"),
      classOf[CombineTextInputFormat],
      classOf[LongWritable],
      classOf[Text]).map(line => line._2.toString)
    //val regiRdd = sc.textFile(PropertiesUtils.getRelativePathValue("gamepublish.offline.regi"))
    val mapRegiRdd = regiRdd.map(line => {
      val splited = line.split("\\|", -1)
      try {
        Row(splited(3), StringUtils.defaultEmptyTo21(splited(13)), Integer.parseInt(splited(4)), Integer.parseInt(splited(6)), splited(5).split(":")(0))
      } catch {
        case ex: Exception => {
          Row("", "pyw", -1, 0, "0000-00-00")
        }
      }
    })
    val regiStruct = (new StructType).add("game_account", StringType).add("expand_channel", StringType).add("game_id", IntegerType).add("resource_id", IntegerType).add("publish_time", StringType)
    val regiDF = sqlContext.createDataFrame(mapRegiRdd, regiStruct)
    regiDF.registerTempTable("tmp_full_regi")
    sqlContext.sql("select a.* from tmp_full_regi a join lastPubGame b on a.game_id=b.game_id where a.game_id <> -1").registerTempTable("full_regi")
  }

  /**
    * 获取全量订单数据
    *
    * @param sc
    * @param sqlContext
    */
  def getOrderFullData(sc: SparkContext, sqlContext: SQLContext): Unit = {
    val orderRddPre = sc.newAPIHadoopFile(ConfigurationUtil.getProperty("gamepublish.offline.order"),
      classOf[CombineTextInputFormat],
      classOf[LongWritable],
      classOf[Text]).map(line => line._2.toString)
    //订单日志，Struct，DF,temp_table
    val orderRdd = orderRddPre.filter(line => line.contains("bi_order")).map(line => {
      try {
        val splited = line.split("\\|", -1)
        Row(splited(5), java.lang.Double.parseDouble(splited(10)), java.lang.Integer.parseInt(splited(19)), splited(6).split(":")(0))
      } catch {
        case ex: Exception => {
          Row("pyww", 0, 0, "00-00-00 00")
        }
      }
    })
    val orderStruct = (new StructType).add("game_account", StringType).add("paymoney", DoubleType).add("order_state", IntegerType).add("publish_time", StringType)
    val orderDF = sqlContext.createDataFrame(orderRdd, orderStruct);
    orderDF.registerTempTable("tmp_full_order")
    val nowDate = DateUtils.getTodayDate()
    sqlContext.sql("select * from tmp_full_order where order_state = 4 and split(publish_time,' ')[0]='" + nowDate + "'").registerTempTable("full_order")
  }

  /**
    * 把登录数据转换为临时表--按天计算
    *
    * @param rdd
    * @param sqlContext
    */
  def convertLogsToLoginDfTmpTable(rdd: RDD[String], sqlContext: SQLContext): Unit = {
    //登录日志，Struct，DF,temp_table
    //2016-09-22 15:06:48,924 [INFO] login: bi_login|ad3198ca-cf95-4c||tc172905839|2016-09-22 15:06:48|||132|192.168.20.228
    val loginRdd = rdd.filter(line => line.contains("bi_login")).map(line => {
      try {
        val splited = line.split("\\|", -1)
        Row(splited(3), splited(4).split(" ")(0), StringUtils.defaultEmptyTo21(splited(6)), Integer.parseInt(splited(7)))
      } catch {
        case ex: Exception => {
          Row("", "0000-00-00 00", "pyw", 0)
        }
      }
    })
    val loginStruct = (new StructType).add("game_account", StringType).add("login_time", StringType).add("expand_channel", StringType).add("game_id", IntegerType)
    val loginDF = sqlContext.createDataFrame(loginRdd, loginStruct);
    loginDF.registerTempTable("tmp_login")
    sqlContext.sql("select a.* from tmp_login a join lastPubGame b on a.game_id=b.game_id").registerTempTable("login")
  }

  /**
    * 把过滤后的登录数据转换为临时表-分时计算
    *
    * @param rdd
    * @param sqlContext
    */
  def converToLoginHourDfTmpTable(rdd: RDD[String], sqlContext: SQLContext): Unit = {

    val loginRdd = rdd.map(line => {
      try {
        val splited = line.split("\\|", -1)
        Row(splited(3), splited(4).split(":")(0), StringUtils.defaultEmptyTo21(splited(6)), Integer.parseInt(splited(8)))
      } catch {
        case ex: Exception => {
          Row("", "0000-00-00 00", "pyw", 0)
        }
      }
    })
    val loginStruct = (new StructType).add("game_account", StringType).add("login_time", StringType).add("expand_channel", StringType).add("game_id", IntegerType)
    val loginDF = sqlContext.createDataFrame(loginRdd, loginStruct);
    loginDF.registerTempTable("tmp_login_hour")
    sqlContext.sql("select a.* from tmp_login_hour a join lastPubGame b on a.game_id=b.game_id").registerTempTable("login_hour")
  }

  /**
    * 把激活数据转换为临时表
    *
    * @param rdd
    * @param sqlContext
    */
  def convertLogsToActiveDfTmpTable(rdd: RDD[String], sqlContext: SQLContext): Unit = {
    //激活日志，Struct，DF,temp_table
    val activeRdd = rdd.filter(line => line.contains("bi_active")).map(line => {
      try {
        val splited = line.split("\\|", -1)
        val imie = splited(8).split("&")(0)
        Row(Integer.parseInt(splited(1)), StringUtils.defaultEmptyTo21(splited(4)), imie, splited(5))
      } catch {
        case ex: Exception => {
          Row(0, "pyw", "", "")
        }
      }
    })
    val activeStruct = (new StructType).add("game_id", IntegerType).add("expand_channel", StringType)
      .add("imie", StringType).add("update_time", StringType)
    val activeDF = sqlContext.createDataFrame(activeRdd, activeStruct)
    activeDF.registerTempTable("active")
  }

  /**
    * 获取渠道投放日志
    *
    * @param rdd
    * @param sqlContext
    */
  def convertLogsToChannelDfTmpTable(rdd: RDD[String], sqlContext: SQLContext): Unit = {
    //2016-09-27 23:13:18,132 [INFO] bi: bi_channel|2|zht_xwst_10004|100.97.15.52|2016-09-27 23:13:18|2216
    val channelRdd = rdd.filter(line => line.contains("bi_channel")).map(line => {
      try {
        val splited = line.split("\\|", -1)
        Row(splited(1).toInt, StringUtils.defaultEmptyTo21(splited(2)), splited(5).toInt,
          splited(4).split(" ")(0), splited(6), splited(7).toInt)
      } catch {
        case ex: Exception => {
          Row(0, "pyw", 0, "0000-00-00 00", "", 0)
        }
      }
    })
    val channelStruct = (new StructType).add("type", IntegerType)
      .add("expand_channel", StringType).add("game_id", IntegerType).add("publish_time", StringType)
      .add("os_info", StringType).add("os", IntegerType)
    val channelDF = sqlContext.createDataFrame(channelRdd, channelStruct);
    channelDF.registerTempTable("tmp_channel")
    sqlContext.sql("select a.* from tmp_channel a join lastPubGame b on a.game_id=b.game_id where a.type=1").registerTempTable("channel")
  }

  def parseChannelToTmpTable(rdd: RDD[String], sqlContext: SQLContext) = {
    //2017-05-08 17:38:01,057 [INFO] bi: bi_channel|3|eaaqy_tjzt_3151208029|100.97.15.51|2017-05-08 17:38:01|3208||
    val channelRdd = rdd.filter(line => line.contains("bi_channel")).map(line => {
      try {
        val splited = line.split("\\|", -1)
        //expand_channel game_id click_date type_id ip
        Row(StringUtils.defaultEmptyTo21(splited(2)), Integer.parseInt(splited(5)), splited(4).split(" ")(0), Integer.parseInt(splited(1)), splited(3))
      } catch {
        case ex: Exception => {
          Row("pyw", 0, "0000-00-00", 0, "")
        }
      }
    })
    val channelStruct = (new StructType)
      .add("expand_channel", StringType).add("game_id", IntegerType).add("publish_time", StringType).add("type_id", IntegerType).add("ip", StringType)
    val channelDF = sqlContext.createDataFrame(channelRdd, channelStruct);
    channelDF.registerTempTable("channel")
  }

  def parseChannelToTmpTableNew(rdd: RDD[String], sqlContext: SQLContext) = {
    //2017-05-08 17:38:01,057 [INFO] bi: bi_channel|3|eaaqy_tjzt_3151208029|100.97.15.51|2017-05-08 17:38:01|3208||
    val channelRdd = rdd.filter(line => line.contains("bi_channel")).map(line => {
      try {
        val splited = line.split("\\|", -1)
        //pkg_id game_id click_time type_id ip
        Row(StringUtils.getArrayChannel(splited(2))(2), Integer.parseInt(splited(5)), splited(4), Integer.parseInt(splited(1)), splited(3))
      } catch {
        case ex: Exception => {
          Row("", 0, "0000-00-00 00:00:00", 0, "")
        }
      }
    })
    val channelStruct = (new StructType)
      .add("pkg_id", StringType).add("game_id", IntegerType).add("click_time", StringType).add("type_id", IntegerType).add("ip", StringType)
    val channelDF = sqlContext.createDataFrame(channelRdd, channelStruct);
    channelDF.registerTempTable("channel")
  }


  /**
    * parse request log detail
    *
    * @param rdd
    * @param sqlContext
    * @return
    */
  def parseRequestToTmpTable(rdd: RDD[String], sqlContext: SQLContext) = {

    val requestRdd = rdd.filter(line => !line.contains("HTTP/1.0\" 404")).filter(line => StringUtils.isRequestLog(line, ".*0800] \"GET /Ssxy/loadComplete[?]p=[\\d|_|a-z|A-Z]+&g=[\\d]+.*")).map(line => {
      try {
        val ip = line.split("\"", -1)(6).split(",")(0)
        val sp = line.split(" ")(6).split("p=", -1)(1).split("\"\"", -1)(0)
        val sp1 = sp.split("&g=", -1)
        val requestDate = DateUtils.getDateForRequest(line)
        if (sp1(1) == "") {
          Row("pyw", 0, requestDate)
        } else {
          //expand_channel game_id request_date type_id ip        >channel日志中的type_id是没有4的，4是从网络请求日志计算来的<
          Row(StringUtils.defaultEmptyTo21(sp1(0)), Integer.parseInt(sp1(1).split("&", -1)(0)), requestDate, 4, ip)
        }
      } catch {
        case ex: Exception => {
          Row("pyw", 0, "0000-00-00", 0, "")
        }
      }
    })
    val requestStruct = (new StructType).add("expand_channel", StringType)
      .add("game_id", IntegerType).add("publish_time", StringType).add("type_id", IntegerType).add("ip", StringType)
    val requestDF = sqlContext.createDataFrame(requestRdd, requestStruct);
    requestDF.registerTempTable("request")
  }

  def parseRequestToTmpTableNew(rdd: RDD[String], sqlContext: SQLContext) = {

    val requestRdd = rdd.filter(line => !line.contains("HTTP/1.0\" 404")).filter(line => StringUtils.isRequestLog(line, ".*0800] \"GET /Ssxy/loadComplete[?]p=[\\d|_|a-z|A-Z]+&g=[\\d]+.*")).map(line => {
      try {
        val ip = line.split("\"", -1)(6).split(",")(0).trim
        val sp = line.split(" ")(6).split("p=", -1)(1).split("\"\"", -1)(0)
        val sp1 = sp.split("&g=", -1)
        val requestTime = DateUtils.getDateForRequestNew(line)
        if (sp1(1) == "") {
          Row("", 0, requestTime, 0, "")
        } else {
          //pkg_id game_id request_time type_id ip        >channel日志中的type_id是没有4的，4是从落地页点击请求计算得来<
          Row(StringUtils.getArrayChannel(sp1(0))(2), Integer.parseInt(sp1(1).split("&", -1)(0)), requestTime, 4, ip)
        }
      } catch {
        case ex: Exception => {
          Row("", 0, "0000-00-00 00:00:00", 0, "")
        }
      }
    })
    val requestStruct = (new StructType)
      .add("pkg_id", StringType).add("game_id", IntegerType).add("click_time", StringType).add("type_id", IntegerType).add("ip", StringType)
    val requestDF = sqlContext.createDataFrame(requestRdd, requestStruct);
    requestDF.registerTempTable("request")
  }

  def convertOrderToDfTmpTable(rdd: RDD[String], sqlContext: SQLContext): Unit = {
    //订单日志，Struct，DF,temp_table
    val orderRdd = rdd.filter(line => {
      val arr = line.split("\\|", -1)
      //排除截断日志   只取存在订单号的数据 只取直充  只取状态为4的数据
      arr(0).contains("bi_order") && arr(22).contains("6") && arr(19).toInt == 4 && arr.length >= 25 && arr(2).trim.length > 0
    }).mapPartitions(it => {

      val pool: JedisPool = JedisUtil.getJedisPool()
      val jedis: Jedis = pool.getResource;

      val rdd = it.map(line => {
        try {
          val splited = line.split("\\|", -1)
          var totalAmt = 0.0
          if (splited(13) != "") {
            totalAmt = java.lang.Double.parseDouble(splited(13))
          }
          jedis.select(3)
          if (!jedis.exists("kpi_orderexists|" + splited(2) + "|" + splited(6) + "|" + splited(5))) //检查redis缓存中是否存在已经半小时内处理的订单
          {
            jedis.set("kpi_orderexists|" + splited(2) + "|" + splited(6) + "|" + splited(5), "1")
            jedis.expire("kpi_orderexists|" + splited(2) + "|" + splited(6) + "|" + splited(5), 3600)
            //game_account,order_status,order_time(这里取的是到小时),imei,game_id,ori_price,total_amt,
            Row(splited(5), java.lang.Integer.parseInt(splited(19)), splited(6).split(":")(0), splited(24),
              java.lang.Integer.parseInt(splited(7)), java.lang.Double.parseDouble(splited(10)),
              totalAmt, java.lang.Integer.parseInt(splited(22)))
            //进缓存并且存半小时

          } else
            Row("pyww", 0, "00-00-00 00", "", 0, 0, 0, 0)
        } catch {
          case ex: Exception => {
            Row("pyww", 0, "00-00-00 00", "", 0, 0, 0, 0)
          }
        }
      })
      pool.returnResource(jedis)
      pool.destroy()
      rdd
    })


    val orderStruct = (new StructType).add("game_account", StringType).add("order_status", IntegerType)
      .add("publish_time", StringType).add("imei", StringType).add("game_id", IntegerType).add("ori_price", DoubleType)
      .add("total_amt", DoubleType).add("prod_type", IntegerType)

    val orderDF = sqlContext.createDataFrame(orderRdd, orderStruct);
    orderDF.registerTempTable("ods_order_tmp")

    sqlContext.sql("select o.* from ods_order_tmp o join lastPubGame pg on o.game_id = pg.game_id where o.order_status = 4 and prod_type=6").cache()
      .registerTempTable("ods_order")

  }


  /**
    *
    * @param typeId
    * @return
    */
  def getValueByClickType(typeId: Int) = {
    var adpageClickUv = 0
    var requestClickUv = 0
    var showUv = 0
    var downloadUv = 0
    if (typeId == 1) {
      showUv = 1
    } else if (typeId == 2) {
      downloadUv = 1
    } else if (typeId == 3) {
      adpageClickUv = 1
    } else if (typeId == 4) {
      requestClickUv = 1
    }
    Array[Int](adpageClickUv, requestClickUv, showUv, downloadUv)
  }
}
