package com.xiaopeng.bi.utils.action

import java.text.SimpleDateFormat
import java.util.Date

import com.xiaopeng.bi.utils._
import com.xiaopeng.bi.utils.action.ThirdDataActs.logger
import com.xiaopeng.bi.utils.dao.ThirdDataDao
import net.sf.json.JSONObject
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import redis.clients.jedis.JedisPool

import scala.collection.mutable.ArrayBuffer


/**
  * Created by bigdata on 17-10-13.
  */
object ThirdDataOffLineActs {
  /**
    * 离线那任务用来更新 groupid,heap_people,medium_account,remark
    *
    * @param startday
    */
  def updateMediumAccountInfo(startday: String) = {
    val conn = JdbcUtil.getConn()
    val update_sql = "update bi_ad_channel_stats set group_id=?,medium_account=?,head_people=? where publish_date=? and game_id=? and pkg_id=?"
    val pstmt = conn.prepareStatement(update_sql)
    val pool: JedisPool = JedisUtil.getJedisPool;
    val jedis = pool.getResource
    //从redis 6号库中取出缓存的pkg_id
    val jedis6 = pool.getResource
    jedis6.select(6)
    //hgetAll方法会比较耗时，所以在这个方法之后再打开connFX的链接
    val keyValues = jedis6.hgetAll("thirddataOffLine|" + startday)
    val connFx = JdbcUtil.getXiaopeng2FXConn()
    val values = keyValues.values().iterator()
    while (values.hasNext) {
      val pkg_id = values.next()
      println("redis_pkg_id : " + pkg_id)
      var game_id = 0
      if (pkg_id.contains("M")) {
        game_id = pkg_id.substring(0, pkg_id.indexOf("M")).toInt
      } else if (pkg_id.contains("Q")) {
        game_id = pkg_id.substring(0, pkg_id.indexOf("Q")).toInt
      }
      val redisValue: Array[String] = CommonsThirdData.getRedisValue(game_id, pkg_id, startday, jedis, connFx)
      val group_id = redisValue.apply(6)
      val medium_account = redisValue(2)
      val head_people = redisValue(5)
      pstmt.setString(1, group_id)
      pstmt.setString(2, medium_account)
      pstmt.setString(3, head_people)
      pstmt.setString(4, startday)
      pstmt.setInt(5, game_id)
      pstmt.setString(6, pkg_id)
      pstmt.executeUpdate()
    }
    pstmt.close()
    pool.returnBrokenResource(jedis)
    pool.returnBrokenResource(jedis6)
    pool.destroy()
    conn.close()
    connFx.close()

  }


  /**
    * 离线那任务用来更新 groupid,heap_people,medium_account,remark
    *
    * @param startday
    */
  def updateMediumAccountInfo2(startday: String) = {
    val conn = JdbcUtil.getConn()
    val connFx = JdbcUtil.getXiaopeng2FXConn()

    val pool: JedisPool = JedisUtil.getJedisPool;
    val jedis = pool.getResource

    //获取需要更新的分包
    val pkg_sql = "select distinct pkg_id from bi_ad_channel_stats where publish_date='" + startday + "' and pkg_id != '' and medium != 0"
    val stmt = conn.createStatement()
    val rs = stmt.executeQuery(pkg_sql);

    //更新需要更新的字段
    val update_sql = "update bi_ad_channel_stats set group_id=?,medium_account=?,head_people=?,remark=? where publish_date=? and game_id=? and pkg_id=?"
    val pstmt = conn.prepareStatement(update_sql)

    //分包备注
    var remark = ""

    while (rs.next()) {
      val pkg_id = rs.getString("pkg_id")
      var game_id = 0
      if (pkg_id.contains("M")) {
        game_id = pkg_id.substring(0, pkg_id.indexOf("M")).toInt
        remark = CommonsThirdData.getMediumRemark(pkg_id, connFx)
      } else if (pkg_id.contains("Q")) {
        game_id = pkg_id.substring(0, pkg_id.indexOf("Q")).toInt
        remark = CommonsThirdData.getChannelRemark(pkg_id, connFx)
      }
      val redisValue: Array[String] = CommonsThirdData.getRedisValue(game_id, pkg_id, startday, jedis, connFx)
      val group_id = redisValue.apply(6)
      val medium_account = redisValue(2)
      val head_people = redisValue(5)

      pstmt.setString(1, group_id)
      pstmt.setString(2, medium_account)
      pstmt.setString(3, head_people)
      pstmt.setString(4, remark)
      pstmt.setString(5, startday)
      pstmt.setInt(6, game_id)
      pstmt.setString(7, pkg_id)
      pstmt.executeUpdate()
    }
    rs.close()
    stmt.close()
    pstmt.close()
    pool.returnBrokenResource(jedis)
    pool.destroy()
    conn.close()
    connFx.close()

  }

  /**
    * 格式化广告点击数据
    *
    * @param clickLogRDD
    * @return
    */
  def formatThirdData(clickLogRDD: RDD[String], startday: String): RDD[(String, String, String, String, String, Int, String, String, String, String, String, String, String, String)] = {
    //二：数据过滤
    //过滤掉钱大师的点击日志
    val thirdData = clickLogRDD.filter(line => {
      //过滤出不包含钱大师的，不为""的，只是startday的日志
      !line.contains("bi_adv_money") && !line.equals("") && line.contains("{") && line.contains("}") && line.contains(startday)
    }).map(line => {
      line.substring(line.indexOf("{"), line.length)
    })

    //    println("thirdDataRDD : " + thirdData.count())

    val formatThirdata = thirdData.map(line => {
      val simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      val jsStr = JSONObject.fromObject(line)
      if (line.contains("bi_adv_momo_click")) {
        try {
          (jsStr.get("pkg_id").toString,
            if (jsStr.get("os").toString.equals("0") || jsStr.get("os").toString.equals("2")) "android" else if (jsStr.get("os").toString.equals("1")) "ios" else "wp",
            jsStr.get("imei").toString,
            if (jsStr.get("ts").toString.length == 10) simpleDateFormat.format(new Date(jsStr.get("ts").toString.toLong * 1000)) else simpleDateFormat.format(new Date(jsStr.get("ts").toString.toLong)),
            jsStr.get("callback").toString,
            1, "", "", "", "", "", "0", "", ""
          )
        } catch {
          case e: Exception =>
            ("", "", "", "0", "", 1, "", "", "", "", "", "0", "", "")
        }

      } else if (line.contains("bi_adv_baidu_click")) {
        try {
          val jsStr = JSONObject.fromObject(line)
          (jsStr.get("pkg_id").toString,
            if (jsStr.get("os").toString.equals("2")) "android" else if (jsStr.get("os").toString.equals("1")) "ios" else "wp",
            jsStr.get("imei").toString,
            if (jsStr.get("ts").toString.length == 10) simpleDateFormat.format(new Date(jsStr.get("ts").toString.toLong * 1000)) else simpleDateFormat.format(new Date(jsStr.get("ts").toString.toLong)),
            jsStr.get("callback_url").toString,
            2, jsStr.get("aid").toString, jsStr.get("uid").toString, jsStr.get("pid").toString, jsStr.get("ip").toString, if (jsStr.get("ua") != null) jsStr.get("ua").toString else "", "0", "", ""
          )
        } catch {
          case e: Exception =>
            ("", "", "", "0", "", 2, "", "", "", "", "", "0", "", "")
        }

      } else if (line.contains("bi_adv_jinretoutiao_click")) {
        try {
          (jsStr.get("pkg_id").toString,
            if (jsStr.get("os").toString.equals("0")) "android" else if (jsStr.get("os").toString.equals("1")) "ios" else "wp",
            jsStr.get("imei").toString,
            if (jsStr.get("timestamp").toString.length == 10) simpleDateFormat.format(new Date(jsStr.get("timestamp").toString.toLong * 1000)) else simpleDateFormat.format(new Date(jsStr.get("timestamp").toString.toLong)),
            jsStr.get("callback").toString,
            3, jsStr.get("cid").toString, "", jsStr.get("adid").toString, "", "", "0", "",
            if (jsStr.get("os").toString.equals("0")) jsStr.get("androidid").toString else ""
          )
        } catch {
          case e: Exception =>
            ("", "", "", "0", "", 3, "", "", "", "", "", "0", "", "")

        }

      } else if (line.contains("bi_adv_aiqiyi_click")) {
        try {
          (jsStr.get("pkg_id").toString,
            if (jsStr.get("os").toString.equals("2")) "android" else if (jsStr.get("os").toString.equals("1")) "ios" else "wp",
            jsStr.get("udid").toString,
            if (jsStr.get("ts").toString.length == 10) simpleDateFormat.format(new Date(jsStr.get("ts").toString.toLong * 1000)) else simpleDateFormat.format(new Date(jsStr.get("ts").toString.toLong)),
            //爱奇异不用回传激活url
            "", 4, "", "", "", "", "", "0", "", "")
        } catch {
          case e: Exception =>
            ("", "", "", "0", "", 4, "", "", "", "", "", "0", "", "")
        }

      } else if (line.contains("bi_adv_uc_click")) {
        try {
          (jsStr.get("pkg_id").toString,
            if (jsStr.get("os").toString.equals("1")) "android" else if (jsStr.get("os").toString.equals("0")) "ios" else "wp",
            jsStr.get("imei").toString,
            if (jsStr.get("time").toString.length == 10) simpleDateFormat.format(new Date(jsStr.get("time").toString.toLong * 1000)) else simpleDateFormat.format(new Date(jsStr.get("time").toString.toLong)),
            jsStr.get("callback").toString,
            5, jsStr.get("cid").toString, "", jsStr.get("aid").toString, "", "", "0", "", ""
          )
        } catch {
          case e: Exception =>
            ("", "", "", "0", "", 5, "", "", "", "", "", "0", "", "")
        }

      } else if (line.contains("bi_adv_guangdiantong_click")) {
        try {
          (jsStr.get("pkg_id").toString,
            if (jsStr.get("app_type").toString.toLowerCase.contains("android")) "android" else if (jsStr.get("app_type").toString.toLowerCase.contains("ios")) "ios" else "wp",
            jsStr.get("muid").toString,
            if (jsStr.get("click_time").toString.length == 10) simpleDateFormat.format(new Date(jsStr.get("click_time").toString.toLong * 1000)) else simpleDateFormat.format(new Date(jsStr.get("click_time").toString.toLong)),
            jsStr.get("callback").toString,
            6, "", "", "", "", "", "0", "", ""
          )
        } catch {
          case e: Exception =>
            ("", "", "", "0", "", 6, "", "", "", "", "", "0", "", "")
        }

      } else if (line.contains("bi_adv_medium_click")) {
        //取出媒介编号adv_id
        val adv_id = jsStr.get("adv_id").toString.toInt
        //根据媒介编号去判断是哪家媒介
        if (adv_id == 7) {
          try {
            (
              jsStr.get("pkg_id").toString,
              if (jsStr.get("os").toString.equals("1")) "android" else if (jsStr.get("os").toString.equals("0")) "ios" else "wp",
              jsStr.get("imei").toString,
              if (jsStr.get("time").toString.length == 10) simpleDateFormat.format(new Date(jsStr.get("time").toString.toLong * 1000)) else simpleDateFormat.format(new Date(jsStr.get("time").toString.toLong)),
              jsStr.get("callback").toString,
              7,
              jsStr.get("cid").toString,
              "",
              jsStr.get("aid").toString, "", "", "0", "", ""
            )
          } catch {
            case e: Exception =>
              ("", "", "", "0", "", 7, "", "", "", "", "", "0", "", "")
          }

        } else if (adv_id == 8) {
          try {
            (
              jsStr.get("pkg_id").toString,
              if (jsStr.get("os").toString.equals("0")) "android" else if (jsStr.get("os").toString().equals("1")) "ios" else "wp",
              jsStr.get("imei").toString,
              if (jsStr.get("ts").toString.length == 10) simpleDateFormat.format(new Date(jsStr.get("ts").toString.toLong * 1000)) else simpleDateFormat.format(new Date(jsStr.get("ts").toString.toLong)),
              jsStr.get("callback").toString,
              8, "", "", "", jsStr.get("ip").toString, jsStr.get("ua").toString, "0", "", ""
            )
          } catch {
            case e: Exception =>
              ("", "", "", "0", "", 8, "", "", "", "", "", "0", "", "")
          }

        } else if (adv_id == 9) {
          try {
            (
              jsStr.get("pkg_id").toString,
              if (jsStr.get("os").toString.equals("0")) "android" else if (jsStr.get("os").toString.equals("1")) "ios" else "wp",
              jsStr.get("imei").toString,
              if (jsStr.get("ts").toString.length == 10) simpleDateFormat.format(new Date(jsStr.get("ts").toString.toLong * 1000)) else simpleDateFormat.format(new Date(jsStr.get("ts").toString.toLong)),
              jsStr.get("callback").toString,
              9, "", "", "", "", "", "0", "", ""
            )

          } catch {
            case e: Exception =>
              ("", "", "", "0", "", 9, "", "", "", "", "", "0", "", "")
          }

        } else if (adv_id == 17) {
          try {
            //dongqiudi
            (
              jsStr.get("pkg_id").toString,
              if (jsStr.get("os").toString.equals("android")) "android" else if (jsStr.get("os").toString.equals("ios")) "ios" else "wp",
              if (jsStr.get("os").toString.equals("android")) jsStr.get("imei").toString else if (jsStr.get("os").toString.equals("ios")) jsStr.get("idfa").toString else "",
              if (jsStr.get("qt").toString.length == 10) simpleDateFormat.format(new Date(jsStr.get("qt").toString.toLong * 1000)) else simpleDateFormat.format(new Date(jsStr.get("qt").toString.toLong)),
              jsStr.get("callback").toString,
              17, "", "", "", "", "", "0", "",
              if (jsStr.get("os").toString.equals("android")) jsStr.get("androidID").toString else ""
            )
          } catch {
            case e: Exception =>
              ("", "", "", "0", "", 17, "", "", "", "", "", "0", "", "")
          }

        } else if (adv_id == 18) {
          try {
            //新浪扶翼
            (
              jsStr.get("pkg_id").toString,
              if (jsStr.get("os").toString.equals("0")) "android" else if (jsStr.get("os").toString.equals("1")) "ios" else "wp",
              jsStr.get("devid").toString,
              if (jsStr.get("ts").toString.length == 10) simpleDateFormat.format(new Date(jsStr.get("ts").toString.toLong * 1000)) else simpleDateFormat.format(new Date(jsStr.get("ts").toString.toLong)),
              jsStr.get("callback").toString,
              18, "", "", "", jsStr.get("ip").toString, jsStr.get("ua").toString, "0", "", ""
            )
          } catch {
            case e: Exception =>
              ("", "", "", "0", "", 18, "", "", "", "", "", "0", "", "")
          }

        } else if (adv_id == -1) {
          try {
            //都是渠道来的数据
            (
              jsStr.get("pkg_id").toString,
              if (jsStr.get("os").toString.equals("0")) "android" else if (jsStr.get("os").toString().equals("1")) "ios" else "wp",
              jsStr.get("imei").toString,
              if (jsStr.get("ts").toString.length == 10) simpleDateFormat.format(new Date(jsStr.get("ts").toString.toLong * 1000)) else simpleDateFormat.format(new Date(jsStr.get("ts").toString.toLong)),
              "", -1, "", "", "", jsStr.get("ip").toString, jsStr.get("ua").toString, jsStr.get("channel_main_id").toString, jsStr.get("channel_name").toString, ""
            )
          } catch {
            case e: Exception =>
              ("", "", "", "0", "", -1, "", "", "", "", "", "0", "", "")
          }

        } else {
          //没有匹配到媒介的数据，就当异常数据处理
          ("", "", "", "0", "", 100, "", "", "", "", "", "0", "", "")
        }

      } else {
        //异常数据处理
        ("", "", "", "0", "", 100, "", "", "", "", "", "0", "", "")
      }
    })
    //(pkg_id,os,imei,ts,callback,adv_id,aid,uid,pid,ip,ua,channel_main_id,channel_name,androidID)
    return formatThirdata
  }

  /**
    * 点击数据的处理
    *
    * @param click_data
    * @param startday
    * @return
    */
  def clickData(formatThirddataRDD: RDD[(String, String, String, String, String, Int, String, String, String, String, String, String, String, String)], click_data: RDD[Row], sparkContext: SparkContext, startday: String) = {
    //1:点击明细表
    //formatThirddataRDD是2天的数据，根据startday过滤出1天的数据
    clickDetail(formatThirddataRDD, startday)

    //2:统计表：点击数，点击设备数
    //Row(pkg_id, game_id, imei, imei_md5_upper, os, clickTime, callback, advName, aid, uid, pid, ip, devType, sysVersion, channel_main_id, channel_name, androidID)
    val click_data_Struct = new StructType()
      .add("pkg_id", StringType)
      .add("game_id", IntegerType)
      .add("imei", StringType)
      .add("imei_md5_upper", StringType)
      .add("os", StringType)
      .add("clickTime", StringType)
      .add("callback", StringType)
      .add("adv_name", IntegerType)
      .add("aid", StringType)
      .add("uid", StringType)
      .add("pid", StringType)
      .add("ip", StringType)
      .add("deviceType", StringType)
      .add("systemVersion", StringType)
      .add("channel_main_id", IntegerType)
      .add("channel_name", StringType)
      .add("androidID", StringType)

    val hiveContext = new HiveContext(sparkContext)
    val clickDataFrame = hiveContext.createDataFrame(click_data, click_data_Struct)
    clickDataFrame.registerTempTable("ods_click")

    //1:媒介分包
    val medium_click_stats_sql = "select \nto_date(clickTime) clickDate,\ngame_id,\npkg_id,\nadv_name,\ncount(*) click_num,\ncount(distinct imei) click_dev_num\nfrom ods_click where adv_name in (1,2,3,4,5,6,7,8,9,17) and to_date(clickTime) = 'startday' group by to_date(clickTime),game_id,pkg_id,adv_name"
      .replace("startday", startday)
    val mediumClickStatsDF = hiveContext.sql(medium_click_stats_sql)
    foreachClickStatsDF(mediumClickStatsDF)

    //2:渠道分包,以及按照渠道方式处理的媒介分包
    val channel_click_stats_sql = "select \nto_date(clickTime) clickDate,\ngame_id,\npkg_id,\nadv_name,\ncount(*) click_num,\ncount(distinct ip,deviceType,systemVersion) click_dev_num\nfrom ods_click where adv_name in (-1,10,11,12,13,14,15,16) and to_date(clickTime) = 'startday' group by to_date(clickTime),game_id,pkg_id,adv_name"
      .replace("startday", startday)
    val channelClickStatsDF = hiveContext.sql(channel_click_stats_sql)
    foreachClickStatsDF(channelClickStatsDF)
  }

  /**
    * 激活匹配点击
    *
    * @param rdd
    * @return
    */
  def activeMatchClick(rdd: RDD[(String, String, String, String, String, String, String, String, String)]) = {
    rdd.foreachPartition(fp => {
      val pool: JedisPool = JedisUtil.getJedisPool;
      val jedis = pool.getResource
      val jedis6 = pool.getResource
      jedis6.select(6)
      val conn = JdbcUtil.getConn()
      //      val connFx = JdbcUtil.getXiaopeng2FXConn()
      val stmt = conn.createStatement()
      fp.foreach(line => {
        //(3802,21,jlcy_lj_1,5896EEA3ECCF40FAA33AF1201DA8B3EF,2017-11-09 10:30:56,IOS,127.0.0.1,iPhone 6,10.3.3)
        val game_id = line._1.toInt
        //通过查询点击明细表，判断这个game_id是否做了广告--->这一步不许要判断了
        //if (CommonsThirdData.isNeedStaGameId(game_id, conn)) {
        val activelog_imei = line._4
        //221132093280616&64110abc12e796d5&f8:2f:48:a1:6d:20
        //安卓设备取最前面部分(15位数字)，苹果设备取完(32位英文数字大写)
        var imei = CommonsThirdData.getImei(activelog_imei)
        //按着ID取中间16位
        val androidID = CommonsThirdData.getAndroidID(activelog_imei)
        //os类型转换为int类型  1：android  2：ios
        val osInt = if (line._6.toLowerCase.contains("android")) 1 else 2
        //117.22.103.195
        val ip = line._7
        //iPhone 6s
        //国航iPhone 7
        val devTypeAll = line._8
        //iPhone
        var devType = ""
        if (!devTypeAll.equals("")) {
          if (devTypeAll.contains("iPhone")) {
            devType = "iPhone"
          } else if (devTypeAll.contains("iPad")) {
            devType = "iPad"
          } else if (devTypeAll.contains("iPod")) {
            devType = "iPod"
          }
        }
        //11.0.3
        val sysVersion = line._9
        //2017-09-09 00:00:00
        val activeTime = line._5
        //2017-09-09
        val activeDate = activeTime.substring(0, 10)
        //激活匹配点击,7天内有效, 往前推7天,如果是7天内，这里传入的参数就因该是-6
        val activeDate6dayBefore = CommonsThirdData.getDateBeforeParams(activeDate, -6)
        //jg, pkg, imei, idfa.replace("-", ""), adv_name, ideaId, firstLevel, secondLevel,channel_main_id,channel_name
        var matched = Tuple10(0, "", "", "", 0, "", "", "", 0, "")

        //---------------通过imei和androidID分别判断该条激活日志是否已经匹配--------------1226新增
        //statusCode是否进行激活匹配点击的标记码
        //默认是false -> 不进行匹配 ,true -> 进行匹配
        var statusCode = false
        if (osInt == 2) {
          //IOS设备
          if (!imei.equals("") && !imei.matches("[0]{7,}")) {
            //激活日志中imei字段有效
            //根据拿到的imei，对同一个game_id下的激活设备，做永久去重
            statusCode = ThirdDataDao.checkImeiIsMatched(game_id, imei, conn, stmt)
            if (statusCode) {
              //匹配第一步：通过imei进行匹配

              //imei加密：因为有些媒介是带横杠的原值，有些是不带横杠的原值，因此这里用两种加密方式
              //1:imei加横杠再md5加密大写
              val imeiWithLine = CommonsThirdData.imeiPlusDash(imei.toUpperCase())
              val imeiWithLine_md5_upper = MD5Util.md5(imeiWithLine).toUpperCase
              //2:imei不加横杠md5加密大写
              val imei_md5_upper = MD5Util.md5(imei).toUpperCase
              //ios设备的匹配：imei + game_id
              //---------------------------->>>>>离线这里要注意，只统计数字，就不需要上报了<<<<<---------------
              matched = ThirdDataDao.iosMatchClickByImeiOffLine(imei, imeiWithLine_md5_upper, imei_md5_upper, game_id, activeDate6dayBefore, activeDate, conn)

              //匹配第二步：如果imei没有匹配到，如果IP和UA有效，再用UA+IP匹配一次
              if (matched._1 == 0) {
                if (!ip.equals("")) {
                  //ip有效
                  if (!devType.equals("")) {
                    // devType有效  --> 执行 game_id + ip + devType + sysVersion 匹配
                    if (!sysVersion.equals("")) {
                      //sysVersion有效  --> 执行 game_id + ip + devType + sysVersion 匹配
                      //---------------------------->>>>>离线这里要注意，只统计数字，就不需要上报了<<<<<---------------
                      matched = ThirdDataDao.iosMatchClickByIPAndDevAndSysOffLine(game_id, activeDate6dayBefore, activeDate, ip, devType, sysVersion, conn)
                      if (matched._1 == 0) {
                        //game_id + ip + devType + sysVersion 没有匹配到  --> 执行 game_id + ip + devType 匹配
                        //---------------------------->>>>>离线这里要注意，只统计数字，就不需要上报了<<<<<---------------
                        matched = ThirdDataDao.iosMatchClickByIPAndDevOffLine(game_id, activeDate6dayBefore, activeDate, ip, devType, conn)
                        if (matched._1 == 0) {
                          //game_id + ip + devType 没有匹配到  --> 执行 game_id + ip 匹配
                          //---------------------------->>>>>离线这里要注意，只统计数字，就不需要上报了<<<<<---------------
                          matched = ThirdDataDao.iosMatchClickByIPOffLine(game_id, activeDate6dayBefore, activeDate, ip, conn)
                        }
                      }
                    } else {
                      //sysVersion无效  --> 执行 game_id + ip + devType 匹配
                      //---------------------------->>>>>离线这里要注意，只统计数字，就不需要上报了<<<<<---------------
                      matched = ThirdDataDao.iosMatchClickByIPAndDevOffLine(game_id, activeDate6dayBefore, activeDate, ip, devType, conn)
                    }
                  } else {
                    // devType无效  --> 执行 game_id + ip 匹配
                    //---------------------------->>>>>离线这里要注意，只统计数字，就不需要上报了<<<<<---------------
                    matched = ThirdDataDao.iosMatchClickByIPOffLine(game_id, activeDate6dayBefore, activeDate, ip, conn)
                  }
                }
              }
            }
          } else {
            logger.info("ios设备imei无效，放弃匹配")
          }
        } else {
          //android设备
          if (!imei.equals("") && !imei.matches("[0]{7,}")) {
            //imei有效
            statusCode = ThirdDataDao.checkImeiIsMatched(game_id, imei, conn, stmt)
            if (statusCode) {
              //imei有效
              val imei_md5_upper = MD5Util.md5(imei).toUpperCase
              //---------------------------->>>>>离线这里要注意，只统计数字，就不需要上报了<<<<<---------------
              matched = ThirdDataDao.androidMatchClickByImeiOffLine(imei_md5_upper, game_id, activeDate6dayBefore, activeDate, conn)
              if (matched._1 == 0) {
                //imei没有匹配到，再用androidID匹配
                if (!androidID.equals("")) {
                  //androidID有效
                  statusCode = ThirdDataDao.checkAndroidIDIsMatched(game_id, androidID, conn, stmt)
                  if (statusCode) {
                    //androidID没有检测到激活 --> 执行匹配动作
                    //---------------------------->>>>>离线这里要注意，只统计数字，就不需要上报了<<<<<---------------
                    matched = ThirdDataDao.androidMatchClickByAndroidIDOffLine(androidID, game_id, activeDate6dayBefore, activeDate, conn)
                  }
                }
              }
            }
          } else {
            //imei无效
            if (!androidID.equals("")) {
              //androidID有效
              statusCode = ThirdDataDao.checkAndroidIDIsMatched(game_id, androidID, conn, stmt)
              if (statusCode) {
                //androidID没有检测到激活 -->执行匹配动作
                matched = ThirdDataDao.androidMatchClickByAndroidIDOffLine(androidID, game_id, activeDate6dayBefore, activeDate, conn)
              }
            } else {
              //imei和androidID都无效，放弃这条激活日志的处理
              logger.info("android设备imei和androidID都无效，放弃匹配")
            }
          }
        }

        //matched取出的是匹配后的设备的点击明细表数据，若能匹配得到则要算设备统计数据，并且写入激活明细表 计算注册时使用
        if (matched._1 >= 1) {
          //这个pkgCode ,android 和 ios 都取的是广告日志里面的
          val pkgCode = matched._2
          //离线处理这里，在这一步connFX链接会超时，如果不能优化，可以将这一步取消
          //缓存有激活的pkgCode
          //CommonsThirdData.cachePkgCode(pkgCode, activeDate, jedis6)
          //val redisValue: Array[String] = CommonsThirdData.getRedisValue(game_id, pkgCode, activeDate, jedis, connFx)
          val adv_name = matched._5

          val group_id = "0"
          val medium_account = ""
          val head_people = ""
          val idea_id = matched._6
          val first_level = matched._7
          val second_level = matched._8
          val channel_main_id = matched._9
          val channel_name = matched._10

          //ios和android的imei如果为空或者全是0，需要转换为"no_imei"再存储
          if (imei.equals("") || imei.matches("[0]{7,}")) {
            imei = "no_imei"
          }

          //分包备注
          val remark = ""
          //          if (pkgCode.contains("M")) {
          //            remark = CommonsThirdData.getMediumRemark(pkgCode, connFx)
          //          } else if (pkgCode.contains("Q")) {
          //            remark = CommonsThirdData.getChannelRemark(pkgCode, connFx)
          //          }

          //激活数 ： 激活匹配到点击
          val activeNum = 1
          //把激活匹配数据写入到明细，后期注册统计使用
          ThirdDataDao.insertActiveDetail(activeTime, imei, pkgCode, adv_name, game_id, osInt, channel_main_id, channel_name, ip, devType, sysVersion, androidID, conn)
          //激活统计,只算匹配量
          ThirdDataDao.insertActiveStat(activeDate, game_id, group_id, pkgCode, head_people, medium_account, adv_name, idea_id, first_level, second_level, activeNum, channel_main_id, channel_name, remark, conn)
        }
      })
      pool.returnBrokenResource(jedis)
      pool.returnResource(jedis6)
      pool.destroy()
      stmt.close()
      conn.close()
      //      connFx.close()
    })
  }

  /**
    * 注册匹配激活
    *
    * @param rdd
    * @return
    */
  def regiMatchActive(rdd: RDD[(String, Int, String, String, String, String)]) = {
    rdd.cache()
    rdd.foreachPartition(part => {
      val pool: JedisPool = JedisUtil.getJedisPool;
      val jedis = pool.getResource
      jedis.select(0)
      val jedis6 = pool.getResource
      jedis6.select(6)
      val conn = JdbcUtil.getConn()
      val stmt = conn.createStatement()
      //      val connFx = JdbcUtil.getXiaopeng2FXConn()
      part.foreach(line => {
        val gameId = line._2.toInt
        var imei = CommonsThirdData.getImei(line._5)
        val androidID = CommonsThirdData.getAndroidID(line._5)
        val gameAccount = line._1
        val osInt = if (line._6.toLowerCase.contains("android")) 1 else 2
        val regiTime = line._4
        val regiDate = line._4.substring(0, 10)
        //注册匹配激活，时间有效期是30天内，这里传入的参数该是-29
        val regiDate29dayBefore = CommonsThirdData.getDateBeforeParams(regiDate, -29)
        var matched = Tuple8(0, "", "", "", "", 0, "", "")
        //状态码，检测注册是否继续匹配
        var statusCode = false
        //根据imei以及game_account判断是否要进行匹配
        if (osInt == 2) {
          //ios
          if (!imei.equals("") && !imei.matches("[0]{7,}")) {
            //因为在本地文件缓存了上一批注册数据，并且将上一批数据和这一批数据合一起传入这一批，因此这里要做一次去重检查
            statusCode = CommonsThirdData.checkAccountIsMatched(gameAccount, stmt)
            if (statusCode) {
              //imei有效 -->执行匹配动作
              matched = CommonsThirdData.iosRegiMatchActive(imei, regiDate29dayBefore, regiDate, gameId, conn)
            }
          }
        } else {
          //android
          if (!imei.equals("") && !imei.matches("[0]{7,}")) {
            //imei有效
            statusCode = CommonsThirdData.checkAccountIsMatched(gameAccount, stmt)
            if (statusCode) {
              //imei有效 -->执行匹配动作
              matched = CommonsThirdData.androidRegiMatchActiveByImei(imei, regiDate29dayBefore, regiDate, gameId, conn)
              val adv_name = matched._1
              if (adv_name == 0) {
                //imei未匹配到 -->如果androidID有效，再用androidID进行匹配
                if (!androidID.equals("")) {
                  matched = CommonsThirdData.androidRegiMatchActiveByAndroidID(androidID, regiDate29dayBefore, regiDate, gameId, conn)
                }
              }
            }
          } else {
            //imei无效  -->如果androidID有效，直接用androidID进行匹配
            if (!androidID.equals("")) {
              statusCode = CommonsThirdData.checkAccountIsMatched(gameAccount, stmt)
              if (statusCode) {
                matched = CommonsThirdData.androidRegiMatchActiveByAndroidID(androidID, regiDate29dayBefore, regiDate, gameId, conn)
              }
            }
          }
        }

        //matched._1取出的是advName
        //从激活明细表中判断advName是广告媒介还是自然量
        //如果是广告媒介，ios和android的pkgCode都从激活明细表中取,
        val adv_name = matched._1
        //从激活明细表中判断advName是广告媒介还是自然量
        //默认返回的是0，如果注册匹配到激活，返回值不等于0
        if (adv_name != 0) {
          //这个pkgCode是从激活数据中拿过来的
          val pkgCode = matched._5
          //如果要做注册上报，在这里就要更新激活明细表中的match字段<<<<<<<<<<<<<<<<<<<<<<<
          //广点通,今日头条注册匹配激活上报
          //---------------------------->>>离线这里只统计数据，不做激活和注册上报<<<--------------------------
          //          if (adv_name == 6 || adv_name == 3) {
          //            //一个设备,现在做的是:注册每次都上报，订单每次都上报
          //            //更新激活明细表中的matched_regi,regi_time,regi_time不能存入注册日志中的regi_time,要存入now(),获取更新数据的时间去存入
          //            //现在regi_time字段存入的是该设备注册最后一个帐号的注册时间
          //            CommonsThirdData.updateRegiMatchedActive(pkgCode, regiTime, imei, conn)
          //          }
          //          val redisValue: Array[String] = CommonsThirdData.getRedisValue(gameId, pkgCode, regiDate, jedis, connFx)
          //缓存pkgCode
          //          CommonsThirdData.cachePkgCode(pkgCode, regiDate, jedis6)

          val group_id = "0"
          val medium_account = ""
          val head_people = ""
          val idea_id = matched._2
          val first_level = matched._3
          val second_level = matched._4
          //渠道商id
          val channel_main_id = matched._6
          //渠道商下渠道名
          val channel_name = matched._7
          //激活时间
          val activeTime = matched._8
          val activeDate = activeTime.substring(0, 10)
          val topic = adv_name match {
            case 0 => "pyw"
            case 1 => "momo"
            case 2 => "baidu"
            case 3 => "jinritoutiao"
            case 4 => "aiqiyi"
            case 5 => "uc"
            case 6 => "guangdiantong"
            case 7 => "uc_appstore"
            case 8 => "youku"
            case 9 => "inmobi"
            case 10 => "fenghuangxinwen"
            case 11 => "baofengyingyin"
            case 12 => "zhiyingxiao"
            case 13 => "wangyixinwen"
            case 14 => "xinlangfensitong"
            case 15 => "sougou"
            case 16 => "shenma"
            case 17 => "dongqiudi"
            case -1 => channel_name
          }
          //android的imei如果为空或者全是0，需要转换为"no_imei"再存储
          if (imei.equals("") || imei.matches("[0]{7,}")) {
            imei = "no_imei"
          }

          //分包备注
          val remark = ""
          //          if (pkgCode.contains("M")) {
          //            remark = CommonsThirdData.getMediumRemark(pkgCode, connFx)
          //          } else if (pkgCode.contains("Q")) {
          //            remark = CommonsThirdData.getChannelRemark(pkgCode, connFx)
          //          }

          //注册帐号数
          val regiNum = 1
          //注册设备数: 按设备去重
          val regiDevNum = CommonsThirdData.isRegiDev(regiDate, gameId, imei, topic, jedis6)
          //新增注册设备数
          var newRegiDevNum = 0
          if (regiDevNum == 1 && regiDate.equals(activeDate)) {
            newRegiDevNum = 1
          }

          //写入广告监测平台注册明细
          ThirdDataDao.insertRegiDetailOffLine(regiTime, imei, pkgCode, adv_name, gameId, osInt, gameAccount, channel_main_id, channel_name, androidID, conn)
          //注册统计
          ThirdDataDao.insertRegiStat(regiDate, gameId, group_id, pkgCode, head_people, medium_account, adv_name, idea_id, first_level, second_level, regiNum, regiDevNum, newRegiDevNum, channel_main_id, channel_name, remark, conn)

        }

      })
      pool.returnBrokenResource(jedis)
      pool.returnResource(jedis6)
      pool.destroy()
      stmt.close()
      conn.close()
      //      connFx.close()
    })
    rdd.unpersist()
  }

  /**
    * 订单匹配注册
    * 实时任务中，redis的去重时间是48H
    * 如果想要用永久去重，用mysql查
    *
    * @param rdd
    */
  def orderMatchRegi(rdd: RDD[(String, String, String, String, Float, String)]) = {
    rdd.foreachPartition(part => {
      val pool: JedisPool = JedisUtil.getJedisPool;
      val jedis = pool.getResource
      val jedis6 = pool.getResource
      jedis6.select(6)
      val conn = JdbcUtil.getConn()
      val stmt = conn.createStatement()
      val connFx = JdbcUtil.getXiaopeng2FXConn()
      part.foreach(line => {
        val gameId = line._4.toInt
        var imei = CommonsThirdData.getImei(line._6)
        val gameAccount = line._1
        val orderID = line._2
        val orderTime = line._3
        val orderDate = orderTime.substring(0, 10)
        //过滤重复日志
        val select_Sql = "select order_id from bi_ad_order_o_detail where order_id = '" + orderID + "' and game_account = '" + gameAccount + "'"
        val resultSet = stmt.executeQuery(select_Sql)
        if (!resultSet.next()) {

          //accountInfo = (pkgId, regiTime, adName, os, ideaId, firstLevel, secondLevel,imei)
          //订单通过帐号关联注册
          val accountInfo = CommonsThirdData.getMatchedAccountInfo(gameAccount, conn)
          val pkgCode = accountInfo._1
          val regiDate = accountInfo._2.substring(0, 10)
          val advName = accountInfo._3
          //advName!=0 就是订单匹配到了注册
          if (advName != 0) {
            //广点通订单上报
            //---------------------->>>离线这里不做订单上报<<<----------------------
            //            if (advName == 6 || advName == 3) {
            //              //不能只根据imei和matched来匹配
            //              //订单匹配到注册，更新bi_ad_active_o_detail表matched_order,ordertime
            //              CommonsThirdData.updateOrderMatchActive(pkgCode, orderTime, imei, conn)
            //            }
            //缓存pkgCode
            //            CommonsThirdData.cachePkgCode(pkgCode, orderDate, jedis6)

            //            val redisValue: Array[String] = CommonsThirdData.getRedisValue(gameId, pkgCode, orderDate, jedis, connFx)
            val medium = advName
            //发行信息
            val group_id = "0"
            val medium_account = ""
            val head_people = ""

            val os = accountInfo._4
            val idea_id = accountInfo._5
            val first_level = accountInfo._6
            val second_level = accountInfo._7
            val channel_main_id = accountInfo._8
            val channel_name = accountInfo._9

            //android的imei如果为空或者全是0，需要转换为"no_imei"再存储
            if (imei.equals("") || imei.matches("[0]{7,}")) {
              imei = "no_imei"
            }

            //分包备注
            var remark = ""
            //            if (pkgCode.contains("M")) {
            //              remark = CommonsThirdData.getMediumRemark(pkgCode, connFx)
            //            } else if (pkgCode.contains("Q")) {
            //              remark = CommonsThirdData.getChannelRemark(pkgCode, connFx)
            //            }

            //是否新增充值帐号 ： 当天注册的帐号是否在当天充值
            val isNewPayAcc = if (regiDate.equals(orderDate)) 1 else 0

            //充值金额 单位为分
            val payPrice = line._5.toFloat * 100
            //充值帐号数 ： 同一天内按帐号去重
            val payAccs = CommonsThirdData.isExistStatPayAcc(orderDate, gameAccount, jedis6)
            //新增充值金额 ： 新增充值帐号的充值金额
            val newPayPrice = if (isNewPayAcc == 1) line._5.toFloat * 100 else 0
            //新增充值帐号 ： 是新增充值帐号，并且同一天内按帐号去重
            val newPayAccs = if (isNewPayAcc == 1) CommonsThirdData.isExistStatNewPayAcc(orderDate, gameAccount, jedis6) else 0


            //消费明细
            ThirdDataDao.insertOrderDetailOffLine(orderID, orderTime, imei, pkgCode, medium, gameId, os, gameAccount, payPrice, channel_main_id, channel_name, conn)
            //消费统计
            ThirdDataDao.insertOrderStat(orderDate, gameId, group_id, pkgCode, head_people, medium_account, medium, idea_id, first_level,
              second_level, payPrice, payAccs, newPayPrice, newPayAccs, channel_main_id, channel_name, remark, conn)

          }
        }
      }
      )
      pool.returnBrokenResource(jedis)
      pool.returnResource(jedis6)
      pool.destroy()
      stmt.close()
      conn.close()
      connFx.close()
    })

  }

  def foreachClickStatsDF(clickStatsDF: DataFrame) = {
    clickStatsDF.foreachPartition(iter => {
      //jedis发行库
      val jedis = JedisUtil.getJedisPool.getResource
      val conn = JdbcUtil.getConn()
      val connFx = JdbcUtil.getXiaopeng2FXConn()
      //离线数据，把一整天当成一批，后面不能用累加
      val insert_click_stats_sql = "insert into bi_ad_channel_stats" +
        "(publish_date,game_id,group_id,pkg_id,head_people,medium_account,medium,idea_id,first_level,second_level,click_num,click_dev_num,remark)" +
        " values(?,?,?,?,?,?,?,?,?,?,?,?,?) " +
        " on duplicate key update click_num=values(click_num),click_dev_num=values(click_dev_num),head_people=values(head_people),medium_account=values(medium_account)"
      val pstat = conn.prepareStatement(insert_click_stats_sql)

      iter.foreach(line => {
        val clickDate = line.getAs[Date](0).toString
        val game_id = line.getAs[Int](1)
        val pkg_id = line.getAs[String](2)
        val adv_name = line.getAs[Int](3)
        val click_num = line.getAs[Long](4)
        val click_dev_num = line.getAs[Long](5)
        val aid = ""
        val uid = ""
        val pid = ""
        //取出发行信息
        val redisValue: Array[String] = CommonsThirdData.getRedisValue(game_id, pkg_id, clickDate, jedis, connFx)
        val group_id = redisValue.apply(6)
        val medium_account = redisValue(2)
        val head_people = redisValue(5)

        //分包备注
        val remark = ""

        //game_id
        //        if (pkg_id.contains("M")) {
        //          remark = CommonsThirdData.getMediumRemark(pkg_id, connFx)
        //        } else if (pkg_id.contains("Q")) {
        //          remark = CommonsThirdData.getChannelRemark(pkg_id, connFx)
        //        }


        //insert
        pstat.setObject(1, clickDate)
        pstat.setObject(2, game_id)
        pstat.setObject(3, group_id)
        pstat.setObject(4, pkg_id)
        pstat.setObject(5, head_people)
        pstat.setObject(6, medium_account)
        pstat.setObject(7, adv_name)
        pstat.setObject(8, aid)
        pstat.setObject(9, uid)
        pstat.setObject(10, pid)
        pstat.setObject(11, click_num)
        pstat.setObject(12, click_dev_num)
        pstat.setObject(13, remark)

        pstat.executeUpdate()
      })
      //关闭数据库链接
      pstat.close()
      connFx.close()
      conn.close()
    })

  }

  /**
    * 干了三件事：
    * 1：过滤掉json解析不合格的数据，pkg_id不合格的数据
    * 2：将tuple14转换成Row11
    * 3：传入的数据是 2 天的数据，要从中过滤出 1 天的数据
    *
    * @param thirddata
    * @return
    */
  def convertFormatThirdDataRDD(thirddata: RDD[(String, String, String, String, String, Int, String, String, String, String, String, String, String, String)], startday: String): RDD[Row] = {
    //把全天的数据当成一批处理
    val click_data = thirddata.filter(line => {
      val clickTime = line._4
      var isVad = false
      //过滤掉json解析错误的数据,
      if (!clickTime.equals("0")) {
        val clickDate = clickTime.substring(0, 10)
        if (clickDate.equals(startday)) {
          //过滤掉分包不合发的数据
          val pkg_id = line._1
          val regx = "\\d{4,5}[MQ]\\d{4,7}[a-z]?"
          if (pkg_id.matches(regx)) {
            isVad = true
          }
        }
      }
      isVad
    }).map(line => {
      //这里取字段的顺序和实时程序一样
      val pkg_id = line._1
      val os = line._2
      val osInt = if (os.toLowerCase.equals("ios")) 2 else 1
      val imei = line._3
      val clickTime = line._4
      val clickDate = clickTime.substring(0, 10)
      var game_id = 0
      if (pkg_id.contains("M")) {
        game_id = pkg_id.substring(0, pkg_id.indexOf("M")).toInt
      } else if (pkg_id.contains("Q")) {
        game_id = pkg_id.substring(0, pkg_id.indexOf("Q")).toInt
      }
      val callback = line._5
      var advName = line._6.toInt
      val aid = line._7
      val uid = line._8
      val pid = line._9
      //10.2.3.116
      val ip = line._10
      //iPhone 10.3.3
      val ua = line._11
      //设备类型
      var devType = ""
      //系统类型
      var sysVersion = ""
      if (!ua.equals("")) {
        devType = ua.split(" ", -1)(0)
        sysVersion = ua.split(" ", -1)(1)
      }
      //渠道商id
      var channel_main_id = 0
      if (!line._12.equals("")) {
        channel_main_id = line._12.toInt
      }
      //渠道商下的渠道名称
      val channel_name = line._13
      //安卓ID
      val androidID = line._14
      //这个方法直接从实时任务那里拷贝
      val imei_md5_upper = CommonsThirdData.imeiToMd5Upper(advName, imei, osInt)

      if (advName == -1 && pkg_id.contains("M")) {
        advName = channel_main_id
      }


      //共17个字段
      Row(pkg_id, game_id, imei, imei_md5_upper, os, clickTime, callback, advName, aid, uid, pid, ip, devType, sysVersion, channel_main_id, channel_name, androidID)
    })
    return click_data
  }

  /**
    * 点击明细表
    * 注意的地方：只插入更新点击明细表，不插入点击统计表，注释掉插入统计表的方法
    *
    * @param rdd
    */
  def clickDetail(rdd: RDD[(String, String, String, String, String, Int, String, String, String, String, String, String, String, String)], startday: String) = {
    rdd.foreachPartition(part => {
      val conn = JdbcUtil.getConn()
      val pool: JedisPool = JedisUtil.getJedisPool;
      //取了2个redis对象，分别选择库1和库6
      val jedis = pool.getResource
      val jedis6 = pool.getResource
      jedis6.select(6)
      //      val connFx = JdbcUtil.getXiaopeng2FXConn()
      part.foreach(line => {
        //(3802M24001,android,,2017-11-07 14:10:05,aaa,8,,,,127.0.0.1,iPhone 10.3.3)
        //println("原始日志: " + line)
        //如果原始广告日志json解析异常，clickTime会返回"0"
        val clickTime = line._4
        //这种clickTime等于0的数据就不处理了
        if (!clickTime.equals("0")) {
          //--->>>离线处理这里,取得是2天的数据，用startday过滤出1天的数据
          val clickDate = clickTime.substring(0, 10)
          if (clickDate.equals(startday)) {
            // <<<---

            //4151M19006/5235Q12003
            val pkg_id = line._1
            //pkg_id正则表达式
            val regx = "\\d{4,5}[MQ]\\d{4,7}[a-z]?"
            //判断pkg_id是否有效，如果无效，则日志不进行处理
            if (pkg_id.matches(regx)) {
              //点击明细表中存入的os字段值为ios/android
              val os = line._2
              //1:andorid 2:ios
              val osInt = if (os.toLowerCase.equals("ios")) 2 else 1
              //这个imei是从点击日志中取出
              var imei = line._3
              //媒介id : 1,2,3,4,5,6,7,8,9......
              //当advName= -1 时，说明 激活匹配点击的模式为 IP + UA
              var advName = line._6.toInt
              //2017-09-13
              val clickDate = clickTime.substring(0, 10)
              //4151
              var game_id = 0
              if (pkg_id.contains("M")) {
                game_id = pkg_id.substring(0, pkg_id.indexOf("M")).toInt
              } else if (pkg_id.contains("Q")) {
                game_id = pkg_id.substring(0, pkg_id.indexOf("Q")).toInt
              }
              val callback = line._5
              //parent_game_id,os,medium_account,promotion_channel,promotion_mode,head_people,groupid
              //              val redisValue: Array[String] = CommonsThirdData.getRedisValue(game_id, pkg_id, clickDate, jedis, connFx)
              //              //从redis中取出发行平台信息
              //              val group_id = redisValue.apply(6)
              //              //媒介帐号
              //              val medium_account = redisValue(2)
              //              //负责人
              //              val head_people = redisValue(5)
              //              //创意id
              //              val idea_id = line._7
              //              //创意第一层
              //              val first_level = line._8
              //              //创意第二层
              //              val second_level = line._9
              //10.2.3.116
              val ip = line._10
              //iPhone 10.3.3
              val ua = line._11
              //设备类型
              var devType = ""
              //系统类型
              var sysVersion = ""
              if (!ua.equals("") && ua.contains(" ")) {
                if (ua.split(" ").length == 2) {
                  devType = ua.split(" ", -1)(0)
                  sysVersion = ua.split(" ", -1)(1)
                }
              }
              //渠道商id
              var channel_main_id = 0
              if (!line._12.equals("")) {
                channel_main_id = line._12.toInt
              }
              //渠道商下的渠道名称
              val channel_name = line._13
              //安卓ID  只有android设备才有androidid
              var androidID = ""
              if (osInt == 1) {
                androidID = line._14
              }
              //当advName==-1时，再判断这个pkg_id是包含M的，还是包含Q的，用不同的方式处理,如果是包含M的，channel_main_id就是媒介id
              if (advName == -1 && pkg_id.contains("M")) {
                advName = channel_main_id
              }
              //topic用于redis去除重复设备 --新增加一个媒介，这里就要增加新的匹配项
              val topic = advName match {
                case 0 => "pyw"
                case 1 => "momo"
                case 2 => "baidu"
                case 3 => "jinritoutiao"
                case 5 => "uc"
                case 4 => "aiqiyi"
                case 6 => "guangdiantong"
                case 7 => "uc_appstore"
                case 8 => "youku"
                case 9 => "inmobi"
                case 10 => "fenghuangxinwen"
                case 11 => "baofengyingyin"
                case 12 => "zhiyingxiao"
                case 13 => "wangyixinwen"
                case 14 => "xinlangfensitong"
                case 15 => "sougou"
                case 16 => "shenma"
                case 17 => "dongqiudi"
                case -1 => channel_name
              }

              //判断哪些类型的数据要按渠道数据处理,纯UA+IP
              val idCache = ArrayBuffer[Int](-1, 10, 11, 12, 13, 14, 15, 16)
              val res = idCache.contains(advName)
              //点击数:有点击日志一条，就计算一个点击数
              val click_num = 1
              //点击设备数：如果 imei,ua,ip都为空，那这个点击不计算设备数
              var click_dev_num = 0
              //当advName == -1时，说明这条数据是渠道来的
              if (res) { //-->渠道数据
                //这里处理渠道数据的UA+IP统计，所以UA和IP不能为空
                if (!ip.equals("") && !ua.equals("")) {
                  //先判断这个设备是否有点击记录 0：没有点击数据 1：点击未激活 2：点击并且激活
                  val clickStatus = CommonsThirdData.checkChannelClickDataStatus(game_id, ip, devType, sysVersion, conn)
                  if (clickStatus == 0) {
                    imei = "channel_no_imei"
                    val imei_md5_upper = "channel_no_imei_md5_upper"
                    ThirdDataDao.insertChannelClickDetail(pkg_id, imei, imei_md5_upper, clickTime, os, callback, game_id.toString, advName, ip, devType, sysVersion, channel_main_id, channel_name, conn)
                  } else if (clickStatus == 1) {
                    ThirdDataDao.updateChannelDataClickDetail(pkg_id, game_id, clickTime, ip, devType, sysVersion, callback, conn)
                  } else if (clickStatus == 2) {
                    //ua+ip的方式，因为已经在激活明细表做了game_id + imei 去重的处理，所以不会出现一个设备多次匹配的问题
                    //当一个ip+ua匹配完之后，需要继续存入点击数据，如果有不同的设备激活，需要做继续匹配
                    imei = "channel_no_imei"
                    val imei_md5_upper = "channel_no_imei_md5_upper"
                    ThirdDataDao.insertChannelClickDetail(pkg_id, imei, imei_md5_upper, clickTime, os, callback, game_id.toString, advName, ip, devType, sysVersion, channel_main_id, channel_name, conn)
                  }
                  click_dev_num = CommonsThirdData.isTheChannelDeviceHasClicked(game_id, topic, clickDate, ip, devType, sysVersion, conn, jedis6)
                }
              } else { //-->媒介数据
                if (!imei.equals("") && !imei.contains("00000000") && !imei.toUpperCase.equals("NULL")) {
                  //imei有效
                  val clickStatus = CommonsThirdData.checkMediumClickDataStatusByImei(imei, game_id, conn)
                  if (clickStatus == 0) {
                    //增加一个字段 imei_md5_upper，所有媒介的imei_md5_upper都统一处理为：imei->md5加密->大写
                    /**
                      * 新增加媒介，都要修改imeiToMd5Upper的代码,根据媒介的不同，添加不同的加密方式
                      * 1:momo
                      * 2:baidu
                      * 3:jinritoutiao
                      * 4:aiqiyi
                      * 5:uc
                      * 6:guangdiantong
                      * 7:UC应用商店
                      * 8:优酷
                      * 9:inmobi
                      * 17:dongqiudi
                      * -1:channel_name
                      */
                    val imei_md5_upper = CommonsThirdData.imeiToMd5Upper(advName, imei, osInt)
                    //渠道设备有效，存入数据也要将imei,ip,devTyoe,sysVersion全部存入
                    ThirdDataDao.insertMediumClickDetail(pkg_id, imei, imei_md5_upper, clickTime, os, callback, game_id.toString, advName, ip, devType, sysVersion, androidID, conn)
                  } else if (clickStatus == 1) {
                    //如果imei的点击已经存在，按游戏去重
                    ThirdDataDao.updateMediumClickByImei(pkg_id, game_id, imei, clickTime, callback, conn)
                  }
                } else {
                  //imei无效
                  //分别用androidID,ip,ua 更新点击明细数据
                  //androidID : [今日头条,懂球第]
                  if (!androidID.equals("") && !androidID.equals("__ANDROIDID__")) {
                    val clickStatus = CommonsThirdData.checkMediumClickDataStatusByAndroidID(game_id, androidID, conn)
                    if (clickStatus == 0) {
                      imei = "medium_no_imei"
                      val imei_md5_upper = "medium_no_imei_md5_upper"
                      ThirdDataDao.insertMediumClickDetail(pkg_id, imei, imei_md5_upper, clickTime, os, callback, game_id.toString, advName, ip, devType, sysVersion, androidID, conn)
                    } else if (clickStatus == 1) {
                      ThirdDataDao.updateMediumClickByAndroidID(pkg_id, game_id, androidID, clickTime, callback, conn)
                    }
                  } else {
                    if (!ip.equals("")) {
                      //ip有效
                      if (!ua.equals("")) {
                        //ua有效  --> IP+UA 检查点击
                        val clickStatus = CommonsThirdData.checkMediumClickDataStatusByIpAndUa(game_id, ip, devType, sysVersion, conn)
                        if (clickStatus == 0) {
                          imei = "medium_no_imei"
                          androidID = "medium_no_androidID"
                          val imei_md5_upper = "medium_no_imei_md5_upper"
                          ThirdDataDao.insertMediumClickDetail(pkg_id, imei, imei_md5_upper, clickTime, os, callback, game_id.toString, advName, ip, devType, sysVersion, androidID, conn)
                        } else if (clickStatus == 1) {
                          ThirdDataDao.updateMediumClickByIpAndUa(pkg_id, game_id, ip, devType, sysVersion, clickTime, callback, conn)
                        } else if (clickStatus == 2) {
                          imei = "medium_no_imei"
                          androidID = "medium_no_androidID"
                          val imei_md5_upper = "medium_no_imei_md5_upper"
                          ThirdDataDao.insertMediumClickDetail(pkg_id, imei, imei_md5_upper, clickTime, os, callback, game_id.toString, advName, ip, devType, sysVersion, androidID, conn)
                        }
                      } else {
                        //ua无效  --> IP 检查点击
                        val clickStatus = CommonsThirdData.checkMediumClickDataStatusByIp(game_id, ip, conn)
                        if (clickStatus == 0) {
                          imei = "medium_no_imei"
                          androidID = "medium_no_androidID"
                          val imei_md5_upper = "medium_no_imei_md5_upper"
                          ThirdDataDao.insertMediumClickDetail(pkg_id, imei, imei_md5_upper, clickTime, os, callback, game_id.toString, advName, ip, devType, sysVersion, androidID, conn)
                        } else if (clickStatus == 1) {
                          ThirdDataDao.updateMediumClickByIp(pkg_id, game_id, ip, clickTime, callback, conn)
                        } else if (clickStatus == 2) {
                          imei = "medium_no_imei"
                          androidID = "medium_no_androidID"
                          val imei_md5_upper = "medium_no_imei_md5_upper"
                          ThirdDataDao.insertMediumClickDetail(pkg_id, imei, imei_md5_upper, clickTime, os, callback, game_id.toString, advName, ip, devType, sysVersion, androidID, conn)
                        }
                      }
                    }
                  }
                }
                //click_dev_num 按游戏按天去重：一天内一个imei在一个游戏内，点击只计算一次
                click_dev_num = CommonsThirdData.isTheMediumDeviceHasClicked(clickDate, game_id, imei, topic, ip, devType, sysVersion, androidID, jedis6)
              }
              //插入数据到统计表
              //如果媒介数据，advName存入的是媒介编号，如果是渠道数据，advName存入的是-1
              //              ThirdDataDao.insertClickStat(clickDate, game_id, group_id, pkg_id, head_people, medium_account, advName, channel_main_id, channel_name, idea_id, first_level, second_level, click_num, click_dev_num, conn)
            }
          }
        }
      })
      pool.returnBrokenResource(jedis)
      pool.returnBrokenResource(jedis6)
      pool.destroy()
      conn.close()
      //      connFx.close()
    })

  }


}
