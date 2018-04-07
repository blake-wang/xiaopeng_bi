package com.xiaopeng.bi.utils.action

import java.text.SimpleDateFormat
import java.util.Date

import com.xiaopeng.bi.utils._
import com.xiaopeng.bi.utils.dao.{ThirdDataDao1}
import net.sf.json.JSONObject
import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import redis.clients.jedis.JedisPool

/**
  * Created by Administrator on 2017/9/1.
  */
object ThirdDataActs1 {
  val logger = Logger.getLogger(this.getClass)

  /**
    * 第三方数据click处理
    *
    * @param adData
    */
  def theThirdData(adData: DStream[(String, String, String, String, String, Int, String, String, String, String, String, String, String, String)]) = {
    adData.foreachRDD(rdd => {
      rdd.cache()
      rdd.foreachPartition(part => {
        val conn = JdbcUtil.getConn()
        val pool: JedisPool = JedisUtil.getJedisPool;
        //取了2个redis对象，分别选择库1和库6
        val jedis = pool.getResource
        val jedis6 = pool.getResource
        jedis6.select(6)
        val connFx = JdbcUtil.getXiaopeng2FXConn()
        part.foreach(line => {
          //(3802M24001,android,,2017-11-07 14:10:05,aaa,8,,,,127.0.0.1,iPhone 10.3.3)
          //println("原始日志: " + line)
          //如果原始广告日志json解析异常，clickTime会返回"0"
          val clickTime = line._4
          //这种clickTime等于0的数据就不处理了
          if (!clickTime.equals("0")) {
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
              val advName = line._6.toInt
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
              val redisValue: Array[String] = CommonsThirdData.getRedisValue(game_id, pkg_id, clickDate, jedis, connFx)
              //从redis中取出发行平台信息
              val group_id = redisValue.apply(6)
              //媒介帐号
              val medium_account = redisValue(2)
              //负责人
              val head_people = redisValue(5)
              //创意id
              val idea_id = line._7
              //创意第一层
              val first_level = line._8
              //创意第二层
              val second_level = line._9
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
                case 17 => "dongqiudi"
                case -1 => channel_name
              }
              //点击数:有点击日志一条，就计算一个点击数
              val click_num = 1
              //点击设备数：如果 imei,ua,ip都为空，那这个点击不计算设备数
              var click_dev_num = 0
              //当advName == -1时，说明这条数据是渠道来的
              if (advName == -1) { //-->渠道数据
                //这里处理渠道数据的UA+IP统计，所以UA和IP不能为空
                if (!ip.equals("") && !ua.equals("")) {
                  //先判断这个设备是否有点击记录 0：没有点击数据 1：点击未激活 2：点击并且激活
                  val clickStatus = CommonsThirdData.checkChannelClickDataStatus(game_id, ip, devType, sysVersion, conn)
                  if (clickStatus == 0) {
                    imei = "channel_no_imei"
                    val imei_md5_upper = "channel_no_imei_md5_upper"
                    ThirdDataDao1.insertChannelClickDetail(pkg_id, imei, imei_md5_upper, clickTime, os, callback, game_id.toString, advName, ip, devType, sysVersion, channel_main_id, channel_name, conn)
                  } else if (clickStatus == 1) {
                    ThirdDataDao1.updateChannelDataClickDetail(pkg_id, game_id, clickTime, ip, devType, sysVersion, callback, conn)
                  } else if (clickStatus == 2) {
                    //ua+ip的方式，因为已经在激活明细表做了game_id + imei 去重的处理，所以不会出现一个设备多次匹配的问题
                    //当一个ip+ua匹配完之后，需要继续存入点击数据，如果有不同的设备激活，需要做继续匹配
                    imei = "channel_no_imei"
                    val imei_md5_upper = "channel_no_imei_md5_upper"
                    ThirdDataDao1.insertChannelClickDetail(pkg_id, imei, imei_md5_upper, clickTime, os, callback, game_id.toString, advName, ip, devType, sysVersion, channel_main_id, channel_name, conn)
                  }
                  click_dev_num = CommonsThirdData.isTheChannelDeviceHasClicked(game_id, topic, clickDate, ip, devType, sysVersion, conn, jedis6)
                }
              } else { //-->媒介数据
                if (!imei.equals("") && !imei.equals("00000000-0000-0000-0000-000000000000")) {
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
                    ThirdDataDao1.insertMediumClickDetail(pkg_id, imei, imei_md5_upper, clickTime, os, callback, game_id.toString, advName, ip, devType, sysVersion, androidID, conn)
                  } else if (clickStatus == 1) {
                    //如果imei的点击已经存在，按游戏去重
                    ThirdDataDao1.updateMediumClickByImei(pkg_id, game_id, imei, clickTime, callback, conn)
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
                      ThirdDataDao1.insertMediumClickDetail(pkg_id, imei, imei_md5_upper, clickTime, os, callback, game_id.toString, advName, ip, devType, sysVersion, androidID, conn)
                    } else if (clickStatus == 1) {
                      ThirdDataDao1.updateMediumClickByAndroidID(pkg_id, game_id, androidID, clickTime, callback, conn)
                    }
                  } else {
                    if (!ip.equals("") && !ua.equals("")) {
                      val clickStatus = CommonsThirdData.checkMediumClickDataStatusByIpAndUa(game_id, ip, devType, sysVersion, conn)
                      if (clickStatus == 0) {
                        imei = "medium_no_imei"
                        androidID = "medium_no_androidID"
                        val imei_md5_upper = "medium_no_imei_md5_upper"
                        ThirdDataDao1.insertMediumClickDetail(pkg_id, imei, imei_md5_upper, clickTime, os, callback, game_id.toString, advName, ip, devType, sysVersion, androidID, conn)
                      } else if (clickStatus == 1) {
                        ThirdDataDao1.updateMediumClickByIpAndUa(pkg_id, game_id, ip, devType, sysVersion, clickTime, callback, conn)
                      } else if (clickStatus == 2) {
                        imei = "medium_no_imei"
                        androidID = "medium_no_androidID"
                        val imei_md5_upper = "medium_no_imei_md5_upper"
                        ThirdDataDao1.insertMediumClickDetail(pkg_id, imei, imei_md5_upper, clickTime, os, callback, game_id.toString, advName, ip, devType, sysVersion, androidID, conn)
                      }
                    }
                  }
                }
                //click_dev_num 按游戏按天去重：一天内一个imei在一个游戏内，点击只计算一次
                click_dev_num = CommonsThirdData.isTheMediumDeviceHasClicked(clickDate, game_id, imei, topic, ip, devType, sysVersion, androidID, jedis6)
              }
              //插入数据到统计表
              //如果媒介数据，advName存入的是媒介编号，如果是渠道数据，advName存入的是-1
              ThirdDataDao1.insertClickStat(clickDate, game_id, group_id, pkg_id, head_people, medium_account, advName, channel_main_id, channel_name, idea_id, first_level, second_level, click_num, click_dev_num, conn)
            }
          }
        })
        pool.returnBrokenResource(jedis)
        pool.returnBrokenResource(jedis6)
        pool.destroy()
        conn.close()
        connFx.close()
      })
      rdd.unpersist()
    })
  }

  /**
    * 激活匹配点击
    *
    * @param dataActive
    */
  def activeMatchClick(dataActive: DStream[(String, String, String, String, String, String, String, String, String)]) = {
    dataActive.foreachRDD(rdd => {
      rdd.cache()
      rdd.foreachPartition(iter => {
        val pool: JedisPool = JedisUtil.getJedisPool;
        val jedis = pool.getResource
        val jedis6 = pool.getResource
        jedis6.select(6)
        val conn = JdbcUtil.getConn()
        val connFx = JdbcUtil.getXiaopeng2FXConn()
        val stmt = conn.createStatement()
        iter.foreach(line => {
          //(3802,21,jlcy_lj_1,5896EEA3ECCF40FAA33AF1201DA8B3EF,2017-11-09 10:30:56,IOS,127.0.0.1,iPhone 6,10.3.3)
          val game_id = line._1.toInt
          val activelog_imei = line._4
          //221132093280616&64110abc12e796d5&f8:2f:48:a1:6d:20
          //安卓设备取最前面部分(15位数字)，苹果设备取完(32位英文数字大写)
          var imei = CommonsThirdData.getImei(activelog_imei)
          //安卓设备：安卓ID取中间16位 / IOS设备：没有androidID，为""
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

          val activeTime = line._5
          //2017-09-09
          val activeDate = activeTime.substring(0, 10)
          //激活匹配点击,7天内有效, 往前推7天,如果是7天内，这里传入的参数就因该是-6
          val activeDate6dayBefore = CommonsThirdData.getDateBeforeParams(activeDate, -6)
          //jg, pkg, imei, idfa.replace("-", ""), adv_name, ideaId, firstLevel, secondLevel,channel_main_id,channel_name
          var matched = Tuple10(0, "", "", "", 0, "", "", "", 0, "");

          //---------------通过imei和androidID分别判断该条激活日志是否已经匹配--------------1226新增
          //statusCode是否进行激活匹配点击的标记码,默认是false -> 不进行匹配
          var statusCode = false
          /**
            * ios设备做激活匹配：关键信息有效性检查和重复性检查
            * 1、若imei无效则跳过IP+UA
            * 2、若idfa匹配不了则用IP+UA匹配
            */
          if (osInt == 2)
          {
            //IOS设备
            if (!imei.equals("") && !imei.matches("[0]{7,}")) { //激活日志中imei字段是否有效，无效则直接跳过ip+ua
              statusCode= ThirdDataDao1.checkImeiIsMatched(game_id,imei,conn,stmt);  //是否已经匹配成功
              if(!statusCode)
                {
                  val imeiWithLine = CommonsThirdData.imeiPlusDash(imei.toUpperCase())
                  val imeiWithLine_md5_upper = MD5Util.md5(imeiWithLine).toUpperCase
                  //2:imei不加横杠md5加密大写
                  val imei_md5_upper = MD5Util.md5(imei).toUpperCase
                  //ios设备的匹配：imei + game_id
                  matched = ThirdDataDao1.iosMatchClickByImei(imei, imeiWithLine_md5_upper, imei_md5_upper, game_id, activeDate6dayBefore, activeDate, conn)

                  //匹配第二步：如果imei没有匹配到，如果IP和UA有效，再用UA+IP匹配一次

                  if (matched._1 == 0) {
                    if (!ip.equals("")) {
                      //ip有效
                      if (!devType.equals("")) {
                        // devType有效  --> 执行 game_id + ip + devType + sysVersion 匹配
                        matched = ThirdDataDao1.iosMatchClickByIPAndUA(game_id, activeDate6dayBefore, activeDate, ip, devType, sysVersion, conn)
                      } else {
                        // devType无效  --> 执行 game_id + ip 匹配
                        matched = ThirdDataDao1.iosMatchClickByIP(game_id, activeDate6dayBefore, activeDate, ip, conn)
                      }
                    }
                  }
                }
            } else{
              logger.info("IOS设备无效，直接终止继续匹配")
            }

            /**
            * Android设备做激活匹配：关键信息有效性检查和重复性检查,
            * 1、若imei无效则进行Androidid匹配
            * 2、若idfa匹配不了则进行Androidid匹配
            */
          } else {
            //android设备
            if (!imei.equals("") && !imei.matches("[0]{7,}")) //设备有效性检查，无效则调到Androidid
            {
              statusCode= ThirdDataDao1.checkImeiIsMatched(game_id,imei,conn,stmt);  //是否已经匹配成功
              val imei_md5_upper = MD5Util.md5(imei).toUpperCase
              if(!statusCode)  //没有历史匹配成功记录
                {
                  matched = ThirdDataDao1.androidMatchClickByImei(imei_md5_upper, game_id, activeDate6dayBefore, activeDate, conn)
                  if (matched._1 == 0) {       //若通过I没匹配到则通过Androidid
                    if (!androidID.equals("")) {  //androidID有效性
                      statusCode= ThirdDataDao1.checkAndroidIsMatched(game_id,imei,conn,stmt);  //是否已经匹配成功
                      if(!statusCode)
                        {
                          matched = ThirdDataDao1.androidMatchClickByAndroidID(androidID, game_id, activeDate6dayBefore, activeDate, conn)
                        }

                    }
                  }
                }
            }
            else
            {
              logger.info("Android设备imei无效，跳过imei号匹配，继续Androidid匹配")
              //androidID有效
              if(!androidID.equals(""))
               {
                 statusCode= ThirdDataDao1.checkAndroidIsMatched(game_id,imei,conn,stmt);  //是否已经匹配成功
                 if(!statusCode)
                 {
                   matched = ThirdDataDao1.androidMatchClickByAndroidID(androidID, game_id, activeDate6dayBefore, activeDate, conn)
                 }
               }
            }
          }
          //matched取出的是匹配后的设备的点击明细表数据，若能匹配得到则要算设备统计数据，并且写入激活明细表 计算注册时使用
          if (matched._1 >= 1) {
              //这个pkgCode ,android 和 ios 都取的是广告日志里面的
              val pkgCode = matched._2
              val redisValue: Array[String] = CommonsThirdData.getRedisValue(game_id, pkgCode, activeDate, jedis, connFx)
              val adv_name = matched._5
              //从redis中取出发行信息
              val group_id = redisValue.apply(6)
              val medium_account = redisValue(2)
              val head_people = redisValue(5)
              val idea_id = matched._6
              val first_level = matched._7
              val second_level = matched._8
              val channel_main_id = matched._9
              val channel_name = matched._10

              //android的imei如果为空或者全是0，需要转换为"no_imei"再存储
              if (imei.equals("") || imei.matches("[0]{7,}")) {
                imei = "no_imei"
              }
              //激活数 ： 激活匹配到点击
              val activeNum = 1
              //把激活匹配数据写入到明细，后期注册统计使用
              ThirdDataDao1.insertActiveDetail(activeTime, imei, pkgCode, adv_name, game_id, osInt, channel_main_id, channel_name, ip, devType, sysVersion, androidID, conn)
              //激活统计,只算匹配量
              ThirdDataDao1.insertActiveStat(activeDate, game_id, group_id, pkgCode, head_people, medium_account, adv_name, idea_id, first_level, second_level, activeNum, channel_main_id, channel_name, conn)
            }
        })
        pool.returnBrokenResource(jedis)
        pool.returnResource(jedis6)
        pool.destroy()
        conn.close()
        connFx.close()
      })
      rdd.unpersist()
    })
  }


  /**
    * 注册匹配激活
    * game_account  game_id  expand_channel._3 reg_time  imei os
    *
    * @param dataRegi
    */

  def regiMatchActive(dataRegi: DStream[(String, Int, String, String, String, String)]) = {

    dataRegi.foreachRDD(rdd => {
      rdd.persist()
      //读取本地缓存的
      val cacheRegiRdd: RDD[String] = rdd.sparkContext.textFile(ConfigurationUtil.getProperty("regi_log_cache"), 1)
      val newCacheRegiRdd = cacheRegiRdd.map(line => {
        val newLine = line.substring(1, line.indexOf(")"))
        val fields = newLine.split(",", -1)
        (fields(0), fields(1).toInt, fields(2), fields(3), fields(4), fields(5))
      })
      val newRegiRdd = rdd.union(newCacheRegiRdd)

      //组合后的rdd处理
      newRegiRdd.foreachPartition(part => {
        val pool: JedisPool = JedisUtil.getJedisPool;
        val jedis = pool.getResource
        jedis.select(0)
        val jedis6 = pool.getResource
        jedis6.select(6)
        val conn = JdbcUtil.getConn()
        val stmt = conn.createStatement()
        val connFx = JdbcUtil.getXiaopeng2FXConn()
        part.foreach(line => {
          val gameId = line._2.toInt
          var imei = CommonsThirdData.getImei(line._5)
          val androidID = CommonsThirdData.getAndroidID(line._5)
          val gameAccount = line._1
          val osInt = if (line._6.equals("android")) 1 else 2
          val regiTime = line._4
          val regiDate = line._4.substring(0, 10)
          //注册匹配激活，时间有效期是30天内，这里传入的参数该是-29
          val regiDate29dayBefore = CommonsThirdData.getDateBeforeParams(regiDate, -29)
          var matched = Tuple8(0, "", "", "", "", 0, "", "")
          //根据imei以及game_account判断是否要进行匹配
          if (osInt == 2) {
            //ios
            if (!imei.equals("") && !imei.matches("[0]{7,}")) {
              //因为在本地文件缓存了上一批注册数据，并且将上一批数据和这一批数据合一起传入这一批，因此这里要做一次去重检查
              val select_regi = "select game_account from bi_ad_regi_o_detail where game_account='" + gameAccount + "'"
              val resultSet = stmt.executeQuery(select_regi)
              if (!resultSet.next()) {
                //imei有效 -->执行匹配动作
                matched = CommonsThirdData.iosRegiMatchActive(imei, regiDate29dayBefore, regiDate, gameId, conn)
              }
            }
          } else {
            //android
            if (!imei.equals("") && !imei.matches("[0]{7,}")) {
              //imei有效
              val select_regi = "select game_account from bi_ad_regi_o_detail where game_account='" + gameAccount + "'"
              val resultSet = stmt.executeQuery(select_regi)
              if (!resultSet.next()) {
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
                matched = CommonsThirdData.androidRegiMatchActiveByAndroidID(androidID, regiDate29dayBefore, regiDate, gameId, conn)
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
            if (adv_name == 6 || adv_name == 3) {
              //一个设备,现在做的是:注册每次都上报，订单每次都上报
              //更新激活明细表中的matched_regi,regi_time,regi_time不能存入注册日志中的regi_time,要存入now(),获取更新数据的时间去存入
              //现在regi_time字段存入的是该设备注册最后一个帐号的注册时间
              CommonsThirdData.updateRegiMatchedActive(pkgCode, regiTime, imei, conn)
            }
            val redisValue: Array[String] = CommonsThirdData.getRedisValue(gameId, pkgCode, regiDate, jedis, connFx)
            val group_id = redisValue(6)
            val medium_account = redisValue(2)
            val head_people = redisValue(5)
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
              case 5 => "uc"
              case 4 => "aiqiyi"
              case 6 => "guangdiantong"
              case 7 => "uc_appstore"
              case 8 => "youku"
              case 9 => "inmobi"
              case -1 => channel_name
            }
            //android的imei如果为空或者全是0，需要转换为"no_imei"再存储
            if (imei.equals("") || imei.matches("[0]{7,}")) {
              imei = "no_imei"
            }
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
            ThirdDataDao1.insertRegiDetail(regiTime, imei, pkgCode, adv_name, gameId, osInt, gameAccount, channel_main_id, channel_name, androidID, conn)
            //注册统计
            ThirdDataDao1.insertRegiStat(regiDate, gameId, group_id, pkgCode, head_people, medium_account, adv_name, idea_id, first_level, second_level, regiNum, regiDevNum, newRegiDevNum, conn)

          }


        })
        pool.returnBrokenResource(jedis)
        pool.returnResource(jedis6)
        pool.destroy()
        conn.close()
        connFx.close()
      })
      //将rdd保存到本地文件
      rdd.saveAsTextFile(ConfigurationUtil.getProperty("regi_log_cache"))

      rdd.unpersist()
    })

  }

  /**
    * 消费匹配注册
    * 游戏账号（5），订单号（2），订单时间（6），游戏id（7）,充值流水（10），imei(24)
    *
    * @param dataOrder
    */
  def orderMatchRegi(dataOrder: DStream[(String, String, String, String, Float, String)]) = {

    dataOrder.foreachRDD(rdd => {
      rdd.foreachPartition(part => {
        val pool: JedisPool = JedisUtil.getJedisPool;
        val jedis = pool.getResource
        val jedis6 = pool.getResource
        jedis6.select(6)
        val conn = JdbcUtil.getConn()
        val connFx = JdbcUtil.getXiaopeng2FXConn()
        part.foreach(line => {
          val gameId = line._4.toInt
          val imei = CommonsThirdData.getImei(line._6)
          val gameAccount = line._1
          val orderID = line._2
          val orderTime = line._3
          val orderDate = orderTime.substring(0, 10)
          //过滤重复日志
          if (!jedis6.exists("adv_orderexists|" + gameAccount + "|" + orderID + "|" + orderTime)) {
            //accountInfo = (pkgId, regiTime, adName, os, ideaId, firstLevel, secondLevel,imei)
            //订单通过帐号关联注册
            val accountInfo = CommonsThirdData.getMatchedAccountInfo(gameAccount, conn)
            val pkgCode = accountInfo._1
            val regiDate = accountInfo._2.substring(0, 10)
            val advName = accountInfo._3
            //advName!=0 就是订单匹配到了注册
            if (advName != 0) {
              //广点通,今日头条订单上报
              if (advName == 6 || advName == 3) {
                //不能只根据imei和matched来匹配
                //订单匹配到注册，更新bi_ad_active_o_detail表matched_order,ordertime
                CommonsThirdData.updateOrderMatchActive(pkgCode, orderTime, imei, conn)
              }

              val redisValue: Array[String] = CommonsThirdData.getRedisValue(gameId, pkgCode, orderDate, jedis, connFx)
              val medium = advName
              //发行信息
              val group_id = redisValue(6)
              val medium_account = redisValue(2)
              val head_people = redisValue(5)

              val os = accountInfo._4
              val idea_id = accountInfo._5
              val first_level = accountInfo._6
              val second_level = accountInfo._7
              val channel_main_id = accountInfo._8
              val channel_name = accountInfo._9

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
              ThirdDataDao1.insertOrderDetail(orderID, orderTime, imei, pkgCode, medium, gameId, os, gameAccount, payPrice, channel_main_id, channel_name, conn)
              //消费统计
              ThirdDataDao1.insertOrderStat(orderDate, gameId, group_id, pkgCode, head_people, medium_account, medium, idea_id, first_level,
                second_level, payPrice, payAccs, newPayPrice, newPayAccs, conn)
            }
          }
          //redis中做订单去重，去除重复计算
          jedis6.set("adv_orderexists|" + gameAccount + "|" + orderID + "|" + orderTime, "")
          jedis6.expire("adv_orderexists|" + gameAccount + "|" + orderID + "|" + orderTime, 3600 * 48)

        })
        pool.returnBrokenResource(jedis)
        pool.returnResource(jedis6)
        pool.destroy()
        conn.close()
        connFx.close()
      }
      )
    })

  }

  /**
    * 本公司日志处理
    *
    * @param ownerData
    */
  def theOwnerData(ownerData: DStream[String]) = {
    /**
      * 激活匹配单击
      */

    val dataActive = ownerData.filter(ats => {
      val fields = ats.split("\\|", -1)
      fields(0).contains("bi_active") && fields.length == 12 && fields(5).length >= 13
    }).map(actives => {
      val splitd = actives.split("\\|", -1)
      //gameid channelid expand_channel,imei,date,os,ip,deviceType,systemVersion
      //bi: bi_active|5202|21|djqy_hj_1|djqy_hj_1|2017-11-08 11:32:02|117.22.103.195|IOS|0CD6416392D14CD9B21109DCCCFC3DE9||11.0.3|iPhone 6s
      (splitd(1), splitd(2), splitd(4), splitd(8), splitd(5), splitd(7), splitd(6), splitd(11), splitd(10))
    })
    activeMatchClick(dataActive)

    /**
      * 注册匹配单击
      */
    val dataRegi = ownerData.filter(ats => ats.contains("bi_regi")).filter(line => {
      val rgInfo = line.split("\\|", -1)
      rgInfo.length > 14 && StringUtils.isNumber(rgInfo(4))
    }).map(regi => {
      val arr = regi.split("\\|", -1)
      //game_account  game_id  expand_channel._3 reg_time  imei,os
      (arr(3), arr(4).toInt, StringUtils.getArrayChannel(arr(13))(2), arr(5), arr(14), arr(11).toLowerCase)
    })
    regiMatchActive(dataRegi)


    /**
      * 消费匹配
      */

    val dataOrder = ownerData.filter(ats => ats.contains("bi_order")).filter(line => {
      val arr = line.split("\\|", -1)
      //排除截断日志   只取存在订单号的数据，不存在订单号的为代金券消费  只取直充  只取状态为4的数据
      arr.length >= 25 && arr(2).trim.length > 0 && arr(22).contains("6") && arr(19).toInt == 4
    }).map(line => {
      val odInfo = line.split("\\|", -1)
      //游戏账号（5），订单号（2），订单时间（6），游戏id（7）,充值流水（10），imei(24)
      (odInfo(5).trim.toLowerCase, odInfo(2), odInfo(6), odInfo(7), Commons.getNullTo0(odInfo(10)) + Commons.getNullTo0(odInfo(13)), odInfo(24))
    })
    orderMatchRegi(dataOrder)

  }

  /**
    * 对广告单击数据进行处理
    *
    * @param thirdData
    */
  def adClick(thirdData: DStream[String]) = {

    val simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    //1：momo
    //android : os 是 0 ,imei 是md5加密小写 9f38c33f5d5e2a4d0052d743f3ef8fcb
    //ios : os 是 1 ,imei 是 带横杠原值 F9106120-4803-4F23-825D-218DC4C3BA07
    val adData = thirdData.filter(x => x.contains("bi_adv_momo_click")).map(line => {
      try {
        val jsStr = JSONObject.fromObject(line)
        (jsStr.get("pkg_id").toString,
          if (jsStr.get("os").toString.equals("0") || jsStr.get("os").toString.equals("2")) "android" else if (jsStr.get("os").toString.equals("1")) "ios" else "wp",
          if (jsStr.get("imei").toString.equals("")) jsStr.get("idfa").toString else jsStr.get("imei").toString,
          if (jsStr.get("ts").toString.length == 10) simpleDateFormat.format(new Date(jsStr.get("ts").toString.toLong * 1000)) else simpleDateFormat.format(new Date(jsStr.get("ts").toString.toLong)),
          jsStr.get("callback").toString,
          1, "", "", "", "", "", "0", "", ""
        )
      } catch {
        case e: Exception => e.printStackTrace; ("", "", "", "0", "", 1, "", "", "", "", "", "0", "", "")
      }

    }).union(//2： baidu
      //2：baidu
      //android : os 是 0 ,imei 是md5加密小写 9f38c33f5d5e2a4d0052d743f3ef8fcb
      //ios : os 是 1 ,imei 是 带横杠原值 F9106120-4803-4F23-825D-218DC4C3BA07
      thirdData.filter(x => x.contains("bi_adv_baidu_click")).map(line => {
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
          case e: Exception => e.printStackTrace; ("", "", "", "0", "", 2, "", "", "", "", "", "0", "", "")
        }

      })
    ).union(//3：今日头条
      //今日头条
      //android : os 是 0 ,imei 是md5加密小写 9f38c33f5d5e2a4d0052d743f3ef8fcb
      //ios : os 是 1 ,imei 是 带横杠原值 F9106120-4803-4F23-825D-218DC4C3BA07
      thirdData.filter(x => x.contains("bi_adv_jinretoutiao_click")).map(line => {
        try {
          val jsStr = JSONObject.fromObject(line)
          (jsStr.get("pkg_id").toString,
            if (jsStr.get("os").toString.equals("0")) "android" else if (jsStr.get("os").toString.equals("1")) "ios" else "wp",
            jsStr.get("imei").toString,
            if (jsStr.get("timestamp").toString.length == 10) simpleDateFormat.format(new Date(jsStr.get("timestamp").toString.toLong * 1000)) else simpleDateFormat.format(new Date(jsStr.get("timestamp").toString.toLong)),
            jsStr.get("callback").toString,
            3, jsStr.get("cid").toString, "", jsStr.get("adid").toString, "", "", "0", "",
            if (jsStr.get("os").toString.equals("0")) jsStr.get("androidid").toString else ""
          )
        } catch {
          case e: Exception => e.printStackTrace; ("", "", "", "0", "", 3, "", "", "", "", "", "0", "", "")
        }
      })
    ).union(//4：爱奇艺
      //爱奇艺
      //android : os 是 0 ,imei 是md5加密小写 9f38c33f5d5e2a4d0052d743f3ef8fcb
      //ios : os 是 1 ,imei 是 带横杠原值 F9106120-4803-4F23-825D-218DC4C3BA07
      //imei字段经常返回的是不合法字段，aiqiyi去udid字段的值为imei
      thirdData.filter(x => x.contains("bi_adv_aiqiyi_click")).map(line => {
        try {
          val jsStr = JSONObject.fromObject(line)
          (jsStr.get("pkg_id").toString,
            if (jsStr.get("os").toString.equals("2")) "android" else if (jsStr.get("os").toString.equals("1")) "ios" else "wp",
            jsStr.get("udid").toString,
            if (jsStr.get("ts").toString.length == 10) simpleDateFormat.format(new Date(jsStr.get("ts").toString.toLong * 1000)) else simpleDateFormat.format(new Date(jsStr.get("ts").toString.toLong)),
            //爱奇异不用回传激活url
            "", 4, "", "", "", "", "", "0", "", "")
        } catch {
          case e: Exception => e.printStackTrace();
            ("", "", "", "0", "", 4, "", "", "", "", "", "0", "", "")
        }
      })
    ).union(//5:UC
      //UC平台的json日志中，ios日志为 D3C03563D0372AC91F04323E6F5D218E ,这个不是直接的imei，是imei MD5加密后大写
      thirdData.filter(x => x.contains("bi_adv_uc_click")).map(line => {
        try {
          val jsStr = JSONObject.fromObject(line)
          (jsStr.get("pkg_id").toString,
            if (jsStr.get("os").toString.equals("1")) "android" else if (jsStr.get("os").toString.equals("0")) "ios" else "wp",
            jsStr.get("imei").toString,
            if (jsStr.get("time").toString.length == 10) simpleDateFormat.format(new Date(jsStr.get("time").toString.toLong * 1000)) else simpleDateFormat.format(new Date(jsStr.get("time").toString.toLong)),
            jsStr.get("callback").toString,
            5, jsStr.get("cid").toString, "", jsStr.get("aid").toString, "", "", "0", "", ""
          )
        } catch {
          case e: Exception => e.printStackTrace;
            ("", "", "", "0", "", 5, "", "", "", "", "", "0", "", "")
        }
      })
    ).union(//6:广点通
      thirdData.filter(x => x.contains("bi_adv_guangdiantong_click")).map(line => {
        try {
          //Android 设备号加密测试用例:
          //原始 IMEI 号：354649050046412
          //加密之后：b496ec1169770ea274a2b4f42ca4fb71
          //iOS 设备号加密测试用例:
          //原始 IDFA 码：1E2DFA89-496A-47FD-9941-DF1FC4E6484A
          //加密之后：40c7084b4845eebce9d07b8a18a055fc
          val jsStr = JSONObject.fromObject(line)
          (jsStr.get("pkg_id").toString,
            if (jsStr.get("app_type").toString.toLowerCase.contains("android")) "android" else if (jsStr.get("app_type").toString.toLowerCase.contains("ios")) "ios" else "wp",
            jsStr.get("muid").toString,
            if (jsStr.get("click_time").toString.length == 10) simpleDateFormat.format(new Date(jsStr.get("click_time").toString.toLong * 1000)) else simpleDateFormat.format(new Date(jsStr.get("click_time").toString.toLong)),
            jsStr.get("callback").toString,
            6, "", "", "", "", "", "0", "", ""
          )
        } catch {
          case e: Exception => e.printStackTrace;
            ("", "", "", "0", "", 6, "", "", "", "", "", "0", "", "")
        }
      })
    ).union(
      thirdData.filter(x => x.contains("bi_adv_medium_click")).map(line => {
        //除旧的6个媒介，其他新增第三方广告
        //10月10日新增加3个媒介
        // 7 : UC应用商店
        // 8 : 优酷
        // 9 : inmobi
        //-1 : -1表示的是渠道以及媒介中专门用ip+ua 做激活匹配的数据
        try {
          //map方法里面，不能直接使用外面的对象
          val simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
          //解析json字符串
          val jsStr = JSONObject.fromObject(line)
          //取出媒介编号adv_id
          val adv_id = jsStr.get("adv_id").toString.toInt
          //根据媒介编号去判断是哪家媒介
          if (adv_id == 7) {
            //UC应用商店,逻辑与UC相同 -->已经接入完成
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

          } else if (adv_id == 8) {
            //优酷
            //android : os 是 0 ,imei 是md5加密小写 9f38c33f5d5e2a4d0052d743f3ef8fcb
            //ios : os 是 1 ,imei 是 不带横杠imei加密后大写
            (
              jsStr.get("pkg_id").toString,
              if (jsStr.get("os").toString.equals("0")) "android" else if (jsStr.get("os").toString().equals("1")) "ios" else "wp",
              jsStr.get("imei").toString,
              if (jsStr.get("ts").toString.length == 10) simpleDateFormat.format(new Date(jsStr.get("ts").toString.toLong * 1000)) else simpleDateFormat.format(new Date(jsStr.get("ts").toString.toLong)),
              jsStr.get("callback").toString,
              8, "", "", "", jsStr.get("ip").toString, jsStr.get("ua").toString, "0", "", ""
            )
          } else if (adv_id == 9) {
            //inmobi
            (
              jsStr.get("pkg_id").toString,
              if (jsStr.get("os").toString.equals("0")) "android" else if (jsStr.get("os").toString.equals("1")) "ios" else "wp",
              jsStr.get("imei").toString,
              if (jsStr.get("ts").toString.length == 10) simpleDateFormat.format(new Date(jsStr.get("ts").toString.toLong * 1000)) else simpleDateFormat.format(new Date(jsStr.get("ts").toString.toLong)),
              jsStr.get("callback").toString,
              9, "", "", "", "", "", "0", "", ""
            )
          } else if (adv_id == 17) {
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
          } else if (adv_id == -1) {
            //都是渠道来的数据
            (
              jsStr.get("pkg_id").toString,
              if (jsStr.get("os").toString.equals("0")) "android" else if (jsStr.get("os").toString().equals("1")) "ios" else "wp",
              jsStr.get("imei").toString,
              if (jsStr.get("ts").toString.length == 10) simpleDateFormat.format(new Date(jsStr.get("ts").toString.toLong * 1000)) else simpleDateFormat.format(new Date(jsStr.get("ts").toString.toLong)),
              "", -1, "", "", "", jsStr.get("ip").toString, jsStr.get("ua").toString, jsStr.get("channel_main_id").toString, jsStr.get("channel_name").toString, ""
            )
          } else {
            //没有匹配到媒介的数据，就当异常数据处理
            ("", "", "", "0", "", 100, "", "", "", "", "", "0", "", "")
          }
        } catch {
          case e: Exception => e.printStackTrace;
            //异常数据处理
            ("", "", "", "0", "", 100, "", "", "", "", "", "0", "", "")
        }
      })
    )

    //共14个字段
    //(pkg_id,os,imei,ts,callback,adv_id,aid,uid,pid,ip,ua,channel_main_id,channel_name,androidID)
    //广告点击日志的处理
    theThirdData(adData)

  }

}
