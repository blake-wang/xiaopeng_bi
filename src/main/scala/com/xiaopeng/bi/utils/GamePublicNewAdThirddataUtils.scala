package com.xiaopeng.bi.utils

import java.sql.{Connection, Statement}
import java.text.SimpleDateFormat
import java.util.Date

import com.xiaopeng.bi.utils.dao.GamePublicNewAdThirdDataDao
import net.sf.json.JSONObject
import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import redis.clients.jedis.JedisPool

import scala.collection.mutable.ArrayBuffer

/**
  * Created by bigdata on 18-3-7.
  */
object GamePublicNewAdThirddataUtils {
  val logger = Logger.getLogger(this.getClass)

  def loadThirddataInfo(rdd: RDD[String]): Unit = {

    val thirdDataRDD = rdd.filter(x => x.contains("bi_thirddata")).filter(x => (!x.contains("bi_adv_money"))) //排除钱大师
      .map(line => line.substring(line.indexOf("{"), line.length))
    val ownerDataRDD = rdd.filter(x => (!x.contains("bi_thirddata")))

    if (!thirdDataRDD.isEmpty()) {
      //处理点击日志
      executeClickData(thirdDataRDD)
    }

    if (!ownerDataRDD.isEmpty()) {
      //处理激活，注册，订单日志
      executeOwnerData(ownerDataRDD)
    }
  }

  /**
    * 处理广告点击数据，存入明细表，统计表
    *
    * @param thirdData
    */
  def executeClickData(thirdData: RDD[String]): Unit = {
    val simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    //1：momo
    //android : os 是 0 ,imei 是md5加密小写 9f38c33f5d5e2a4d0052d743f3ef8fcb
    //ios : os 是 1 ,imei 是 带横杠原值 F9106120-4803-4F23-825D-218DC4C3BA07
    val adData = thirdData.filter(x => x.contains("bi_adv_momo_click")).map(line => {
      try {
        val jsStr = JSONObject.fromObject(line)
        (jsStr.get("pkg_id").toString,
          if (jsStr.get("os").toString.equals("0") || jsStr.get("os").toString.equals("2")) "android" else if (jsStr.get("os").toString.equals("1")) "ios" else "wp",
          jsStr.get("imei").toString,
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
            "", 4, "", "", "", jsStr.get("ip").toString, jsStr.get("ua").toString, "0", "", "")
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
          } else if (adv_id == 18) {
            //新浪扶翼
            (
              jsStr.get("pkg_id").toString,
              if (jsStr.get("os").toString.equals("0")) "android" else if (jsStr.get("os").toString.equals("1")) "ios" else "wp",
              jsStr.get("devid").toString,
              if (jsStr.get("ts").toString.length == 10) simpleDateFormat.format(new Date(jsStr.get("ts").toString.toLong * 1000)) else simpleDateFormat.format(new Date(jsStr.get("ts").toString.toLong)),
              jsStr.get("callback").toString,
              18, "", "", "", jsStr.get("ip").toString, jsStr.get("ua").toString, "0", "", ""
            )
          }
          else if (adv_id == -1) {
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

  def theThirdData(adData: RDD[(String, String, String, String, String, Int, String, String, String, String, String, String, String, String)]): Unit = {
    adData.foreachPartition(part => {
      val conn = JdbcUtil.getConn()
      val pool: JedisPool = JedisUtil.getJedisPool;
      //取了2个redis对象，分别选择库1和库8
      val jedis = pool.getResource
      val jedis8 = pool.getResource
      jedis8.select(8)
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
            var advName = line._6.toInt
            //2017-09-13
            val clickDate = clickTime.substring(0, 10)
            //4151
            var game_id = 0
            val callback = line._5
            //parent_game_id,os,medium_account,promotion_channel,promotion_mode,head_people,groupid
            val redisValue: Array[String] = GamePublicNewAdThirddataCommonUtils.getRedisValue(game_id, pkg_id, clickDate, jedis, connFx)
            //从redis中取出发行平台信息
            val group_id = redisValue.apply(6)
            //媒介帐号
            val medium_account = redisValue(2)
            //负责人
            val head_people = redisValue(5)
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
            //渠道商下的渠道名称，或者是媒介id
            val channel_name = line._13
            //安卓ID  只有android设备才有androidid
            var androidID = ""
            if (osInt == 1) {
              androidID = line._14
            }
            //分包备注
            var remark = ""

            //game_id
            if (pkg_id.contains("M")) {
              game_id = pkg_id.substring(0, pkg_id.indexOf("M")).toInt
              remark = GamePublicNewAdThirddataCommonUtils.getMediumRemark(pkg_id, connFx)
            } else if (pkg_id.contains("Q")) {
              game_id = pkg_id.substring(0, pkg_id.indexOf("Q")).toInt
              remark = GamePublicNewAdThirddataCommonUtils.getChannelRemark(pkg_id, connFx)
            }


            //点击数:有点击日志一条，就计算一个点击数
            val click_num = 1
            //点击设备数：如果 imei,ua,ip都为空，那这个点击不计算设备数
            var click_dev_num = 0

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
              case 18 => "xinlang"
              case -1 => channel_name
            }


            //判断哪些类型的数据要按渠道数据处理,纯UA+IP
            val idCache = ArrayBuffer[Int](-1, 10, 11, 12, 13, 14, 15, 16)
            val res = idCache.contains(advName)

            //有些媒介也是到这里处理(如网易新闻,凤凰新闻等)
            //当res为true时，说明这条数据是按渠道的方式处理
            if (res) { //-->渠道数据.
              //这里处理渠道数据的UA+IP统计，所以UA和IP不能为空
              if (!ip.equals("") && !ua.equals("")) {
                //先判断这个设备是否有点击记录 0：没有点击数据 1：点击未激活 2：点击并且激活
                val clickStatus = GamePublicNewAdThirddataCommonUtils.checkChannelClickDataStatus(game_id, ip, devType, sysVersion, conn)
                if (clickStatus == 0) {
                  imei = "channel_no_imei"
                  val imei_md5_upper = "channel_no_imei_md5_upper"
                  GamePublicNewAdThirdDataDao.insertChannelClickDetail(pkg_id, imei, imei_md5_upper, clickTime, os, callback, game_id.toString, advName, ip, devType, sysVersion, channel_main_id, channel_name, conn)
                } else if (clickStatus == 1) {
                  GamePublicNewAdThirdDataDao.updateChannelDataClickDetail(pkg_id, game_id, clickTime, ip, devType, sysVersion, callback, conn)
                } else if (clickStatus == 2) {
                  //ua+ip的方式，因为已经在激活明细表做了game_id + imei 去重的处理，所以不会出现一个设备多次匹配的问题
                  //当一个ip+ua匹配完之后，需要继续存入点击数据，如果有不同的设备激活，需要做继续匹配
                  imei = "channel_no_imei"
                  val imei_md5_upper = "channel_no_imei_md5_upper"
                  GamePublicNewAdThirdDataDao.insertChannelClickDetail(pkg_id, imei, imei_md5_upper, clickTime, os, callback, game_id.toString, advName, ip, devType, sysVersion, channel_main_id, channel_name, conn)
                }
                click_dev_num = GamePublicNewAdThirddataCommonUtils.isTheChannelDeviceHasClicked(game_id, topic, clickDate, ip, devType, sysVersion, conn, jedis8)
              }
            } else { //-->媒介数据
              if (!imei.equals("") && !imei.contains("00000000") && !imei.toUpperCase.equals("NULL")) {
                //imei有效
                val clickStatus = GamePublicNewAdThirddataCommonUtils.checkMediumClickDataStatusByImei(imei, game_id, conn)
                if (clickStatus == 0) {
                  //增加一个字段 imei_md5_upper，所有媒介的imei_md5_upper都统一处理为：imei->md5加密->大写
                  /**
                    * 新增加媒介，都要修改imeiToMd5Upper的代码,根据媒介的不同，添加不同的加密方式
                    */
                  val imei_md5_upper = GamePublicNewAdThirddataCommonUtils.imeiToMd5Upper(advName, imei, osInt)
                  //渠道设备有效，存入数据也要将imei,ip,devTyoe,sysVersion全部存入
                  GamePublicNewAdThirdDataDao.insertMediumClickDetail(pkg_id, imei, imei_md5_upper, clickTime, os, callback, game_id.toString, advName, ip, devType, sysVersion, androidID, conn)
                } else if (clickStatus == 1) {
                  //如果imei的点击已经存在，按游戏去重
                  GamePublicNewAdThirdDataDao.updateMediumClickByImei(pkg_id, game_id, imei, clickTime, callback, conn)
                }
              } else {
                //imei无效
                //分别用androidID,ip,ua 更新点击明细数据
                //androidID : [今日头条,懂球第]
                if (!androidID.equals("") && !androidID.equals("__ANDROIDID__")) {
                  val clickStatus = GamePublicNewAdThirddataCommonUtils.checkMediumClickDataStatusByAndroidID(game_id, androidID, conn)
                  if (clickStatus == 0) {
                    imei = "medium_no_imei"
                    val imei_md5_upper = "medium_no_imei_md5_upper"
                    GamePublicNewAdThirdDataDao.insertMediumClickDetail(pkg_id, imei, imei_md5_upper, clickTime, os, callback, game_id.toString, advName, ip, devType, sysVersion, androidID, conn)
                  } else if (clickStatus == 1) {
                    GamePublicNewAdThirdDataDao.updateMediumClickByAndroidID(pkg_id, game_id, androidID, clickTime, callback, conn)
                  }
                } else {
                  if (!ip.equals("")) {
                    //ip有效
                    if (!ua.equals("")) {
                      //ua有效  --> IP+UA 检查点击
                      val clickStatus = GamePublicNewAdThirddataCommonUtils.checkMediumClickDataStatusByIpAndUa(game_id, ip, devType, sysVersion, conn)
                      if (clickStatus == 0) {
                        imei = "medium_no_imei"
                        androidID = "medium_no_androidID"
                        val imei_md5_upper = "medium_no_imei_md5_upper"
                        GamePublicNewAdThirdDataDao.insertMediumClickDetail(pkg_id, imei, imei_md5_upper, clickTime, os, callback, game_id.toString, advName, ip, devType, sysVersion, androidID, conn)
                      } else if (clickStatus == 1) {
                        GamePublicNewAdThirdDataDao.updateMediumClickByIpAndUa(pkg_id, game_id, ip, devType, sysVersion, clickTime, callback, conn)
                      } else if (clickStatus == 2) {
                        imei = "medium_no_imei"
                        androidID = "medium_no_androidID"
                        val imei_md5_upper = "medium_no_imei_md5_upper"
                        GamePublicNewAdThirdDataDao.insertMediumClickDetail(pkg_id, imei, imei_md5_upper, clickTime, os, callback, game_id.toString, advName, ip, devType, sysVersion, androidID, conn)
                      }
                    } else {
                      //ua无效  --> IP 检查点击
                      val clickStatus = GamePublicNewAdThirddataCommonUtils.checkMediumClickDataStatusByIp(game_id, ip, conn)
                      if (clickStatus == 0) {
                        imei = "medium_no_imei"
                        androidID = "medium_no_androidID"
                        val imei_md5_upper = "medium_no_imei_md5_upper"
                        GamePublicNewAdThirdDataDao.insertMediumClickDetail(pkg_id, imei, imei_md5_upper, clickTime, os, callback, game_id.toString, advName, ip, devType, sysVersion, androidID, conn)
                      } else if (clickStatus == 1) {
                        GamePublicNewAdThirdDataDao.updateMediumClickByIp(pkg_id, game_id, ip, clickTime, callback, conn)
                      } else if (clickStatus == 2) {
                        imei = "medium_no_imei"
                        androidID = "medium_no_androidID"
                        val imei_md5_upper = "medium_no_imei_md5_upper"
                        GamePublicNewAdThirdDataDao.insertMediumClickDetail(pkg_id, imei, imei_md5_upper, clickTime, os, callback, game_id.toString, advName, ip, devType, sysVersion, androidID, conn)
                      }
                    }
                  }
                }
              }
              //click_dev_num 按游戏按天去重：一天内一个imei在一个游戏内，点击只计算一次
              click_dev_num = GamePublicNewAdThirddataCommonUtils.isTheMediumDeviceHasClicked(clickDate, game_id, imei, topic, ip, devType, sysVersion, androidID, jedis8)
            }
            //插入数据到统计表
            //如果媒介数据，advName存入的是媒介编号，如果是渠道数据，advName存入的是-1
            GamePublicNewAdThirdDataDao.insertClickStat(clickDate, game_id, group_id, pkg_id, head_people, medium_account, advName, channel_main_id, channel_name, click_num, click_dev_num, remark, conn)
          }
        }
      })
      pool.returnBrokenResource(jedis)
      pool.returnBrokenResource(jedis8)
      pool.destroy()
      conn.close()
      connFx.close()
    })
  }


  /**
    * 处理激活匹配点击，注册匹配激活，订单匹配注册
    *
    * @param ownerData
    */
  def executeOwnerData(ownerData: RDD[String]): Unit = {

    /**
      * 激活匹配点击
      */
    val dataActive = ownerData.filter(ats => {
      val fields = ats.split("\\|", -1)
      fields(0).contains("bi_active") && fields.length >= 12 && fields(5).length >= 13
    }).map(actives => {
      val splitd = actives.split("\\|", -1)
      //gameid channelid expand_channel,imei,date,os,ip,deviceType,systemVersion
      //bi: bi_active|5202|21|djqy_hj_1|djqy_hj_1|2017-11-08 11:32:02|117.22.103.195|IOS|0CD6416392D14CD9B21109DCCCFC3DE9||11.0.3|iPhone 6s|h5
      (splitd(1), splitd(2), splitd(4), splitd(8), splitd(5), splitd(7), splitd(6), splitd(11), splitd(10))
    })
    activeMatchClick(dataActive)

    /**
      * 注册匹配激活
      */
    val dataRegi = ownerData.filter(ats => ats.contains("bi_regi")).filter(line => {
      val rgInfo = line.split("\\|", -1)
      rgInfo.length >= 15 && StringUtils.isNumber(rgInfo(4))
    }).map(regi => {
      val arr = regi.split("\\|", -1)
      //game_account  game_id  expand_channel._3 reg_time  imei,os
      (arr(3), arr(4).toInt, StringUtils.getArrayChannel(arr(13))(2), arr(5), arr(14), arr(11).toLowerCase)
    })
    regiMatchActive(dataRegi)

    /**
      * 登录匹配注册
      */
    val dataLogin = ownerData.filter(line => {
      val fields = line.split("\\|", -1)
      fields(0).contains("bi_login") && fields.length >= 9 && StringUtils.isNumber(fields(8))
    }).map(line => {
      val fields = line.split("\\|", -1)
      //game_account(3),login_time(4),game_id(8)，pkg_id(6),imei(7)
      (fields(3).trim.toLowerCase, fields(4), fields(8).toInt, StringUtils.getArrayChannel(fields(6))(2), fields(7))
    })
    loginMatchRegi(dataLogin)


    /**
      * 订单匹配注册``
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
    * 激活匹配点击
    *
    * @param dataActive
    */
  def activeMatchClick(dataActive: RDD[(String, String, String, String, String, String, String, String, String)]) = {

    dataActive.foreachPartition(iter => {
      val pool: JedisPool = JedisUtil.getJedisPool;
      val jedis = pool.getResource
      val jedis8 = pool.getResource
      jedis8.select(8)
      val conn = JdbcUtil.getConn()
      val connFx = JdbcUtil.getXiaopeng2FXConn()
      val stmt = conn.createStatement()
      iter.foreach(line => {
        //(3802,21,jlcy_lj_1,5896EEA3ECCF40FAA33AF1201DA8B3EF,2017-11-09 10:30:56,IOS,127.0.0.1,iPhone 6,10.3.3)
        val game_id = line._1.toInt
        val activelog_imei = line._4
        //221132093280616&64110abc12e796d5&f8:2f:48:a1:6d:20
        //安卓设备取最前面部分(15位数字)，苹果设备取完(32位英文数字大写)
        var imei = GamePublicNewAdThirddataCommonUtils.getImei(activelog_imei)
        //安卓设备：安卓ID取中间16位 / IOS设备：没有androidID，为""
        val androidID = GamePublicNewAdThirddataCommonUtils.getAndroidID(activelog_imei)
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
        val activeDate6dayBefore = GamePublicNewAdThirddataCommonUtils.getDateBeforeParams(activeDate, -6)
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
            statusCode = GamePublicNewAdThirdDataDao.checkImeiIsMatched(game_id, imei, conn, stmt)
            if (statusCode) {
              //匹配第一步：通过imei进行匹配

              //imei加密：因为有些媒介是带横杠的原值，有些是不带横杠的原值，因此这里用两种加密方式
              //1:imei加横杠再md5加密大写
              val imeiWithLine = GamePublicNewAdThirddataCommonUtils.imeiPlusDash(imei.toUpperCase())
              val imeiWithLine_md5_upper = MD5Util.md5(imeiWithLine).toUpperCase
              //2:imei不加横杠md5加密大写
              val imei_md5_upper = MD5Util.md5(imei).toUpperCase
              //ios设备的匹配：imei + game_id
              matched = GamePublicNewAdThirdDataDao.iosMatchClickByImei(imei, imeiWithLine_md5_upper, imei_md5_upper, game_id, activeDate6dayBefore, activeDate, conn)

              //匹配第二步：如果imei没有匹配到，如果IP和UA有效，再用UA+IP匹配一次
              if (matched._1 == 0) {
                if (!ip.equals("")) {
                  //ip有效
                  if (!devType.equals("")) {
                    // devType有效  --> 执行 game_id + ip + devType + sysVersion 匹配
                    if (!sysVersion.equals("")) {
                      //sysVersion有效  --> 执行 game_id + ip + devType + sysVersion 匹配
                      matched = GamePublicNewAdThirdDataDao.iosMatchClickByIPAndDevAndSys(game_id, activeDate6dayBefore, activeDate, ip, devType, sysVersion, conn)
                      if (matched._1 == 0) {
                        //game_id + ip + devType + sysVersion 没有匹配到  --> 执行 game_id + ip + devType 匹配
                        matched = GamePublicNewAdThirdDataDao.iosMatchClickByIPAndDev(game_id, activeDate6dayBefore, activeDate, ip, devType, conn)
                        if (matched._1 == 0) {
                          //game_id + ip + devType 没有匹配到  --> 执行 game_id + ip 匹配
                          matched = GamePublicNewAdThirdDataDao.iosMatchClickByIP(game_id, activeDate6dayBefore, activeDate, ip, conn)
                        }
                      }
                    } else {
                      //sysVersion无效  --> 执行 game_id + ip + devType 匹配
                      matched = GamePublicNewAdThirdDataDao.iosMatchClickByIPAndDev(game_id, activeDate6dayBefore, activeDate, ip, devType, conn)
                    }
                  } else {
                    // devType无效  --> 执行 game_id + ip 匹配
                    matched = GamePublicNewAdThirdDataDao.iosMatchClickByIP(game_id, activeDate6dayBefore, activeDate, ip, conn)
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
            statusCode = GamePublicNewAdThirdDataDao.checkImeiIsMatched(game_id, imei, conn, stmt)
            if (statusCode) {
              //imei有效
              val imei_md5_upper = MD5Util.md5(imei).toUpperCase
              matched = GamePublicNewAdThirdDataDao.androidMatchClickByImei(imei_md5_upper, game_id, activeDate6dayBefore, activeDate, conn)
              if (matched._1 == 0) {
                //imei没有匹配到，再用androidID匹配
                if (!androidID.equals("")) {
                  //androidID有效
                  statusCode = GamePublicNewAdThirdDataDao.checkAndroidIDIsMatched(game_id, androidID, conn, stmt)
                  if (statusCode) {
                    //androidID没有检测到激活 --> 执行匹配动作
                    matched = GamePublicNewAdThirdDataDao.androidMatchClickByAndroidID(androidID, game_id, activeDate6dayBefore, activeDate, conn)
                  }
                }
              }
            }
          } else {
            //imei无效
            if (!androidID.equals("")) {
              //androidID有效
              statusCode = GamePublicNewAdThirdDataDao.checkAndroidIDIsMatched(game_id, androidID, conn, stmt)
              if (statusCode) {
                //androidID没有检测到激活 -->执行匹配动作
                matched = GamePublicNewAdThirdDataDao.androidMatchClickByAndroidID(androidID, game_id, activeDate6dayBefore, activeDate, conn)
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
          val redisValue: Array[String] = GamePublicNewAdThirddataCommonUtils.getRedisValue(game_id, pkgCode, activeDate, jedis, connFx)
          val adv_name = matched._5
          //从redis中取出发行信息
          val group_id = redisValue.apply(6)
          val medium_account = redisValue(2)
          val head_people = redisValue(5)
          val channel_main_id = matched._9
          val channel_name = matched._10

          //android的imei如果为空或者全是0，需要转换为"no_imei"再存储
          if (imei.equals("") || imei.matches("[0]{7,}")) {
            imei = "no_imei"
          }

          //分包备注
          var remark = ""
          if (pkgCode.contains("M")) {
            remark = GamePublicNewAdThirddataCommonUtils.getMediumRemark(pkgCode, connFx)
          } else if (pkgCode.contains("Q")) {
            remark = GamePublicNewAdThirddataCommonUtils.getChannelRemark(pkgCode, connFx)
          }

          //激活数 ： 激活匹配到点击
          val activeNum = 1
          //把激活匹配数据写入到明细，后期注册统计使用
          GamePublicNewAdThirdDataDao.insertActiveDetail(activeTime, imei, pkgCode, adv_name, game_id, osInt, channel_main_id, channel_name, ip, devType, sysVersion, androidID, conn)
          //激活统计,只算匹配量
          GamePublicNewAdThirdDataDao.insertActiveStat(activeDate, game_id, group_id, pkgCode, head_people, medium_account, adv_name, activeNum, channel_main_id, channel_name, remark, conn)
        }
      })
      pool.returnBrokenResource(jedis)
      pool.returnResource(jedis8)
      pool.destroy()
      stmt.close()
      conn.close()
      connFx.close()
    })
  }


  /**
    * 注册匹配激活
    * game_account  game_id  expand_channel._3 reg_time  imei os
    *
    * @param dataRegi
    */

  def regiMatchActive(dataRegi: RDD[(String, Int, String, String, String, String)]) = {


    //读取本地缓存的
    //    val cacheRegiRdd: RDD[String] = dataRegi.sparkContext.textFile(ConfigurationUtil.getProperty("new_ad_regi_log_cache"), 1)
    //    val newCacheRegiRdd = cacheRegiRdd.map(line => {
    //      val newLine = line.substring(1, line.indexOf(")"))
    //      val fields = newLine.split(",", -1)
    //      (fields(0), fields(1).toInt, fields(2), fields(3), fields(4), fields(5))
    //    })
    //    val newRegiRdd = dataRegi.union(newCacheRegiRdd)
    //
    //    //组合后的rdd处理
    //    newRegiRdd.foreachPartition(part => {
    dataRegi.foreachPartition(part => {
      val pool: JedisPool = JedisUtil.getJedisPool;
      val jedis = pool.getResource
      jedis.select(0)
      val jedis8 = pool.getResource
      jedis8.select(8)
      val conn = JdbcUtil.getConn()
      val stmt = conn.createStatement()
      val connFx = JdbcUtil.getXiaopeng2FXConn()
      part.foreach(line => {
        val game_id = line._2.toInt
        var imei = GamePublicNewAdThirddataCommonUtils.getImei(line._5)
        val androidID = GamePublicNewAdThirddataCommonUtils.getAndroidID(line._5)
        val gameAccount = line._1
        val osInt = if (line._6.toLowerCase.contains("android")) 1 else 2
        val regiTime = line._4
        val regi_date = line._4.substring(0, 10)
        //注册匹配激活，时间有效期是30天内，这里传入的参数该是-29
        val regiDate29dayBefore = GamePublicNewAdThirddataCommonUtils.getDateBeforeParams(regi_date, -29)
        var matched = Tuple8(0, "", "", "", "", 0, "", "")
        //状态码，检测注册是否继续匹配
        var statusCode = false
        //根据imei以及game_account判断是否要进行匹配
        if (osInt == 2) {
          //ios
          if (!imei.equals("") && !imei.matches("[0]{7,}")) {
            //因为在本地文件缓存了上一批注册数据，并且将上一批数据和这一批数据合一起传入这一批，因此这里要做一次去重检查
            statusCode = GamePublicNewAdThirddataCommonUtils.checkAccountIsMatched(gameAccount, stmt)
            if (statusCode) {
              //imei有效 -->执行匹配动作
              matched = GamePublicNewAdThirddataCommonUtils.iosRegiMatchActive(imei, regiDate29dayBefore, regi_date, game_id, conn)
            }
          }
        } else {
          //android
          if (!imei.equals("") && !imei.matches("[0]{7,}")) {
            //imei有效
            statusCode = GamePublicNewAdThirddataCommonUtils.checkAccountIsMatched(gameAccount, stmt)
            if (statusCode) {
              //imei有效 -->执行匹配动作
              matched = GamePublicNewAdThirddataCommonUtils.androidRegiMatchActiveByImei(imei, regiDate29dayBefore, regi_date, game_id, conn)
              val adv_name = matched._1
              if (adv_name == 0) {
                //imei未匹配到 -->如果androidID有效，再用androidID进行匹配
                if (!androidID.equals("")) {
                  matched = GamePublicNewAdThirddataCommonUtils.androidRegiMatchActiveByAndroidID(androidID, regiDate29dayBefore, regi_date, game_id, conn)
                }
              }
            }
          } else {
            //imei无效  -->如果androidID有效，直接用androidID进行匹配
            if (!androidID.equals("")) {
              statusCode = GamePublicNewAdThirddataCommonUtils.checkAccountIsMatched(gameAccount, stmt)
              if (statusCode) {
                matched = GamePublicNewAdThirddataCommonUtils.androidRegiMatchActiveByAndroidID(androidID, regiDate29dayBefore, regi_date, game_id, conn)
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
          if (adv_name == 6 || adv_name == 3) {
            //一个设备,现在做的是:注册每次都上报，订单每次都上报
            //更新激活明细表中的matched_regi,regi_time,regi_time不能存入注册日志中的regi_time,要存入now(),获取更新数据的时间去存入
            //现在regi_time字段存入的是该设备注册最后一个帐号的注册时间
            GamePublicNewAdThirddataCommonUtils.updateRegiMatchedActive(pkgCode, regiTime, imei, conn)
          }
          val redisValue: Array[String] = GamePublicNewAdThirddataCommonUtils.getRedisValue(game_id, pkgCode, regi_date, jedis, connFx)
          val group_id = redisValue(6)
          val medium_account = redisValue(2)
          val head_people = redisValue(5)
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
            case 18 => "xinlang"
            case -1 => channel_name
          }
          //android的imei如果为空或者全是0，需要转换为"no_imei"再存储
          if (imei.equals("") || imei.matches("[0]{7,}")) {
            imei = "no_imei"
          }
          //分包备注
          var remark = ""
          if (pkgCode.contains("M")) {
            remark = GamePublicNewAdThirddataCommonUtils.getMediumRemark(pkgCode, connFx)
          } else if (pkgCode.contains("Q")) {
            remark = GamePublicNewAdThirddataCommonUtils.getChannelRemark(pkgCode, connFx)
          }


          //注册设备数: 按设备去重
          val regi_dev_num = GamePublicNewAdThirddataCommonUtils.isRegiDev(regi_date, game_id, imei, topic, jedis8)
          //新增注册设备数 : 第一次注册的设备
          val new_regi_dev_num = GamePublicNewAdThirddataCommonUtils.isNewRegiDev(regi_date, imei, pkgCode, stmt)
          //激活注册设备数 ：今天激活，今天第一次注册的设备
          var active_regi_dev_num = 0
          if (new_regi_dev_num == 1) {
            active_regi_dev_num = GamePublicNewAdThirddataCommonUtils.isActiveRegiDevNum(regi_date, imei, pkgCode, stmt)
          }

          //把匹配到的游戏ID缓存一下，在登录匹配注册的时候有用
          GamePublicNewAdThirddataCommonUtils.cacheRegiGameId(regi_date, game_id, jedis8)

          //写入广告监测平台注册明细
          GamePublicNewAdThirdDataDao.insertRegiDetail(regiTime, imei, pkgCode, adv_name, game_id, osInt, gameAccount, channel_main_id, channel_name, androidID, conn)
          //注册统计
          GamePublicNewAdThirdDataDao.insertRegiStat(regi_date, game_id, group_id, pkgCode, head_people, medium_account, adv_name, channel_main_id, channel_name, remark, regi_dev_num, new_regi_dev_num, active_regi_dev_num, conn)

        }


      })
      pool.returnBrokenResource(jedis)
      pool.returnResource(jedis8)
      pool.destroy()
      stmt.close()
      conn.close()
      connFx.close()
    })
    //    //将rdd保存到本地文件
    //    dataRegi.saveAsTextFile(ConfigurationUtil.getProperty("new_ad_regi_log_cache"))

  }

  /**
    * 登录匹配注册
    *
    * @param dataLogin
    */
  def loginMatchRegi(dataLogin: RDD[(String, String, Int, String, String)]) = {
    dataLogin.foreachPartition(iter => {
      val pool: JedisPool = JedisUtil.getJedisPool;
      val jedis8 = pool.getResource
      jedis8.select(8)
      val conn: Connection = JdbcUtil.getConn()
      val stmt = conn.createStatement()
      iter.foreach(line => {
        //game_account(3),login_time(4),game_id(8)，pkg_id(6),imei(7)
        val game_account = line._1
        val login_time = line._2
        val login_date = login_time.substring(0, 10)
        val game_id = line._3
        val imei = GamePublicNewAdThirddataCommonUtils.getImei(line._5)

        //用广告监测游戏id过滤登录日志
        if (jedis8.exists("ThirddataGameId|" + login_date + "|" + game_id)) {
          val matched = GamePublicNewAdThirddataCommonUtils.loginMatchRegiByImei(login_date, game_id, imei, stmt)
          val pkg_id = matched._1
          //新增注册设备数
          val new_regi_dev_num = matched._2
          //新增活跃设备数 : 新增注册的设备，在今天登录大于等于2次的设备数
          var new_active_dev_num = 0

          //判断这个设备是否是新增注册设备
          if (new_regi_dev_num == 1) {
            val imei_login_num = GamePublicNewAdThirddataCommonUtils.getImeiTodayLoginNum(login_date, game_id, pkg_id, imei, jedis8)
            if (imei_login_num == 2) {
              new_active_dev_num = 1
            }
          }
          GamePublicNewAdThirdDataDao.insertLoginStats(login_date, game_id, pkg_id, new_active_dev_num, conn)
        }

      })
      pool.returnResource(jedis8)
      pool.destroy()
      stmt.close()
      conn.close()
    })
  }


  /**
    * 消费匹配注册
    * 游戏账号（5），订单号（2），订单时间（6），游戏id（7）,充值流水（10），imei(24)
    *
    * @param dataOrder
    */
  def orderMatchRegi(dataOrder: RDD[(String, String, String, String, Float, String)]) = {


    dataOrder.foreachPartition(part => {
      val pool: JedisPool = JedisUtil.getJedisPool;
      val jedis = pool.getResource
      val jedis8 = pool.getResource
      jedis8.select(8)
      val conn = JdbcUtil.getConn()
      val connFx = JdbcUtil.getXiaopeng2FXConn()
      part.foreach(line => {
        val game_id = line._4.toInt
        var order_imei = GamePublicNewAdThirddataCommonUtils.getImei(line._6)
        val gameAccount = line._1
        val orderID = line._2
        val orderTime = line._3
        val order_date = orderTime.substring(0, 10)
        //过滤重复日志
        if (!jedis8.exists("ThirddataOrder|" + orderID + "|" + orderTime)) {
          //订单通过设备关联注册
          val matchedRegiImeiInfo = GamePublicNewAdThirddataCommonUtils.orderMatchedRegiByIMEI(game_id, order_imei, conn)
          //注册表中的_id
          val pkg_id = matchedRegiImeiInfo._1
          //获取设备的注册时间
          val regi_date = matchedRegiImeiInfo._2.substring(0, 10)
          val adv_name = matchedRegiImeiInfo._3
          //advName!=0 就是订单匹配到了注册
          if (adv_name != 0) {
            //广点通,今日头条订单上报
            if (adv_name == 6 || adv_name == 3) {
              //不能只根据imei和matched来匹配
              //订单匹配到注册，更新bi_ad_active_o_detail表matched_order,ordertime
              GamePublicNewAdThirddataCommonUtils.updateOrderMatchActive(pkg_id, orderTime, order_imei, conn)
            }

            val redisValue: Array[String] = GamePublicNewAdThirddataCommonUtils.getRedisValue(game_id, pkg_id, order_date, jedis, connFx)
            val medium = adv_name
            //发行信息
            val group_id = redisValue(6)
            val medium_account = redisValue(2)
            val head_people = redisValue(5)

            val os = matchedRegiImeiInfo._4
            val channel_main_id = matchedRegiImeiInfo._5
            val channel_name = matchedRegiImeiInfo._6

            //android的imei如果为空或者全是0，需要转换为"no_imei"再存储
            if (order_imei.equals("") || order_imei.matches("[0]{7,}")) {
              order_imei = "no_imei"
            }

            //分包备注
            var remark = ""
            if (pkg_id.contains("M")) {
              remark = GamePublicNewAdThirddataCommonUtils.getMediumRemark(pkg_id, connFx)
            } else if (pkg_id.contains("Q")) {
              remark = GamePublicNewAdThirddataCommonUtils.getChannelRemark(pkg_id, connFx)
            }
            //充值金额 : 当日充值总额，单位为元
            val pay_price = line._5.toInt
            //充值设备数 ： 当日充值设备数
            val pay_dev_num = GamePublicNewAdThirddataCommonUtils.isExistPayIMEI(order_date, order_imei, jedis8)
            //新增充值金额 ： 新增注册设备，在当日充值的总金额
            var new_pay_price = 0
            //新增充值设备数 ： 新增注册设备，在当日有付费的设备数
            var new_pay_dev = 0

            if (regi_date.equals(order_date)) {
              new_pay_price = pay_price
              new_pay_dev = 1
            }
            //消费明细
            GamePublicNewAdThirdDataDao.insertOrderDetail(orderID, orderTime, order_imei, pkg_id, medium, game_id, os, gameAccount, pay_price, channel_main_id, channel_name, conn)
            //消费统计
            GamePublicNewAdThirdDataDao.insertOrderStat(order_date, game_id, group_id, pkg_id, head_people, medium_account, medium, pay_price, pay_dev_num, new_pay_price, new_pay_dev, channel_main_id, channel_name, remark, conn)
          }
        }
        //redis中做订单去重，去除重复计算
        jedis8.set("ThirddataOrder|" + orderID + "|" + orderTime, "")
        jedis8.expire("ThirddataOrder|" + orderID + "|" + orderTime, 3600 * 8)

      })
      pool.returnBrokenResource(jedis)
      pool.returnResource(jedis8)
      pool.destroy()
      conn.close()
      connFx.close()
    })
  }

}
