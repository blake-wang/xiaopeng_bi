package com.xiaopeng.bi.utils

import java.sql.{Connection, PreparedStatement, ResultSet, Statement}
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import org.apache.log4j.Logger
import redis.clients.jedis.Jedis

/**
  * Created by Administrator on 2017/9/3.
  */
object CommonsThirdData {

  /**
    * 获取渠道分包的分包备注
    *
    * @param pkg_id
    * @param connFx
    */
  def getChannelRemark(pkg_id: String, connFx: Connection): String = {
    var remark = ""
    val sql = "select remark from channel_pkg where pkg_code = '" + pkg_id + "' limit 1"
    val stmt = connFx.createStatement()
    val resultSet = stmt.executeQuery(sql)
    if (resultSet.next()) {
      remark = resultSet.getString("remark")
    }
    remark

  }

  /**
    * 获取媒介分包的分包备注
    *
    * @param pkg_id
    * @param connFx
    */
  def getMediumRemark(pkg_id: String, connFx: Connection): String = {
    var remark = ""
    val sql = "select remark from medium_package where subpackage_id = '" + pkg_id + "' limit 1"
    val stmt = connFx.createStatement()
    val resultSet = stmt.executeQuery(sql)
    if (resultSet.next()) {
      remark = resultSet.getString("remark")
    }
    remark
  }


  def checkMediumClickDataStatusByIp(game_id: Int, ip: String, conn: Connection): Int = {
    //0:没有点击, 1：有点击未激活, 2：有点击并且激活
    var clickStatus = 0
    val stmt = conn.createStatement()
    val selectByIpUa = "select matched from bi_ad_momo_click where game_id='" + game_id + "' and ip='" + ip + "' order by matched limit 1"
    val resultSet = stmt.executeQuery(selectByIpUa)
    if (resultSet.next()) {
      val matched = resultSet.getInt("matched")
      if (matched == 1) {
        //点击并且激活
        clickStatus = 2
      } else {
        //点击未激活
        clickStatus = 1
      }
    }
    resultSet.close()
    stmt.close()
    return clickStatus
  }

  /**
    * 检测帐号是否已经匹配过
    *
    * @param gameAccount
    * @param stmt
    * @return
    */
  def checkAccountIsMatched(gameAccount: String, stmt: Statement): Boolean = {
    var statusCode = false
    val select_regi = "select game_account from bi_ad_regi_o_detail where game_account='" + gameAccount + "'"
    val resultSet = stmt.executeQuery(select_regi)
    if (!resultSet.next()) {
      statusCode = true
    }
    return statusCode
  }


  val logger = Logger.getLogger(this.getClass)


  /**
    * 离线任务用来存储pkgCode
    *
    * @param pkgCode
    * @param day
    * @param jedis6
    */
  def cachePkgCode(pkgCode: String, day: String, jedis6: Jedis) = {
    jedis6.hset("thirddataOffLine|" + day, day + "|" + pkgCode, pkgCode)
    jedis6.expire("thirddataOffLine|" + day, 3600 * 2)
  }

  /**
    * 通过androidID+game_id给click点击数据去重
    *
    * @param game_id
    * @param androidID
    * @param conn
    */
  def checkMediumClickDataStatusByAndroidID(game_id: Int, androidID: String, conn: Connection): Int = {
    //0:没有点击, 1：有点击未激活, 2：有点击并且激活
    var clickStatus = 0
    val stmt = conn.createStatement()
    val selectByImei = "select matched from bi_ad_momo_click where game_id='" + game_id + "' and androidID='" + androidID + "' limit 1"
    val resultSet = stmt.executeQuery(selectByImei)
    if (resultSet.next()) {
      val matched = resultSet.getInt("matched")
      if (matched == 1) {
        //点击并且激活
        clickStatus = 2
      } else {
        //点击未激活
        clickStatus = 1
      }
    }
    resultSet.close()
    stmt.close()
    return clickStatus
  }

  /**
    * 通过imei+game_id给click点击数据去重
    *
    * @param imei
    * @param game_id
    * @param conn
    */
  def checkMediumClickDataStatusByImei(imei: String, game_id: Int, conn: Connection): Int = {
    //0:没有点击, 1：有点击未激活, 2：有点击并且激活
    var clickStatus = 0
    val stmt = conn.createStatement()
    val selectByImei = "select matched from bi_ad_momo_click where game_id='" + game_id + "' and imei='" + imei + "' limit 1"
    val resultSet = stmt.executeQuery(selectByImei)
    if (resultSet.next()) {
      val matched = resultSet.getInt("matched")
      if (matched == 1) {
        //点击并且激活
        clickStatus = 2
      } else {
        //点击未激活
        clickStatus = 1
      }
    }
    resultSet.close()
    stmt.close()
    return clickStatus
  }

  /**
    * game_id + ip + devType + sysVersion
    *
    * @param game_id
    * @param ip
    * @param devType
    * @param sysVersion
    * @param conn
    */
  def checkMediumClickDataStatusByIpAndUa(game_id: Int, ip: String, devType: String, sysVersion: String, conn: Connection): Int = {
    //0:没有点击, 1：有点击未激活, 2：有点击并且激活
    var clickStatus = 0
    val stmt = conn.createStatement()
    val selectByIpUa = "select matched from bi_ad_momo_click where game_id='" + game_id + "' and ip='" + ip + "' and deviceType = '" + devType + "' and systemVersion = '" + sysVersion + "' order by matched limit 1"
    val resultSet = stmt.executeQuery(selectByIpUa)
    if (resultSet.next()) {
      val matched = resultSet.getInt("matched")
      if (matched == 1) {
        //点击并且激活
        clickStatus = 2
      } else {
        //点击未激活
        clickStatus = 1
      }
    }
    resultSet.close()
    stmt.close()
    return clickStatus


  }


  /**
    * 渠道点击设备数去重
    *
    * @param game_id
    * @param clickDate
    * @param ip
    * @param devType
    * @param sysVersion
    * @param conn
    * @return
    */
  def isTheChannelDeviceHasClicked(game_id: Int, topic: String, clickDate: String, ip: String, devType: String, sysVersion: String, conn: Connection, jedis: Jedis): Int = {

    var click_imei_num = 0
    if (jedis.exists("click|" + topic + "|" + game_id + "|" + ip + "|" + devType + "|" + sysVersion + "|" + clickDate)) {
      click_imei_num = 0
    } else {
      click_imei_num = 1
      jedis.set("click|" + topic + "|" + game_id + "|" + ip + "|" + devType + "|" + sysVersion + "|" + clickDate, topic)
      jedis.expire("click|" + topic + "|" + game_id + "|" + ip + "|" + devType + "|" + sysVersion + "|" + clickDate, 3600 * 48)
    }
    return click_imei_num
  }


  /**
    * 检查渠道来的点击数据是否已经存在
    *
    * @param game_id
    * @param ip
    * @param devType
    * @param sysVersion
    * @param conn
    */
  def checkChannelClickDataStatus(game_id: Int, ip: String, devType: String, sysVersion: String, conn: Connection): Int = {
    //0:没有记录， 1：有记录但是没有激活  2：有记录并且激活
    var clickStatus = 0
    //这里用matched排序的原因是：在同一个game_id下，有 匹配到的点击 和 没有匹配到的 ，取matched最大值
    val selectClick = "select matched from bi_ad_momo_click where game_id = '" + game_id + "' and ip = '" + ip + "' and deviceType = '" + devType + "' and systemVersion = '" + sysVersion + "' order by matched limit 1"
    val stmt = conn.createStatement()
    val resultSet = stmt.executeQuery(selectClick)
    //如果有数据，说明记录存在
    if (resultSet.next()) {
      val matched = resultSet.getInt("matched")
      if (matched == 0) {
        clickStatus = 1
      } else {
        clickStatus = 2
      }
    }
    return clickStatus
  }


  /**
    * 有新的媒介增加，就绪要更改这个方法
    * 有效设备，设备号不为00000000000000000000000000000000,000000000000000,''''(陌陌需要特殊处理5284047f4ffb4e04824a2fd1d1f0cd62）
    *
    * @param imei
    * @return
    */
  def isVadDev(imei: String, os: Int, advName: Int): Boolean = {
    var jg = true
    //ios设备 os = 2
    if (os == 2 && advName != 5 && advName != 6 && advName != 7 && advName != 8) {
      if (imei.replace("-", "").equals("00000000000000000000000000000000") || imei.replace("-", "").equals(""))
        jg = false
    } else {
      if (advName == 5 && (imei.equals("9F89C84A559F573636A47FF8DAED0D33") || imei.equals(""))) {
        //uc 00000000-0000-0000-0000-000000000000 加密后 9F89C84A559F573636A47FF8DAED0D33
        jg = false
      } else if (advName == 6 && (imei.equals("9f89c84a559f573636a47ff8daed0d33") || imei.equals(""))) {
        //guangdiantong 00000000-0000-0000-0000-000000000000 加密后 9f89c84a559f573636a47ff8daed0d33
        jg = false
      } else if (advName == 7 && (imei.equals("9F89C84A559F573636A47FF8DAED0D33") || imei.equals(""))) {
        //UC应用商店 00000000-0000-0000-0000-000000000000 加密后 9F89C84A559F573636A47FF8DAED0D33
        jg = false
      } else if (advName == 8 && (imei.equals("CD9E459EA708A948D5C2F5A6CA8838CF") || imei.equals(""))) {
        //youku 00000000000000000000000000000000 加密后 CD9E459EA708A948D5C2F5A6CA8838CF
        jg = false
      }
    }

    //android设备 os = 1
    //000000000000000  md5加密  5284047f4ffb4e04824a2fd1d1f0cd62
    if (os == 1 && (imei.equals("5284047f4ffb4e04824a2fd1d1f0cd62") || imei.equals("000000000000000") || imei.equals(""))) //陌陌、今日头条特殊处理
    {
      jg = false
    }
    return jg
  }


  /**
    * 之前只是为了过滤日志数据
    * 有广告监控的游戏才统计
    *
    * @param gameId
    * @param conn
    * @return
    */
  def isNeedStaGameId(gameId: Int, conn: Connection): Boolean = {
    var jg = false
    var stmt: PreparedStatement = null
    val sql: String = "select 1 as flag from bi_ad_momo_click where game_id=? limit 1"
    stmt = conn.prepareStatement(sql)
    stmt.setInt(1, gameId)
    val rs: ResultSet = stmt.executeQuery()
    while (rs.next) {
      if (rs.getString("flag").toInt == 1) {
        jg = true
      }
    }
    stmt.close()
    return jg

  }

  /**
    * 判断是否已经新被统计过
    *
    * @param orderDate
    * @param gameAccount
    * @return
    */
  def isExistStatNewPayAcc(orderDate: String, gameAccount: String, jedis: Jedis): Int = {
    var jg = 0
    if (jedis.exists("isExistStatNewPayAcc|" + orderDate + "|" + gameAccount))
      jg = 0
    else {
      jg = 1
      jedis.set("isExistStatNewPayAcc|" + orderDate + "|" + gameAccount, gameAccount)
      jedis.expire(gameAccount, 3600 * 48)
    }
    return jg
  }


  /**
    * 判断是否已经付费账号被统计过
    *
    * @param orderDate
    * @param gameAccount
    * @return
    */
  def isExistStatPayAcc(orderDate: String, gameAccount: String, jedis: Jedis): Int = {
    var jg = 0
    if (jedis.exists("isExistStatPayAcc|" + orderDate + "|" + gameAccount))
      jg = 0
    else {
      jg = 1
      jedis.set("isExistStatPayAcc|" + orderDate + "|" + gameAccount, gameAccount)
      jedis.expire(gameAccount, 3600 * 48)
    }
    return jg
  }


  /**
    * 获取游戏信息
    *
    * @param gameAccount
    * @param conn
    */
  def getMatchedAccountInfo(gameAccount: String, conn: Connection): (String, String, Int, Int, String, String, String, Int, String) = {
    var tp9 = Tuple9("", "", 0, 1, "", "", "", 0, "")
    var regiTime = "0000-00-00 00:00:00"
    var adName = 0
    var pkgId = ""
    var os = 1
    var ideaId = ""
    var firstLevel = ""
    var secondLevel = ""
    var channel_main_id = 0
    var channel_name = ""
    var ps: PreparedStatement = null
    val instSql = "select pkg_id,regi_time,adv_name,os,idea_id,first_level,second_level,channel_main_id,channel_name from bi_ad_regi_o_detail where game_account=? limit 1"
    ps = conn.prepareStatement(instSql)
    ps.setString(1, gameAccount)
    val rs = ps.executeQuery()
    while (rs.next()) {
      logger.info("订单匹配注册成功：" + gameAccount)
      pkgId = rs.getString("pkg_id")
      adName = rs.getInt("adv_name")
      regiTime = rs.getString("regi_time")
      os = rs.getString("os").toInt
      ideaId = rs.getString("idea_id")
      firstLevel = rs.getString("first_level")
      secondLevel = rs.getString("second_level")
      channel_main_id = rs.getInt("channel_main_id")
      channel_name = rs.getString("channel_name")
    }
    rs.close()
    ps.close()
    //这个pkgId是从注册明细表中拿来的
    tp9 = new Tuple9(pkgId, regiTime, adName, os, ideaId, firstLevel, secondLevel, channel_main_id, channel_name)
    return tp9
  }

  /**
    * 第三方广告平台imei同一处理
    *
    * @param advName
    * @param imei
    * @param osInt
    * @return
    */
  def imeiToMd5Upper(advName: Int, imei: String, osInt: Int): String = {
    var imei_md5_upper = ""
    /**
      *
      * 新增加媒介，都要修改这里的代码,根据媒介的不同，添加不同的加密方式
      * 1:momo
      * 2:baidu
      * 3:jinritoutiao
      * 4:aiqiyi
      * 5:uc
      * 6:guangdiantong
      * 7:UC应用商店
      * 8:优酷
      * 9:inmobi
      * 10:fenghuangxinwen
      * 11:baofengyingyin
      * 12:zhiyingxiao
      * 13:wangyixinwen
      * 14:xinlangfensitong
      * 15:sougou
      * 16:shenma
      * 17:dongqiudi
      * 18:xinlang
      * -1:渠道，没有imei
      */
    if (osInt == 2) {
      //ios设备
      //所有的广告日志imei，都处理成imei_md5_upper
      //加密的媒介：[uc,guangdiantong,UC应用商店,优库]   每家都要区别处理
      //未加密的媒介:[momo,baidu,jinritoutiao,aiqiyi,dongqiudi,wangyi]  要统一处理
      //特殊的媒介：百度ocpc和非ocpc发来的imei有两种，带横杠的原值和不带横杠的原值
      if (advName != 5 && advName != 6 && advName != 7 && advName != 8) {
        //这里处理的，是广告平台发的idfa是：未加密的原值。
        //未加密的原值有两种类型：带横杠的36长度的原值<E77F37A8-8346-496D-2865-C0A00E7EE0D0>，不带横杠的32长度的原值<172C01A54FBB43D381BC06EB169F7B22>
        //如果广告平台发的imei是不带横杠的原值，需要给这种原值加横杠
        if (imei_md5_upper.contains("-")) {
          //如果idfa带横杠 --> md5加密后大写
          imei_md5_upper = MD5Util.md5(imei).toUpperCase
        } else {
          //如果idfa不带横杠 --> 给idfa加横杠 --> md5加密后大写
          val imeiWithLine = CommonsThirdData.imeiPlusDash(imei.toUpperCase())
          imei_md5_upper = MD5Util.md5(imeiWithLine).toUpperCase
        }
      } else if (advName == 5) {
        //uc原始日志是：<带横杠>idfa md5加密后的大写
        imei_md5_upper = imei.toUpperCase
      } else if (advName == 6) {
        //guangdiantong原始日志是：<带横杠>idfa md5加密后的小写
        imei_md5_upper = imei.toUpperCase
      } else if (advName == 7) {
        //uc应用商店原始日志是：<带横杠>idfa md5加密后的大写
        imei_md5_upper = imei.toUpperCase
      } else if (advName == 8) {
        //youku的原始日志是： <不带横杠>idfa md5加密后大写
        imei_md5_upper = imei.toUpperCase()
      }
    } else if (osInt == 1) {
      //android设备
      //imei都是md5加密后小写:[momo,baidu,jinritoutiao,aiqiyi,uc,guangdiantong,UC应用商店,优酷]
      //imei是原始值:[dongqiudi,xinlang]
      if (advName == 17 || advName == 18) {
        imei_md5_upper = MD5Util.md5(imei).toUpperCase
      } else {
        imei_md5_upper = imei.toUpperCase
      }
    }
    //ios返回的imei_md5_upper ： []带横杠的idfa -> md5加密 -> 大写
    //android返回的imei_md5_upper ： imei -> md5加密 ->大写
    return imei_md5_upper
  }


  /**
    * 获取到原生idfa，带横线的
    *
    * @param idfa
    * @return
    */
  def imeiPlusDash(idfa: String): String = {
    var jg = idfa
    if (jg.length == 32)
      jg = jg.substring(0, 8) + "-" + jg.substring(8, 12) + "-" + jg.substring(12, 16) + "-" + jg.substring(16, 20) + "-" + jg.substring(20, 32)
    return jg
  }


  /**
    * 获取游戏OS
    *
    * @param gameId
    * @param conn
    * @return
    */
  def getOs(gameId: Int, conn: Connection): Int = {
    var jg = 1
    var stmt: PreparedStatement = null
    val sql: String = "select system_type from game_sdk where old_game_id=? limit 1"
    stmt = conn.prepareStatement(sql)
    stmt.setInt(1, gameId)
    val rs: ResultSet = stmt.executeQuery()
    while (rs.next) {
      jg = rs.getString("system_type").toInt
    }
    stmt.close()
    return jg
  }


  /**
    * 注册设备数 一天只能算一次
    *
    * @param regiDate
    * @param game_id
    * @param imei
    * @param topic
    * @param jedis
    * @return
    */
  def isRegiDev(regiDate: String, game_id: Int, imei: String, topic: String, jedis: Jedis): Int = {
    var jg = 0
    if (jedis.exists("regi|" + topic + "|" + game_id + "|" + imei + "|" + regiDate))
      jg = 0
    else {
      jg = 1
      jedis.set("regi|" + topic + "|" + game_id + "|" + imei + "|" + regiDate, topic)
      jedis.expire("regi|" + topic + "|" + game_id + "|" + imei + "|" + regiDate, 3600 * 48)
    }
    return jg

  }


  /**
    * 获取媒介账号
    *
    * @param pkgCode
    * @return
    */
  def getPubGameMedAcc(pkgCode: String, connFx: Connection): String = {
    var mediumAccount = ""
    var stmt: PreparedStatement = null
    val sql: String = " select mc.merchant as medium_account from medium_package mpk join merchant mc on mc.merchant_id=mpk.merchant_id where subpackage_id=? limit 1"
    stmt = connFx.prepareStatement(sql)
    stmt.setString(1, pkgCode)
    val rs: ResultSet = stmt.executeQuery()
    while (rs.next) {
      mediumAccount = rs.getString("medium_account")
    }
    stmt.close()
    return mediumAccount

  }

  /**
    *
    * @param pkgCode
    * @param connFx
    * @return
    */
  def getPubGameHeadPeop(pkgCode: String, connFx: Connection): String = {
    var headPeople = ""
    var stmt: PreparedStatement = null
    val sql: String = " select head_people  from (select mpk.subpackage_id as pkg_code,us.name as head_people\nfrom medium_package mpk  join `user` as us on us.id=mpk.user_id \nunion all\nSELECT pkg.pkg_code,us.name\nfrom channel_pkg pkg  join `user` as us on us.id=pkg.manager ) rs where rs.pkg_code=? limit 1"
    stmt = connFx.prepareStatement(sql)
    stmt.setString(1, pkgCode)
    val rs: ResultSet = stmt.executeQuery()
    while (rs.next) {
      headPeople = rs.getString("head_people")
    }
    stmt.close()
    return headPeople
  }

  /**
    * get 媒介账号,推广渠道,推广模式,负责人,发行组....info
    * parent_game_id,medium_account,promotion_channel,promotion_mode,head_people,os,groupid
    *
    * @param game_id
    * @param pkg_code
    * @param order_date
    * @param jedis
    * @return
    */
  def getRedisValue(game_id: Int, pkg_code: String, order_date: String, jedis: Jedis, connFx: Connection) = {
    var parent_game_id = jedis.hget(game_id.toString + "_publish_game", "mainid")
    if (parent_game_id == null) parent_game_id = "0"
    var medium_account = jedis.hget(pkg_code + "_pkgcode", "medium_account")
    if (medium_account == null || medium_account.equals("")) {
      medium_account = getPubGameMedAcc(pkg_code, connFx)
    }
    var promotion_channel = jedis.hget(pkg_code + "_pkgcode", "promotion_channel")
    if (promotion_channel == null) promotion_channel = ""
    var promotion_mode = jedis.hget(pkg_code + "_" + order_date + "_pkgcode", "promotion_mode")
    if (promotion_mode == null) promotion_mode = ""
    var head_people = jedis.hget(pkg_code + "_" + order_date + "_pkgcode", "head_people")
    if (head_people == null || head_people.equals("")) {
      head_people = getPubGameHeadPeop(pkg_code, connFx)
    }
    val os = getPubGameGroupIdAndOs(game_id, connFx)(1)
    val groupid = getPubGameGroupIdAndOs(game_id, connFx)(0)

    Array[String](parent_game_id, os, medium_account, promotion_channel, promotion_mode, head_people, groupid)
  }

  /**
    * 获取发行组&平台
    *
    * @param gameId
    * @param conn
    */
  def getPubGameGroupIdAndOs(gameId: Int, conn: Connection): Array[String] = {
    var jg = Array[String]("0", "1")
    var stmt: PreparedStatement = null
    val sql: String = " select distinct system_type os,group_id from game_sdk  where old_game_id=? limit 1"
    stmt = conn.prepareStatement(sql)
    stmt.setInt(1, gameId)
    val rs: ResultSet = stmt.executeQuery()
    while (rs.next) {
      jg = Array[String](rs.getString("group_id"), rs.getString("os"))
    }
    stmt.close()
    return jg
  }

  def androidRegiMatchActiveByImei(imei: String, regiDate29dayBefore: String, regiDate: String, gameId: Int, conn: Connection): (Int, String, String, String, String, Int, String, String) = {
    var tp8 = Tuple8(0, "", "", "", "", 0, "", "")
    var adv_name = 0
    var ideaId = ""
    var firstLevel = ""
    var secondLevel = ""
    var pkgId = ""
    var channel_main_id = 0
    var channel_name = ""
    var activeTime = ""
    var stmt: PreparedStatement = null

    val sql: String = "select adv_name,pkg_id,idea_id,first_level,second_level,channel_main_id,channel_name,active_time from bi_ad_active_o_detail where imei=? and active_time>=? and active_time<=? and game_id=? limit 1"
    stmt = conn.prepareStatement(sql)
    stmt.setString(1, imei)
    stmt.setString(2, regiDate29dayBefore)
    stmt.setString(3, regiDate + " 23:59:59")
    stmt.setInt(4, gameId)
    val rs: ResultSet = stmt.executeQuery()

    while (rs.next) {
      logger.info("android注册通过imei匹配激活成功：" + imei + " gameid:" + gameId)
      ideaId = rs.getString("idea_id")
      firstLevel = rs.getString("first_level")
      secondLevel = rs.getString("second_level")
      adv_name = rs.getInt("adv_name")
      pkgId = rs.getString("pkg_id")
      channel_main_id = rs.getInt("channel_main_id")
      channel_name = rs.getString("channel_name")
      activeTime = rs.getString("active_time")
    }
    rs.close()
    stmt.close()
    //注册匹配激活成功，取出激活明细表中的adv_name
    tp8 = new Tuple8(adv_name, ideaId, firstLevel, secondLevel, pkgId, channel_main_id, channel_name, activeTime)
    return tp8
  }

  def androidRegiMatchActiveByAndroidID(androidID: String, regiDate29dayBefore: String, regiDate: String, gameId: Int, conn: Connection): (Int, String, String, String, String, Int, String, String) = {
    var tp8 = Tuple8(0, "", "", "", "", 0, "", "")
    var adv_name = 0
    var ideaId = ""
    var firstLevel = ""
    var secondLevel = ""
    var pkgId = ""
    var channel_main_id = 0
    var channel_name = ""
    var activeTime = ""
    var stmt: PreparedStatement = null

    val sql: String = "select adv_name,pkg_id,idea_id,first_level,second_level,channel_main_id,channel_name,active_time from bi_ad_active_o_detail where androidID=? and active_time>=? and active_time<=? and game_id=? limit 1"
    stmt = conn.prepareStatement(sql)
    stmt.setString(1, androidID)
    stmt.setString(2, regiDate29dayBefore)
    stmt.setString(3, regiDate + " 23:59:59")
    stmt.setInt(4, gameId)
    val rs: ResultSet = stmt.executeQuery()

    while (rs.next) {
      logger.info("android注册通过androidID匹配激活成功：" + androidID + " gameid:" + gameId)
      ideaId = rs.getString("idea_id")
      firstLevel = rs.getString("first_level")
      secondLevel = rs.getString("second_level")
      adv_name = rs.getInt("adv_name")
      pkgId = rs.getString("pkg_id")
      channel_main_id = rs.getInt("channel_main_id")
      channel_name = rs.getString("channel_name")
      activeTime = rs.getString("active_time")
    }
    rs.close()
    stmt.close()
    tp8 = new Tuple8(adv_name, ideaId, firstLevel, secondLevel, pkgId, channel_main_id, channel_name, activeTime)
    return tp8
  }


  def iosRegiMatchActive(imei: String, regiDate29dayBefore: String, regiDate: String, gameId: Int, conn: Connection): (Int, String, String, String, String, Int, String, String) = {
    var tp8 = Tuple8(0, "", "", "", "", 0, "", "")
    var adv_name = 0
    var ideaId = ""
    var firstLevel = ""
    var secondLevel = ""
    var pkgId = ""
    var channel_main_id = 0
    var channel_name = ""
    var activeTime = ""
    var stmt: PreparedStatement = null

    val sql: String = "select adv_name,pkg_id,idea_id,first_level,second_level,channel_main_id,channel_name,active_time from bi_ad_active_o_detail where imei=? and active_time>=? and active_time<=? and game_id=? limit 1"
    stmt = conn.prepareStatement(sql)
    stmt.setString(1, imei)
    stmt.setString(2, regiDate29dayBefore)
    stmt.setString(3, regiDate + " 23:59:59")
    stmt.setInt(4, gameId)
    val rs: ResultSet = stmt.executeQuery()

    while (rs.next) {
      logger.info("ios注册通过imei匹配激活成功：" + imei + " gameid:" + gameId)
      ideaId = rs.getString("idea_id")
      firstLevel = rs.getString("first_level")
      secondLevel = rs.getString("second_level")
      adv_name = rs.getString("adv_name").toInt
      pkgId = rs.getString("pkg_id")
      channel_main_id = rs.getInt("channel_main_id")
      channel_name = rs.getString("channel_name")
      activeTime = rs.getString("active_time")
    }
    rs.close()
    stmt.close()
    //注册匹配激活成功，取出激活明细表中的adv_name
    tp8 = new Tuple8(adv_name, ideaId, firstLevel, secondLevel, pkgId, channel_main_id, channel_name, activeTime)
    return tp8
  }


  /**
    * 注册匹配激活
    *
    * @param imei
    * @param regiDate29dayBefore
    * @param regiDate
    * @param conn
    */
  def regiMatchActive(imei: String, regiDate29dayBefore: String, regiDate: String, gameId: Int, osInt: Int, conn: Connection): (Int, String, String, String, String, Int, String, String) = {
    var tp8 = Tuple8(0, "", "", "", "", 0, "", "")
    var adv_name = 0
    var ideaId = ""
    var firstLevel = ""
    var secondLevel = ""
    var pkgId = ""
    var channel_main_id = 0
    var channel_name = ""
    var activeTime = ""
    var stmt: PreparedStatement = null

    val sql: String = "select adv_name,pkg_id,idea_id,first_level,second_level,channel_main_id,channel_name,active_time from bi_ad_active_o_detail where imei=? and active_time>=? and active_time<=? and game_id=? limit 1"
    stmt = conn.prepareStatement(sql)
    stmt.setString(1, imei)
    stmt.setString(2, regiDate29dayBefore)
    stmt.setString(3, regiDate + " 23:59:59")
    stmt.setInt(4, gameId)
    val rs: ResultSet = stmt.executeQuery()

    while (rs.next) {
      logger.info("注册匹配激活成功：" + imei + " gameid:" + gameId)
      ideaId = rs.getString("idea_id")
      firstLevel = rs.getString("first_level")
      secondLevel = rs.getString("second_level")
      adv_name = rs.getString("adv_name").toInt
      pkgId = rs.getString("pkg_id")
      channel_main_id = rs.getInt("channel_main_id")
      channel_name = rs.getString("channel_name")
      activeTime = rs.getString("active_time")
    }
    rs.close()
    stmt.close()
    //注册匹配激活成功，取出激活明细表中的adv_name
    tp8 = new Tuple8(adv_name, ideaId, firstLevel, secondLevel, pkgId, channel_main_id, channel_name, activeTime)
    return tp8
  }

  /**
    * 注册匹配到激活，更新激活明细表中matched_regi,regi_time
    *
    * @param regiTime
    * @param imei
    * @param conn
    */
  def updateRegiMatchedActive(pkgCode: String, regiTime: String, imei: String, conn: Connection) = {

    val sql = "update bi_ad_active_o_detail set matched_regi=1,regi_time=now() where imei = '" + imei + "' and pkg_id = '" + pkgCode + "' and matched_regi = 0"
    val pstat = conn.createStatement()
    pstat.executeUpdate(sql)
    pstat.close()

  }

  /**
    * 订单匹配到激活，更新激活明细表中matched_order,order_time
    *
    * @param orderTime
    * @param imei
    * @param conn
    * @return
    */
  def updateOrderMatchActive(pkgCode: String, orderTime: String, imei: String, conn: Connection) = {
    //    val sql = "update bi_ad_active_o_detail set matched_order=1,order_time = '" + orderTime + "' where pkg_id = '" + pkgCode + "' and imei = '" + imei + "' and matched_order = 0"
    val sql = "update bi_ad_active_o_detail set matched_order=1,order_time=now() where imei = '" + imei + "' and pkg_id = '" + pkgCode + "' and matched_order = 0"
    val pstat = conn.createStatement()
    pstat.executeUpdate(sql)
    pstat.close()
  }


  /**
    * 单击设备数，一天只算一次
    *
    * @param clickDate
    * @param game_id
    * @param imei
    * @param topic
    * @param jedis
    * @return
    */
  def isTheMediumDeviceHasClicked(clickDate: String, game_id: Int, imei: String, topic: String, ip: String, devType: String, sysVersion: String, androidID: String, jedis: Jedis): Int = {
    var jg = 0
    //分别用imei,androidID,IP+UA做点击设备的去重
    if (!imei.equals("") && !imei.equals("00000000-0000-0000-0000-000000000000") && !imei.equals("medium_no_imei")) {
      if (jedis.exists("click|" + topic + "|" + game_id + "|" + imei + "|" + clickDate))
        jg = 0
      else {
        jg = 1
        jedis.set("click|" + topic + "|" + game_id + "|" + imei + "|" + clickDate, topic)
        jedis.expire("click|" + topic + "|" + game_id + "|" + imei + "|" + clickDate, 3600 * 48)
      }
    } else {
      if (!androidID.equals("") && !androidID.equals("medium_no_androidID")) {
        if (jedis.exists("click|" + topic + "|" + game_id + "|" + androidID + "|" + clickDate)) {
          jg = 0
        } else {
          jg = 1
          jedis.set("click|" + topic + "|" + game_id + "|" + androidID + "|" + clickDate, topic)
          jedis.expire("click|" + topic + "|" + game_id + "|" + androidID + "|" + clickDate, 3600 * 48)
        }
      } else if (!ip.equals("")) {
        if (jedis.exists("click|" + topic + "|" + game_id + "|" + ip + "|" + devType + "|" + sysVersion + "|" + clickDate)) {
          jg = 0
        } else {
          jg = 1
          jedis.set("click|" + topic + "|" + game_id + "|" + ip + "|" + devType + "|" + sysVersion + "|" + clickDate, topic)
          jedis.expire("click|" + topic + "|" + game_id + "|" + ip + "|" + devType + "|" + sysVersion + "|" + clickDate, 3600 * 48)
        }
      }
    }
    return jg
  }


  /**
    * 获取游戏ID
    *
    * @param pkgCode
    * @return
    */
  def getGameId(pkgCode: String, connFx: Connection): Int = {
    var jg = 0
    var stmt: PreparedStatement = null
    val sql: String = "select game_id from (select subpackage_id pkg_code,game_id from medium_package union all select pkg_code,game_id from channel_pkg ) rs where pkg_code=? limit 1"
    stmt = connFx.prepareStatement(sql)
    stmt.setString(1, pkgCode)
    val rs: ResultSet = stmt.executeQuery()
    while (rs.next) {
      jg = rs.getString("game_id").toInt
    }
    stmt.close()
    return jg
  }


  /**
    * 获取imei
    *
    * @param device_imei
    * @return
    */
  def getImei(device_imei: String): String = {
    var imei = ""
    if (device_imei.contains("&")) {
      //android设备
      val fields = device_imei.split("&", -1)
      if (fields.length == 3) {
        //&&
        //24ee9aff51efd6a3&88:6a:b1:fc:01:f7
        //221132093280616&64110abc12e796d5&f8:2f:48:a1:6d:20
        imei = fields(0)

      }
    } else {
      if (device_imei.length >= 36) {
        //h5游戏 XMgH5Sdk73a411aa7e249846be2e2ae51513597154727 ,只取前36位
        imei = device_imei.substring(0, 36)
      } else {
        //苹果设备 4CB796F3-009A-4E6C-8527-6ADA2D395151
        imei = device_imei
      }
    }
    return imei
  }

  /**
    * 取出激活日志中的androidID
    *
    * @param imei_androidid_mac
    * @return
    */
  def getAndroidID(imei_androidid_mac: String): String = {
    var androidID = ""
    if (imei_androidid_mac.contains("&")) {
      val fields = imei_androidid_mac.split("&", -1)
      if (fields.length == 3) {
        //&&
        //221132093280616&64110abc12e796d5&f8:2f:48:a1:6d:20
        androidID = fields(1)
      } else if (fields.length == 2) {
        //24ee9aff51efd6a3&88:6a:b1:fc:01:f7
        androidID = fields(0)
      }
    }
    //如果是android设备，返回androidID。如果是ios设备，返回""
    return androidID
  }


  /**
    * 获取传入参数与当天的时间差
    *
    * @param pidt
    */
  def getDateBeforeParams(pidt: String, i: Int): String = {
    val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val cal: Calendar = Calendar.getInstance
    val date: Date = dateFormat.parse(pidt)
    cal.setTime(date)
    cal.add(Calendar.DATE, i)
    val dt = dateFormat.format(cal.getTime())
    return dt
  }


  /**
    * 查询设备是否有点击
    *
    * @param pkgCode
    * @param imei
    * @param conn
    */
  def checkMediumClickDataStatus(pkgCode: String, imei: String, ip: String, devType: String, sysVersion: String, conn: Connection): Int = {
    //0:没有点击, 1：有点击未激活, 2：有点击并且激活
    var clickStatus = 0
    val stmt = conn.createStatement()

    //先判断imei是否有效，如果无效就用ip+ua匹配
    //匹配规则为 imei > ip+ua > ip
    if (!imei.equals("") && !imei.equals("00000000-0000-0000-0000-000000000000")) {
      val selectByImei = "select matched from bi_ad_momo_click where pkg_id='" + pkgCode + "' and imei='" + imei + "' limit 1"
      val resultSet = stmt.executeQuery(selectByImei)
      if (resultSet.next()) {
        val matched = resultSet.getInt("matched")
        if (matched == 1) {
          //点击并且激活
          clickStatus = 2
        } else {
          //点击未激活
          clickStatus = 1
        }
      }
      resultSet.close()
    } else {
      val selectByUA = "select matched from bi_ad_momo_click where pkg_id='" + pkgCode + "' and ip = '" + ip + "' and deviceType='" + devType + "' and systemVersion='" + sysVersion + "' limit 1"
      val resultSet = stmt.executeQuery(selectByUA)
      if (resultSet.next()) {
        val matched = resultSet.getInt("matched")
        if (matched == 1) {
          //点击并且激活
          clickStatus = 2
        } else {
          //点击未激活
          clickStatus = 1
        }
      }
      resultSet.close()
    }
    stmt.close()
    return clickStatus
  }

}
