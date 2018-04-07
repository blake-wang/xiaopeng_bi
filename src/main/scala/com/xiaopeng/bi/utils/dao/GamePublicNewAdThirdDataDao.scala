package com.xiaopeng.bi.utils.dao

import java.sql.{Connection, PreparedStatement, Statement}

import org.apache.log4j.Logger

/**
  * Created by bigdata on 18-3-12.
  */
object GamePublicNewAdThirdDataDao {
  /**
    * 插入登录信息到统计表
    *
    * @param login_date
    * @param game_id
    * @param pkg_id
    * @param new_active_dev_num
    * @param conn
    */
  def insertLoginStats(login_date: String, game_id: Int, pkg_id: String, new_active_dev_num: Int, conn: Connection) = {
    val insert_sql = "insert into bi_new_merge_ad_kpi (publish_date,child_game_id,pkg_id,new_active_dev_num) values (?,?,?,?) on duplicate key update new_active_dev_num=new_active_dev_num+values(new_active_dev_num)"
    val pstmt = conn.prepareStatement(insert_sql)
    pstmt.setObject(1, login_date)
    pstmt.setObject(2, game_id)
    pstmt.setObject(3, pkg_id)
    pstmt.setObject(4, new_active_dev_num)
    pstmt.executeUpdate()
    pstmt.close()
  }

  /**
    * android设备通过 game_id + androidID来检测是否激活匹配
    *
    * @param game_id
    * @param androidID
    * @param conn
    * @param stmt
    * @return
    */
  def checkAndroidIDIsMatched(game_id: Int, androidID: String, conn: Connection, stmt: Statement): Boolean = {
    var statusCode = false;
    val selectImei = "select imei from bi_new_merge_ad_active_detail where game_id='" + game_id + "' and androidID='" + androidID + "' limit 1"
    val resultSet = stmt.executeQuery(selectImei)
    if (!resultSet.next()) {
      //激活明细表不存在game_id + androidID记录,允许匹配操作
      statusCode = true
    }
    return statusCode;
  }

  /**
    * ios设备和android设备通过 game_id + imei 来检测是否激活匹配
    *
    * @param game_id
    * @param imei
    * @param conn
    * @param stmt
    * @return
    */
  def checkImeiIsMatched(game_id: Int, imei: String, conn: Connection, stmt: Statement): Boolean = {
    var statusCode = false;
    val selectImei = "select imei from bi_new_merge_ad_active_detail where game_id='" + game_id + "' and imei='" + imei + "' limit 1"
    val resultSet = stmt.executeQuery(selectImei)
    if (!resultSet.next()) {
      //激活明细表不存在game_id + imei记录,允许匹配操作
      statusCode = true
    }
    return statusCode;
  }


  def updateMediumClickByIp(pkg_id: String, game_id: Int, ip: String, clickTime: String, callback: String, conn: Connection) = {
    val instSql = "update bi_new_merge_ad_click_detail set ts=?,callback=?,pkg_id=? where game_id=? and ip=? and matched=0"
    val ps: PreparedStatement = conn.prepareStatement(instSql)
    ps.setString(1, clickTime)
    ps.setString(2, callback)
    ps.setString(3, pkg_id)
    ps.setInt(4, game_id)
    ps.setString(5, ip)
    ps.executeUpdate()
    ps.close();
  }


  val logger = Logger.getLogger(this.getClass)

  /**
    * 通过game_id + androidID 更新已经存在的媒介点击明细
    *
    * @param pkg_id
    * @param game_id
    * @param androidID
    * @param clickTime
    * @param callback
    * @param conn
    */
  def updateMediumClickByAndroidID(pkg_id: String, game_id: Int, androidID: String, clickTime: String, callback: String, conn: Connection) = {
    val instSql = "update bi_new_merge_ad_click_detail set ts=?,callback=?,pkg_id=? where game_id=? and androidID=? and matched=0"
    val ps: PreparedStatement = conn.prepareStatement(instSql)
    ps.setString(1, clickTime)
    ps.setString(2, callback)
    ps.setString(3, pkg_id)
    ps.setInt(4, game_id)
    ps.setString(5, androidID)
    ps.executeUpdate()
    ps.close();
  }

  /**
    * 更新已经存在的渠道点击明细
    *
    * @param pkg_id
    * @param clickTime
    * @param ip
    * @param devType
    * @param sysVersion
    * @param conn
    */
  def updateChannelDataClickDetail(pkg_id: String, game_id: Int, clickTime: String, ip: String, devType: String, sysVersion: String, callback: String, conn: Connection) = {
    val updateSql = "update bi_new_merge_ad_click_detail set ts=?,callback=?,pkg_id=? where game_id=? and ip=? and deviceType=? and systemVersion=? and matched=0"
    val pstmt = conn.prepareStatement(updateSql)
    pstmt.setString(1, clickTime)
    pstmt.setString(2, callback)
    pstmt.setString(3, pkg_id)
    pstmt.setInt(4, game_id)
    pstmt.setString(5, ip)
    pstmt.setString(6, devType)
    pstmt.setString(7, sysVersion)
    pstmt.executeUpdate()
    pstmt.close()
  }


  def updateMediumClickByIpAndUa(pkg_id: String, game_id: Int, ip: String, devType: String, sysVersion: String, clickTime: String, callback: String, conn: Connection) = {
    val instSql = "update bi_new_merge_ad_click_detail set ts=?,callback=?,pkg_id=? where game_id=? and ip=? and deviceType=? and systemVersion=? and matched=0"
    val ps: PreparedStatement = conn.prepareStatement(instSql)
    ps.setString(1, clickTime)
    ps.setString(2, callback)
    ps.setString(3, pkg_id)
    ps.setInt(4, game_id)
    ps.setString(5, ip)
    ps.setString(6, devType)
    ps.setString(7, sysVersion)
    ps.executeUpdate()
    ps.close();
  }


  /**
    * 把订单明细写入到订单详情表中
    *
    * @param orderTime
    * @param imei
    * @param pkgCode
    * @param medium
    * @param gameId
    * @param os
    * @param gameAccount
    * @param payPrice
    * @param conn
    */
  def insertOrderDetail(orderID: String, orderTime: String, imei: String, pkgCode: String, medium: Int, gameId: Int, os: Int, gameAccount: String, payPrice: Float, channel_main_id: Int, channel_name: String, conn: Connection) = {
    val orderSql = "insert into bi_new_merge_ad_order_detail (order_id,pkg_id,game_id,imei,os,order_time,adv_name,game_account,pay_price,channel_main_id,channel_name) values (?,?,?,?,?,?,?,?,?,?,?) "
    val ps: PreparedStatement = conn.prepareStatement(orderSql)
    //insert
    ps.setString(1, orderID)
    ps.setString(2, pkgCode)
    ps.setInt(3, gameId)
    ps.setString(4, imei)
    ps.setInt(5, os)
    ps.setString(6, orderTime)
    ps.setInt(7, medium)
    ps.setString(8, gameAccount)
    ps.setFloat(9, payPrice)
    ps.setInt(10, channel_main_id)
    ps.setString(11, channel_name)
    ps.executeUpdate()
    ps.close()
  }

  def insertOrderDetailOffLine(orderID: String, orderTime: String, imei: String, pkgCode: String, medium: Int, gameId: Int, os: Int, gameAccount: String, payPrice: Float, channel_main_id: Int, channel_name: String, conn: Connection) = {
    val orderSql = "insert into bi_new_merge_ad_order_detail (order_id,pkg_id,game_id,imei,os,order_time,adv_name,game_account,pay_price,channel_main_id,channel_name,create_time,update_time) values (?,?,?,?,?,?,?,?,?,?,?,?,?) "
    val ps: PreparedStatement = conn.prepareStatement(orderSql)
    //insert
    ps.setString(1, orderID)
    ps.setString(2, pkgCode)
    ps.setInt(3, gameId)
    ps.setString(4, imei)
    ps.setInt(5, os)
    ps.setString(6, orderTime)
    ps.setInt(7, medium)
    ps.setString(8, gameAccount)
    ps.setFloat(9, payPrice)
    ps.setInt(10, channel_main_id)
    ps.setString(11, channel_name)
    ps.setString(12, "0000-00-00 00:00:00")
    ps.setString(13, "0000-00-00 00:00:00")
    ps.executeUpdate()
    ps.close()
  }


  /**
    * 插入订单数据
    *
    * @param orderDate
    * @param gameId
    * @param group_id
    * @param pkgCode
    * @param head_people
    * @param medium_account
    * @param medium
    * @param idea_id
    * @param first_level
    * @param second_level
    * @param pa_price
    * @param pay_dev_num
    * @param new_pay_price
    * @param new_pay_device
    * @param conn
    */
  def insertOrderStat(orderDate: String, gameId: Int, group_id: String, pkgCode: String, head_people: String, medium_account: String, medium: Int,
                      pa_price: Int, pay_dev_num: Int, new_pay_price: Int, new_pay_device: Int, channel_main_id: Int, channel_name: String, remark: String, conn: Connection) = {

    val sql2Mysql = "insert into bi_new_merge_ad_kpi" +
      "(publish_date,child_game_id,group_id,pkg_id,head_people,medium_account,medium,pay_price,pay_dev_num,new_pay_price,new_pay_dev_num,channel_main_id,channel_name,remark)" +
      " values(?,?,?,?,?,?,?,?,?,?,?,?,?,?) " +
      " on duplicate key update pay_price=pay_price+values(pay_price),pay_dev_num=pay_dev_num+values(pay_dev_num),new_pay_price=new_pay_price+values(new_pay_price),new_pay_dev_num=new_pay_dev_num+values(new_pay_dev_num),head_people=values(head_people),medium_account=values(medium_account),remark=values(remark)"
    val ps: PreparedStatement = conn.prepareStatement(sql2Mysql)
    /*insert*/
    ps.setString(1, orderDate)
    ps.setInt(2, gameId)
    ps.setString(3, group_id)
    ps.setString(4, pkgCode)
    ps.setString(5, head_people)
    ps.setString(6, medium_account)
    ps.setInt(7, medium)
    ps.setFloat(8, pa_price)
    ps.setInt(9, pay_dev_num)
    ps.setFloat(10, new_pay_price)
    ps.setInt(11, new_pay_device)
    ps.setInt(12, channel_main_id)
    ps.setString(13, channel_name)
    ps.setString(14, remark)

    ps.executeUpdate()
    ps.close()

  }

  /**
    * 把注册明细写入到注册明细表中
    *
    * @param regiTime
    * @param imei
    * @param pkgCode
    * @param medium
    * @param gameId
    * @param os
    * @param conn
    */
  def insertRegiDetail(regiTime: String, imei: String, pkgCode: String, medium: Int, gameId: Int, os: Int, gameAccount: String, channel_main_id: Int, channel_name: String, androidID: String, conn: Connection) = {
    val instSql = "insert into bi_new_merge_ad_regi_detail(pkg_id,game_id,imei,os,regi_time,adv_name,game_account,channel_main_id,channel_name,androidID) values(?,?,?,?,?,?,?,?,?,?)"
    val ps: PreparedStatement = conn.prepareStatement(instSql)
    //insert
    ps.setString(1, pkgCode)
    ps.setInt(2, gameId)
    ps.setString(3, imei)
    ps.setInt(4, os)
    ps.setString(5, regiTime)
    ps.setInt(6, medium)
    ps.setString(7, gameAccount)
    ps.setInt(8, channel_main_id)
    ps.setString(9, channel_name)
    ps.setString(10, androidID)
    ps.executeUpdate()
    ps.close()
  }

  def insertRegiDetailOffLine(regiTime: String, imei: String, pkgCode: String, medium: Int, gameId: Int, os: Int, gameAccount: String, channel_main_id: Int, channel_name: String, androidID: String, conn: Connection) = {
    val instSql = "insert into bi_new_merge_ad_regi_detail(pkg_id,game_id,imei,os,regi_time,adv_name,game_account,channel_main_id,channel_name,androidID,update_time) values(?,?,?,?,?,?,?,?,?,?,?)"
    val ps: PreparedStatement = conn.prepareStatement(instSql)
    //insert
    ps.setString(1, pkgCode)
    ps.setInt(2, gameId)
    ps.setString(3, imei)
    ps.setInt(4, os)
    ps.setString(5, regiTime)
    ps.setInt(6, medium)
    ps.setString(7, gameAccount)
    ps.setInt(8, channel_main_id)
    ps.setString(9, channel_name)
    ps.setString(10, androidID)
    ps.setString(11, "0000-00-00 00:00:00")
    ps.executeUpdate()
    ps.close()
  }


  /**
    * 把激活明细写入到激活明细表中
    *
    * @param activeTime
    * @param imei
    * @param pkgCode
    * @param adv_name
    * @param gameId
    * @param os
    * @param conn
    */
  def insertActiveDetail(activeTime: String, imei: String, pkgCode: String, adv_name: Int, gameId: Int, os: Int, channel_main_id: Int, channel_name: String, ip: String, devType: String, sysVersion: String, androidID: String, conn: Connection) = {
    val instSql = "insert into bi_new_merge_ad_active_detail(pkg_id,game_id,imei,os,active_time,adv_name,channel_main_id,channel_name,ip,deviceType,systemVersion,androidID) values(?,?,?,?,?,?,?,?,?,?,?,?)"
    val ps: PreparedStatement = conn.prepareStatement(instSql)
    //insert
    ps.setString(1, pkgCode)
    ps.setInt(2, gameId)
    ps.setString(3, imei)
    ps.setInt(4, os)
    ps.setString(5, activeTime)
    ps.setInt(6, adv_name)
    ps.setInt(7, channel_main_id)
    ps.setString(8, channel_name)
    ps.setString(9, ip)
    ps.setString(10, devType)
    ps.setString(11, sysVersion)
    ps.setString(12, androidID)
    ps.executeUpdate()
    ps.close()

  }


  /**
    * 注册账号数和注册设备数统计
    *
    * @param regiDate
    * @param gameId
    * @param group_id
    * @param pkgCode
    * @param head_people
    * @param medium_account
    * @param medium
    * @param regi_dev_num
    * @param new_regi_dev_num
    * @param conn
    */
  def insertRegiStat(regiDate: String, gameId: Int, group_id: String, pkgCode: String, head_people: String, medium_account: String, medium: Int,
                     channel_main_id: Int, channel_name: String, remark: String, regi_dev_num: Int, new_regi_dev_num: Int, active_regi_dev_num: Int, conn: Connection) = {

    val sql2Mysql = "insert into bi_new_merge_ad_kpi" +
      "(publish_date,child_game_id,group_id,pkg_id,head_people,medium_account,medium,channel_main_id,channel_name,remark,regi_dev_num,new_regi_dev_num,active_regi_dev_num)" +
      " values(?,?,?,?,?,?,?,?,?,?,?,?,?) " +
      " on duplicate key update group_id=values(group_id),head_people=values(head_people),medium_account=values(medium_account),remark=values(remark),regi_dev_num=regi_dev_num+values(regi_dev_num),new_regi_dev_num=new_regi_dev_num+values(new_regi_dev_num),active_regi_dev_num=active_regi_dev_num+values(active_regi_dev_num)"
    val ps: PreparedStatement = conn.prepareStatement(sql2Mysql)
    /*insert*/
    ps.setString(1, regiDate)
    ps.setInt(2, gameId)
    ps.setString(3, group_id)
    ps.setString(4, pkgCode)
    ps.setString(5, head_people)
    ps.setString(6, medium_account)
    ps.setInt(7, medium)
    ps.setInt(8, channel_main_id)
    ps.setString(9, channel_name)
    ps.setString(10, remark)
    ps.setInt(11, regi_dev_num)
    ps.setInt(12, new_regi_dev_num)
    ps.setInt(13, active_regi_dev_num)


    ps.executeUpdate()
    ps.close()

  }


  /**
    * 统计激活数
    *
    * @param activeDate
    * @param gameId
    * @param group_id
    * @param pkgCode
    * @param head_people
    * @param medium_account
    * @param medium
    * @param active_ad_num
    * @param conn
    */
  def insertActiveStat(activeDate: String, gameId: Int, group_id: String, pkgCode: String, head_people: String, medium_account: String, medium: Int, active_ad_num: Int, channel_main_id: Int, channel_name: String, remark: String, conn: Connection) = {

    val sql2Mysql = "insert into bi_new_merge_ad_kpi" +
      "(publish_date,child_game_id,group_id,pkg_id,head_people,medium_account,medium,active_ad_num,channel_main_id,channel_name,remark)" +
      " values(?,?,?,?,?,?,?,?,?,?,?) " +
      " on duplicate key update active_ad_num=active_ad_num+values(active_ad_num),head_people=values(head_people),medium_account=values(medium_account),remark=values(remark)"
    val ps: PreparedStatement = conn.prepareStatement(sql2Mysql)
    /*insert*/
    ps.setString(1, activeDate)
    ps.setInt(2, gameId)
    ps.setString(3, group_id)
    ps.setString(4, pkgCode)
    ps.setString(5, head_people)
    ps.setString(6, medium_account)
    ps.setInt(7, medium)
    ps.setInt(8, active_ad_num)
    ps.setInt(9, channel_main_id)
    ps.setString(10, channel_name)
    ps.setString(11, remark)


    ps.executeUpdate()
    ps.close()


  }


  /**
    * 单击统计
    *
    * @param clickDate
    * @param gameId
    * @param group_id
    * @param pkgCode
    * @param head_people
    * @param medium_account
    * @param medium
    * @param clicks
    * @param clickDevs
    */
  def insertClickStat(clickDate: String, gameId: Int, group_id: String, pkgCode: String, head_people: String, medium_account: String, medium: Int, channel_main_id: Int, channel_name: String,
                      clicks: Int, clickDevs: Int, remark: String, conn: Connection) = {

    val sql2Mysql = "insert into bi_new_merge_ad_kpi" +
      "(publish_date,child_game_id,group_id,pkg_id,head_people,medium_account,medium,channel_main_id,channel_name,click_num,click_dev_num,remark)" +
      " values(?,?,?,?,?,?,?,?,?,?,?,?) " +
      " on duplicate key update click_num=click_num+values(click_num),click_dev_num=click_dev_num+values(click_dev_num),head_people=values(head_people),medium_account=values(medium_account),channel_main_id=values(channel_main_id),channel_name=values(channel_name),remark=values(remark)"
    val ps: PreparedStatement = conn.prepareStatement(sql2Mysql)
    /*insert*/
    ps.setString(1, clickDate)
    ps.setInt(2, gameId)
    ps.setString(3, group_id)
    ps.setString(4, pkgCode)
    ps.setString(5, head_people)
    ps.setString(6, medium_account)
    ps.setInt(7, medium)
    ps.setInt(8, channel_main_id)
    ps.setString(9, channel_name)
    ps.setInt(10, clicks)
    ps.setInt(11, clickDevs)
    ps.setString(12, remark)


    ps.executeUpdate()
    ps.close()


  }


  /**
    * IOS通过 game_id + imei 进行激活匹配
    *
    * @param imeiWithLine_md5_upper
    * @param activeDate6dayBefore
    * @param activeDate
    */
  def iosMatchClickByImei(imei: String, imeiWithLine_md5_upper: String, imei_md5_upper: String, gameId: Int, activeDate6dayBefore: String, activeDate: String, conn: Connection): Tuple10[Int, String, String, String, Int, String, String, String, Int, String] = {
    var tp10 = Tuple10(0, "", "", "", 0, "", "", "", 0, "")
    var pkg_id = ""
    var adv_name = 0
    var ideaId = ""
    var firstLevel = ""
    var secondLevel = ""
    var channel_main_id = 0
    var channel_name = ""
    var jg = 0
    val update_sql = "update bi_new_merge_ad_click_detail set matched=1,active_time=now() where game_id=? and ts>=? and ts<=? and matched=0 and (imei_md5_upper=? or imei_md5_upper=?) limit 1"
    var ps: PreparedStatement = conn.prepareStatement(update_sql)
    ps.setInt(1, gameId)
    ps.setString(2, activeDate6dayBefore)
    ps.setString(3, activeDate + " 23:59:59")
    ps.setString(4, imeiWithLine_md5_upper)
    ps.setString(5, imei_md5_upper)
    jg = ps.executeUpdate()
    //jg是更新的数据的行数
    if (jg > 0) {
      logger.info("ios激活通过imei匹配成功 : " + imei_md5_upper + ", gameid:" + gameId)
      val slSql = "select pkg_id,adv_name,idea_id,first_level,second_level,channel_main_id,channel_name from bi_new_merge_ad_click_detail  where game_id=? and ts>=? and ts<=? and matched=1 and (imei_md5_upper=? or imei_md5_upper=?) limit 1"
      ps = conn.prepareStatement(slSql);
      ps.setInt(1, gameId)
      ps.setString(2, activeDate6dayBefore)
      ps.setString(3, activeDate + " 23:59:59")
      ps.setString(4, imeiWithLine_md5_upper)
      ps.setString(5, imei_md5_upper)
      val rs = ps.executeQuery()
      while (rs.next()) {
        //取出广告日志中的pkgCode
        pkg_id = rs.getString("pkg_id")
        adv_name = rs.getInt("adv_name")
        ideaId = rs.getString("idea_id")
        firstLevel = rs.getString("first_level")
        secondLevel = rs.getString("second_level")
        channel_main_id = rs.getInt("channel_main_id")
        channel_name = rs.getString("channel_name")
      }
      rs.close()
    }
    //关闭ps
    if (ps != null) {
      ps.close();
    }
    tp10 = new Tuple10(jg, pkg_id, "", "", adv_name, ideaId, firstLevel, secondLevel, channel_main_id, channel_name)
    return tp10
  }

  def iosMatchClickByImeiOffLine(imei: String, imeiWithLine_md5_upper: String, imei_md5_upper: String, gameId: Int, activeDate6dayBefore: String, activeDate: String, conn: Connection): Tuple10[Int, String, String, String, Int, String, String, String, Int, String] = {
    var tp10 = Tuple10(0, "", "", "", 0, "", "", "", 0, "")
    var pkg_id = ""
    var adv_name = 0
    var ideaId = ""
    var firstLevel = ""
    var secondLevel = ""
    var channel_main_id = 0
    var channel_name = ""
    var jg = 0
    val update_sql = "update bi_new_merge_ad_click_detail set matched=1 where game_id=? and ts>=? and ts<=? and matched=0 and (imei_md5_upper=? or imei_md5_upper=?) limit 1"
    var ps: PreparedStatement = conn.prepareStatement(update_sql)
    ps.setInt(1, gameId)
    ps.setString(2, activeDate6dayBefore)
    ps.setString(3, activeDate + " 23:59:59")
    ps.setString(4, imeiWithLine_md5_upper)
    ps.setString(5, imei_md5_upper)
    jg = ps.executeUpdate()
    //jg是更新的数据的行数
    if (jg > 0) {
      logger.info("ios激活通过imei匹配成功 : " + imei_md5_upper + ", gameid:" + gameId)
      val slSql = "select pkg_id,adv_name,idea_id,first_level,second_level,channel_main_id,channel_name from bi_new_merge_ad_click_detail  where game_id=? and ts>=? and ts<=? and matched=1 and (imei_md5_upper=? or imei_md5_upper=?) limit 1"
      ps = conn.prepareStatement(slSql);
      ps.setInt(1, gameId)
      ps.setString(2, activeDate6dayBefore)
      ps.setString(3, activeDate + " 23:59:59")
      ps.setString(4, imeiWithLine_md5_upper)
      ps.setString(5, imei_md5_upper)
      val rs = ps.executeQuery()
      while (rs.next()) {
        //取出广告日志中的pkgCode
        pkg_id = rs.getString("pkg_id")
        adv_name = rs.getInt("adv_name")
        ideaId = rs.getString("idea_id")
        firstLevel = rs.getString("first_level")
        secondLevel = rs.getString("second_level")
        channel_main_id = rs.getInt("channel_main_id")
        channel_name = rs.getString("channel_name")
      }
      rs.close()
    }
    //关闭ps
    if (ps != null) {
      ps.close();
    }
    tp10 = new Tuple10(jg, pkg_id, "", "", adv_name, ideaId, firstLevel, secondLevel, channel_main_id, channel_name)
    return tp10
  }


  /**
    * IOS通过 game_id + ip + dev + sys 进行激活匹配
    *
    * @param gameId
    * @param activeDate6dayBefore
    * @param activeDate
    * @param ip
    * @param devType
    * @param sysVersion
    * @param conn
    * @return
    */
  def iosMatchClickByIPAndDevAndSys(gameId: Int, activeDate6dayBefore: String, activeDate: String, ip: String, devType: String, sysVersion: String, conn: Connection): Tuple10[Int, String, String, String, Int, String, String, String, Int, String] = {
    var tp10 = Tuple10(0, "", "", "", 0, "", "", "", 0, "")
    var pkg_id = ""
    var adv_name = 0
    var ideaId = ""
    var firstLevel = ""
    var secondLevel = ""
    var channel_main_id = 0
    var channel_name = ""
    var jg = 0
    val update_sql = "update bi_new_merge_ad_click_detail set matched=1,active_time=now() where game_id=? and ts>=? and ts<=? and matched=0 and ip=? and deviceType=? and systemVersion=? limit 1"
    var ps: PreparedStatement = conn.prepareStatement(update_sql)
    ps.setInt(1, gameId)
    ps.setString(2, activeDate6dayBefore)
    ps.setString(3, activeDate + " 23:59:59")
    ps.setString(4, ip)
    ps.setString(5, devType)
    ps.setString(6, sysVersion)
    jg = ps.executeUpdate()
    if (jg > 0) {
      logger.info("ios激活通过IP+Dev+Sys匹配成功 : " + ip + "|" + devType + "|" + sysVersion + ", gameid:" + gameId)
      val selectSql = "select pkg_id,adv_name,idea_id,first_level,second_level,channel_main_id,channel_name from bi_new_merge_ad_click_detail where game_id=? and ts>=? and ts<=? and matched=1 and ip=? and deviceType=? and systemVersion=? limit 1"
      ps = conn.prepareStatement(selectSql)
      ps.setInt(1, gameId)
      ps.setString(2, activeDate6dayBefore)
      ps.setString(3, activeDate + " 23:59:59")
      ps.setString(4, ip)
      ps.setString(5, devType)
      ps.setString(6, sysVersion)

      val rs = ps.executeQuery()
      while (rs.next()) {
        //取出广告日志中的pkgCode
        pkg_id = rs.getString("pkg_id")
        adv_name = rs.getInt("adv_name")
        ideaId = rs.getString("idea_id")
        firstLevel = rs.getString("first_level")
        secondLevel = rs.getString("second_level")
        channel_main_id = rs.getInt("channel_main_id")
        channel_name = rs.getString("channel_name")
      }
      rs.close()
    }
    if (ps != null) {
      ps.close()
    }
    tp10 = new Tuple10(jg, pkg_id, "", "", adv_name, ideaId, firstLevel, secondLevel, channel_main_id, channel_name)
    return tp10
  }

  def iosMatchClickByIPAndDevAndSysOffLine(gameId: Int, activeDate6dayBefore: String, activeDate: String, ip: String, devType: String, sysVersion: String, conn: Connection): Tuple10[Int, String, String, String, Int, String, String, String, Int, String] = {
    var tp10 = Tuple10(0, "", "", "", 0, "", "", "", 0, "")
    var pkg_id = ""
    var adv_name = 0
    var ideaId = ""
    var firstLevel = ""
    var secondLevel = ""
    var channel_main_id = 0
    var channel_name = ""
    var jg = 0
    val update_sql = "update bi_new_merge_ad_click_detail set matched=1 where game_id=? and ts>=? and ts<=? and matched=0 and ip=? and deviceType=? and systemVersion=? limit 1"
    var ps: PreparedStatement = conn.prepareStatement(update_sql)
    ps.setInt(1, gameId)
    ps.setString(2, activeDate6dayBefore)
    ps.setString(3, activeDate + " 23:59:59")
    ps.setString(4, ip)
    ps.setString(5, devType)
    ps.setString(6, sysVersion)
    jg = ps.executeUpdate()
    if (jg > 0) {
      logger.info("ios激活通过IP+Dev+Sys匹配成功 : " + ip + "|" + devType + "|" + sysVersion + ", gameid:" + gameId)
      val selectSql = "select pkg_id,adv_name,idea_id,first_level,second_level,channel_main_id,channel_name from bi_new_merge_ad_click_detail where game_id=? and ts>=? and ts<=? and matched=1 and ip=? and deviceType=? and systemVersion=? limit 1"
      ps = conn.prepareStatement(selectSql)
      ps.setInt(1, gameId)
      ps.setString(2, activeDate6dayBefore)
      ps.setString(3, activeDate + " 23:59:59")
      ps.setString(4, ip)
      ps.setString(5, devType)
      ps.setString(6, sysVersion)

      val rs = ps.executeQuery()
      while (rs.next()) {
        //取出广告日志中的pkgCode
        pkg_id = rs.getString("pkg_id")
        adv_name = rs.getInt("adv_name")
        ideaId = rs.getString("idea_id")
        firstLevel = rs.getString("first_level")
        secondLevel = rs.getString("second_level")
        channel_main_id = rs.getInt("channel_main_id")
        channel_name = rs.getString("channel_name")
      }
      rs.close()
    }
    if (ps != null) {
      ps.close()
    }
    tp10 = new Tuple10(jg, pkg_id, "", "", adv_name, ideaId, firstLevel, secondLevel, channel_main_id, channel_name)
    return tp10
  }


  /**
    * IOS通过 game_id + ip + dev 进行激活匹配
    *
    * @param gameId
    * @param activeDate6dayBefore
    * @param activeDate
    * @param ip
    * @param devType
    * @param conn
    * @return
    */
  def iosMatchClickByIPAndDev(gameId: Int, activeDate6dayBefore: String, activeDate: String, ip: String, devType: String, conn: Connection): Tuple10[Int, String, String, String, Int, String, String, String, Int, String] = {
    var tp10 = Tuple10(0, "", "", "", 0, "", "", "", 0, "")
    var pkg_id = ""
    var adv_name = 0
    var ideaId = ""
    var firstLevel = ""
    var secondLevel = ""
    var channel_main_id = 0
    var channel_name = ""
    var jg = 0
    val update_sql = "update bi_new_merge_ad_click_detail set matched=1,active_time=now() where game_id=? and ts>=? and ts<=? and matched=0 and ip=? and deviceType=? limit 1"
    var ps: PreparedStatement = conn.prepareStatement(update_sql)
    ps.setInt(1, gameId)
    ps.setString(2, activeDate6dayBefore)
    ps.setString(3, activeDate + " 23:59:59")
    ps.setString(4, ip)
    ps.setString(5, devType)
    jg = ps.executeUpdate()
    if (jg > 0) {
      logger.info("ios激活通过IP+Dev匹配成功 : " + ip + "|" + devType + ", gameid:" + gameId)
      val selectSql = "select pkg_id,adv_name,idea_id,first_level,second_level,channel_main_id,channel_name from bi_new_merge_ad_click_detail where game_id=? and ts>=? and ts<=? and matched=1 and ip=? and deviceType=? limit 1"
      ps = conn.prepareStatement(selectSql)
      ps.setInt(1, gameId)
      ps.setString(2, activeDate6dayBefore)
      ps.setString(3, activeDate + " 23:59:59")
      ps.setString(4, ip)
      ps.setString(5, devType)

      val rs = ps.executeQuery()
      while (rs.next()) {
        //取出广告日志中的pkgCode
        pkg_id = rs.getString("pkg_id")
        adv_name = rs.getInt("adv_name")
        ideaId = rs.getString("idea_id")
        firstLevel = rs.getString("first_level")
        secondLevel = rs.getString("second_level")
        channel_main_id = rs.getInt("channel_main_id")
        channel_name = rs.getString("channel_name")
      }
      rs.close()
    }
    if (ps != null) {
      ps.close()
    }
    tp10 = new Tuple10(jg, pkg_id, "", "", adv_name, ideaId, firstLevel, secondLevel, channel_main_id, channel_name)
    return tp10
  }

  def iosMatchClickByIPAndDevOffLine(gameId: Int, activeDate6dayBefore: String, activeDate: String, ip: String, devType: String, conn: Connection): Tuple10[Int, String, String, String, Int, String, String, String, Int, String] = {
    var tp10 = Tuple10(0, "", "", "", 0, "", "", "", 0, "")
    var pkg_id = ""
    var adv_name = 0
    var ideaId = ""
    var firstLevel = ""
    var secondLevel = ""
    var channel_main_id = 0
    var channel_name = ""
    var jg = 0
    val update_sql = "update bi_new_merge_ad_click_detail set matched=1 where game_id=? and ts>=? and ts<=? and matched=0 and ip=? and deviceType=? limit 1"
    var ps: PreparedStatement = conn.prepareStatement(update_sql)
    ps.setInt(1, gameId)
    ps.setString(2, activeDate6dayBefore)
    ps.setString(3, activeDate + " 23:59:59")
    ps.setString(4, ip)
    ps.setString(5, devType)
    jg = ps.executeUpdate()
    if (jg > 0) {
      logger.info("ios激活通过IP+Dev匹配成功 : " + ip + "|" + devType + ", gameid:" + gameId)
      val selectSql = "select pkg_id,adv_name,idea_id,first_level,second_level,channel_main_id,channel_name from bi_new_merge_ad_click_detail where game_id=? and ts>=? and ts<=? and matched=1 and ip=? and deviceType=? limit 1"
      ps = conn.prepareStatement(selectSql)
      ps.setInt(1, gameId)
      ps.setString(2, activeDate6dayBefore)
      ps.setString(3, activeDate + " 23:59:59")
      ps.setString(4, ip)
      ps.setString(5, devType)

      val rs = ps.executeQuery()
      while (rs.next()) {
        //取出广告日志中的pkgCode
        pkg_id = rs.getString("pkg_id")
        adv_name = rs.getInt("adv_name")
        ideaId = rs.getString("idea_id")
        firstLevel = rs.getString("first_level")
        secondLevel = rs.getString("second_level")
        channel_main_id = rs.getInt("channel_main_id")
        channel_name = rs.getString("channel_name")
      }
      rs.close()
    }
    if (ps != null) {
      ps.close()
    }
    tp10 = new Tuple10(jg, pkg_id, "", "", adv_name, ideaId, firstLevel, secondLevel, channel_main_id, channel_name)
    return tp10
  }

  /**
    * IOS通过 game_id + ip 进行激活匹配
    *
    * @param gameId
    * @param activeDate6dayBefore
    * @param activeDate
    * @param ip
    * @param conn
    * @return
    */
  def iosMatchClickByIP(gameId: Int, activeDate6dayBefore: String, activeDate: String, ip: String, conn: Connection): Tuple10[Int, String, String, String, Int, String, String, String, Int, String] = {
    var tp10 = Tuple10(0, "", "", "", 0, "", "", "", 0, "")
    var pkg_id = ""
    var adv_name = 0
    var ideaId = ""
    var firstLevel = ""
    var secondLevel = ""
    var channel_main_id = 0
    var channel_name = ""
    var jg = 0
    val update_sql = "update bi_new_merge_ad_click_detail set matched=1,active_time=now() where game_id=? and ts>=? and ts<=? and matched=0 and ip=? limit 1"
    var ps: PreparedStatement = conn.prepareStatement(update_sql)
    ps.setInt(1, gameId)
    ps.setString(2, activeDate6dayBefore)
    ps.setString(3, activeDate + " 23:59:59")
    ps.setString(4, ip)

    jg = ps.executeUpdate()
    if (jg > 0) {
      logger.info("ios通过IP激活匹配成功 : " + ip + ", gameid:" + gameId)
      val selectSql = "select pkg_id,adv_name,idea_id,first_level,second_level,channel_main_id,channel_name from bi_new_merge_ad_click_detail where game_id=? and ts>=? and ts<=? and matched=1 and ip=? limit 1"
      ps = conn.prepareStatement(selectSql)
      ps.setInt(1, gameId)
      ps.setString(2, activeDate6dayBefore)
      ps.setString(3, activeDate + " 23:59:59")
      ps.setString(4, ip)

      val rs = ps.executeQuery()
      while (rs.next()) {
        //取出广告日志中的pkgCode
        pkg_id = rs.getString("pkg_id")
        adv_name = rs.getInt("adv_name")
        ideaId = rs.getString("idea_id")
        firstLevel = rs.getString("first_level")
        secondLevel = rs.getString("second_level")
        channel_main_id = rs.getInt("channel_main_id")
        channel_name = rs.getString("channel_name")
      }
      rs.close()
    }
    if (ps != null) {
      ps.close()
    }
    tp10 = new Tuple10(jg, pkg_id, "", "", adv_name, ideaId, firstLevel, secondLevel, channel_main_id, channel_name)
    return tp10
  }


  def iosMatchClickByIPOffLine(gameId: Int, activeDate6dayBefore: String, activeDate: String, ip: String, conn: Connection): Tuple10[Int, String, String, String, Int, String, String, String, Int, String] = {
    var tp10 = Tuple10(0, "", "", "", 0, "", "", "", 0, "")
    var pkg_id = ""
    var adv_name = 0
    var ideaId = ""
    var firstLevel = ""
    var secondLevel = ""
    var channel_main_id = 0
    var channel_name = ""
    var jg = 0
    val update_sql = "update bi_new_merge_ad_click_detail set matched=1 where game_id=? and ts>=? and ts<=? and matched=0 and ip=? limit 1"
    var ps: PreparedStatement = conn.prepareStatement(update_sql)
    ps.setInt(1, gameId)
    ps.setString(2, activeDate6dayBefore)
    ps.setString(3, activeDate + " 23:59:59")
    ps.setString(4, ip)

    jg = ps.executeUpdate()
    if (jg > 0) {
      logger.info("ios通过IP激活匹配成功 : " + ip + ", gameid:" + gameId)
      val selectSql = "select pkg_id,adv_name,idea_id,first_level,second_level,channel_main_id,channel_name from bi_new_merge_ad_click_detail where game_id=? and ts>=? and ts<=? and matched=1 and ip=? limit 1"
      ps = conn.prepareStatement(selectSql)
      ps.setInt(1, gameId)
      ps.setString(2, activeDate6dayBefore)
      ps.setString(3, activeDate + " 23:59:59")
      ps.setString(4, ip)

      val rs = ps.executeQuery()
      while (rs.next()) {
        //取出广告日志中的pkgCode
        pkg_id = rs.getString("pkg_id")
        adv_name = rs.getInt("adv_name")
        ideaId = rs.getString("idea_id")
        firstLevel = rs.getString("first_level")
        secondLevel = rs.getString("second_level")
        channel_main_id = rs.getInt("channel_main_id")
        channel_name = rs.getString("channel_name")
      }
      rs.close()
    }
    if (ps != null) {
      ps.close()
    }
    tp10 = new Tuple10(jg, pkg_id, "", "", adv_name, ideaId, firstLevel, secondLevel, channel_main_id, channel_name)
    return tp10
  }

  def matchClickIos(imei: String, imeiWithLine_md5_upper: String, imei_md5_upper: String, gameId: Int, activeDate6dayBefore: String, activeDate: String, ip: String, devType: String, sysVersion: String, conn: Connection): Tuple10[Int, String, String, String, Int, String, String, String, Int, String] = {
    var tp10 = Tuple10(0, "", "", "", 0, "", "", "", 0, "")
    var pkg_id = ""
    var adv_name = 0
    var ideaId = ""
    var firstLevel = ""
    var secondLevel = ""
    var channel_main_id = 0
    var channel_name = ""
    var ps: PreparedStatement = null
    //jg用来标记数据更新的行数
    var jg = 0
    if (!imei.equals("00000000000000000000000000000000")) {
      //imei = 5896EEA3ECCF40FAA33AF1201DA8B3EF
      //1：先用imei匹配
      //2：如果用imei没有匹配到，再用ua+ip匹配
      val update_sql = "update bi_new_merge_ad_click_detail set matched=1,active_time=now() where game_id=? and ts>=? and ts<=? and matched=0 and (imei_md5_upper=? or imei_md5_upper=?) limit 1"
      ps = conn.prepareStatement(update_sql)
      ps.setInt(1, gameId)
      ps.setString(2, activeDate6dayBefore)
      ps.setString(3, activeDate + " 23:59:59")
      ps.setString(4, imeiWithLine_md5_upper)
      ps.setString(5, imei_md5_upper)
      jg = ps.executeUpdate()
      //jg是更新的数据的行数,先用设备imei去匹配，如果没有匹配到，再用ua+ip去匹配
      if (jg > 0) {
        logger.info("ios激活匹配成功:" + imeiWithLine_md5_upper + " gameid:" + gameId)
        val slSql = "select pkg_id,adv_name,idea_id,first_level,second_level,channel_main_id,channel_name from bi_new_merge_ad_click_detail  where game_id=? and ts>=? and ts<=? and matched=1 and (imei_md5_upper=? or imei_md5_upper=?) limit 1"
        ps = conn.prepareStatement(slSql);
        ps.setInt(1, gameId)
        ps.setString(2, activeDate6dayBefore)
        ps.setString(3, activeDate + " 23:59:59")
        ps.setString(4, imeiWithLine_md5_upper)
        ps.setString(5, imei_md5_upper)
        val rs = ps.executeQuery()
        while (rs.next()) {
          //取出广告日志中的pkgCode
          pkg_id = rs.getString("pkg_id")
          adv_name = rs.getInt("adv_name")
          ideaId = rs.getString("idea_id")
          firstLevel = rs.getString("first_level")
          secondLevel = rs.getString("second_level")
          channel_main_id = rs.getInt("channel_main_id")
          channel_name = rs.getString("channel_name")
        }
        rs.close()
        tp10 = new Tuple10(jg, pkg_id, "", "", adv_name, ideaId, firstLevel, secondLevel, channel_main_id, channel_name)
      } else {
        //当sysVersion不为空，devType肯定不为空，就用ip+devType+sysVersion
        //如果sysVersion为空，devType可能为空，可能不为空，这时候，只用ip去匹配
        if (!sysVersion.equals("")) {
          //          val update_sql = "update bi_new_merge_ad_click_detail set matched=1,active_time=now() where date(ts)>=? and date(ts)<=? and ip=? and deviceType=? and systemVersion=? and matched=0 and game_id=?"
          val update_sql = "update bi_new_merge_ad_click_detail set matched=1,active_time=now() where game_id=? and ts>=? and ts<=? and matched=0 and ip=? and deviceType=? and systemVersion=? "
          ps = conn.prepareStatement(update_sql)
          ps.setInt(1, gameId)
          ps.setString(2, activeDate6dayBefore)
          ps.setString(3, activeDate + " 23:59:59")
          ps.setString(4, ip)
          ps.setString(5, devType)
          ps.setString(6, sysVersion)
          jg = ps.executeUpdate()
          if (jg > 0) {
            logger.info("ios激活匹配成功:|" + ip + "|" + devType + "|" + sysVersion + " gameid:" + gameId)
            val selectSql = "select pkg_id,adv_name,idea_id,first_level,second_level,channel_main_id,channel_name from bi_new_merge_ad_click_detail where game_id=? and ts>=? and ts<=? and matched=1 and ip=? and deviceType=? and systemVersion=? limit 1"
            ps = conn.prepareStatement(selectSql)
            ps.setInt(1, gameId)
            ps.setString(2, activeDate6dayBefore)
            ps.setString(3, activeDate + " 23:59:59")
            ps.setString(4, ip)
            ps.setString(5, devType)
            ps.setString(6, sysVersion)

            val rs = ps.executeQuery()
            while (rs.next()) {
              //取出广告日志中的pkgCode
              pkg_id = rs.getString("pkg_id")
              adv_name = rs.getInt("adv_name")
              ideaId = rs.getString("idea_id")
              firstLevel = rs.getString("first_level")
              secondLevel = rs.getString("second_level")
              channel_main_id = rs.getInt("channel_main_id")
              channel_name = rs.getString("channel_name")
            }
            rs.close()
          }
          //ios设备的pkgCode取的是广告日志中的,idfa返回的是激活日志中原始idfa
          tp10 = new Tuple10(jg, pkg_id, "", "", adv_name, ideaId, firstLevel, secondLevel, channel_main_id, channel_name)
        } else {
          val update_sql = "update bi_new_merge_ad_click_detail set matched=1,active_time=now() where game_id=? and ts>=? and ts<=? and matched=0 and ip=? limit 1"
          ps = conn.prepareStatement(update_sql)
          ps.setInt(1, gameId)
          ps.setString(2, activeDate6dayBefore)
          ps.setString(3, activeDate + " 23:59:59")
          ps.setString(4, ip)
          jg = ps.executeUpdate()
          if (jg > 0) {
            logger.info("ios激活匹配成功:|" + ip + "|" + " gameid:" + gameId)
            val selectSql = "select pkg_id,adv_name,idea_id,first_level,second_level,channel_main_id,channel_name from bi_new_merge_ad_click_detail where game_id=? and ts>=? and ts<=? and matched=1 and ip=? limit 1"
            ps = conn.prepareStatement(selectSql)
            ps.setInt(1, gameId)
            ps.setString(2, activeDate6dayBefore)
            ps.setString(3, activeDate + " 23:59:59")
            ps.setString(4, ip)
            val rs = ps.executeQuery()
            while (rs.next()) {
              //取出广告日志中的pkgCode
              pkg_id = rs.getString("pkg_id")
              adv_name = rs.getInt("adv_name")
              ideaId = rs.getString("idea_id")
              firstLevel = rs.getString("first_level")
              secondLevel = rs.getString("second_level")
              channel_main_id = rs.getInt("channel_main_id")
              channel_name = rs.getString("channel_name")
            }
            rs.close()
          }
          //ios设备的pkgCode取的是广告日志中的,idfa返回的是激活日志中原始idfa
          tp10 = new Tuple10(jg, pkg_id, "", "", adv_name, ideaId, firstLevel, secondLevel, channel_main_id, channel_name)
        }
      }
    } else {
      //如果imei=00000000000000000000000000000000 ，就直接用ua+ip匹配
      //如果 sysVersion为空，就只用ip匹配，如果sysVersion不为空，用ip+devType+sysVersion匹配
      if (!sysVersion.equals("")) {
        val update_sql = "update bi_new_merge_ad_click_detail set matched=1,active_time=now() where game_id=? and ts>=? and ts<=? and matched=0 and ip=? and deviceType=? and systemVersion=? limit 1"
        ps = conn.prepareStatement(update_sql)
        ps.setInt(1, gameId)
        ps.setString(2, activeDate6dayBefore)
        ps.setString(3, activeDate + " 23:59:59")
        ps.setString(4, ip)
        ps.setString(5, devType)
        ps.setString(6, sysVersion)
        jg = ps.executeUpdate()
        if (jg > 0) {
          logger.info("ios激活匹配成功:|" + ip + "|" + devType + "|" + sysVersion + " gameid:" + gameId)
          val selectSql = "select pkg_id,adv_name,idea_id,first_level,second_level,channel_main_id,channel_name from bi_new_merge_ad_click_detail where game_id=? and ts>=? and ts<=? and matched=1 and ip=? and deviceType=? and systemVersion=? limit 1"
          ps = conn.prepareStatement(selectSql)
          ps.setInt(1, gameId)
          ps.setString(2, activeDate6dayBefore)
          ps.setString(3, activeDate + " 23:59:59")
          ps.setString(4, ip)
          ps.setString(5, devType)
          ps.setString(6, sysVersion)
          val rs = ps.executeQuery()
          while (rs.next()) {
            //取出广告日志中的pkgCode
            pkg_id = rs.getString("pkg_id")
            adv_name = rs.getInt("adv_name")
            ideaId = rs.getString("idea_id")
            firstLevel = rs.getString("first_level")
            secondLevel = rs.getString("second_level")
            channel_main_id = rs.getInt("channel_main_id")
            channel_name = rs.getString("channel_name")
          }
          rs.close()
        }
        //这个pkg_id是取的点击明细表中的pkg_id
        tp10 = new Tuple10(jg, pkg_id, "00000000000000000000000000000000", "", adv_name, ideaId, firstLevel, secondLevel, channel_main_id, channel_name)
      } else {
        val update_sql = "update bi_new_merge_ad_click_detail set matched=1,active_time=now() where game_id=? and ts>=? and ts<=? and matched=0 and ip=? limit 1"
        ps = conn.prepareStatement(update_sql)
        ps.setInt(1, gameId)
        ps.setString(2, activeDate6dayBefore)
        ps.setString(3, activeDate + " 23:59:59")
        ps.setString(4, ip)
        jg = ps.executeUpdate()
        if (jg > 0) {
          logger.info("ios激活匹配成功:|" + ip + "|" + " gameid:" + gameId)
          val selectSql = "select pkg_id,adv_name,idea_id,first_level,second_level,channel_main_id,channel_name from bi_new_merge_ad_click_detail where game_id=? and ts>=? and ts<=? and matched=1 and ip=? limit 1"
          ps = conn.prepareStatement(selectSql)
          ps.setInt(1, gameId)
          ps.setString(2, activeDate6dayBefore)
          ps.setString(3, activeDate + " 23:59:59")
          ps.setString(4, ip)
          val rs = ps.executeQuery()
          while (rs.next()) {
            //取出广告日志中的pkgCode
            pkg_id = rs.getString("pkg_id")
            adv_name = rs.getInt("adv_name")
            ideaId = rs.getString("idea_id")
            firstLevel = rs.getString("first_level")
            secondLevel = rs.getString("second_level")
            channel_main_id = rs.getInt("channel_main_id")
            channel_name = rs.getString("channel_name")
          }
          rs.close()
        }
        //这个pkg_id是取的点击明细表中的pkg_id
        tp10 = new Tuple10(jg, pkg_id, "00000000000000000000000000000000", "", adv_name, ideaId, firstLevel, secondLevel, channel_main_id, channel_name)
      }

    }
    //关闭ps
    if (ps != null) {
      ps.close();
    }
    return tp10
  }


  /**
    * 通过 ip+devType+sysVersion 更新点击数据
    *
    * @param pkg_id
    * @param ip
    * @param devType
    * @param sysVersion
    * @param clickTime
    * @param callback
    * @param conn
    */
  def updateMomoClickByUA(pkg_id: String, ip: String, devType: String, sysVersion: String, clickTime: String, callback: String, conn: Connection) = {
    val updateSql = "update bi_new_merge_ad_click_detail set ts='" + clickTime + "',callback ='" + callback + "' where pkg_id='" + pkg_id + "' and ip='" + ip + "' and deviceType='" + devType + "' and systemVersion='" + sysVersion + "'"
    val stmt = conn.createStatement()
    stmt.executeUpdate(updateSql)
    stmt.close()
  }


  /**
    * android 通过 gamei_id + imei 激活匹配点击
    *
    * @param imei_md5_upper
    * @param game_id
    * @param activeDate6dayBefore
    * @param activeDate
    * @param conn
    * @return
    */
  def androidMatchClickByImei(imei_md5_upper: String, game_id: Int, activeDate6dayBefore: String, activeDate: String, conn: Connection): Tuple10[Int, String, String, String, Int, String, String, String, Int, String] = {
    var tp10 = Tuple10(0, "", "", "", 0, "", "", "", 0, "")
    var advName = 0
    var pkg_id = ""
    var ideaId = ""
    var firstLevel = ""
    var secondLevel = ""
    var ps: PreparedStatement = null
    //jg用来标记数据被更新的行数
    var jg = 0
    //imei_md5_upper匹配
    val instSql = "update bi_new_merge_ad_click_detail set matched=1,active_time=now() where game_id=? and ts>=? and ts<=? and matched=0 and imei_md5_upper=? limit 1"
    ps = conn.prepareStatement(instSql)
    ps.setInt(1, game_id)
    ps.setString(2, activeDate6dayBefore)
    ps.setString(3, activeDate + " 23:59:59")
    ps.setString(4, imei_md5_upper)
    jg = ps.executeUpdate()
    //根据匹配结果返回值
    if (jg > 0) {
      logger.info("android通过 imei_md5_upper : " + imei_md5_upper + " 激活匹配成功！ game_id:" + game_id)
      val slSql = "select pkg_id,adv_name,idea_id,first_level,second_level from bi_new_merge_ad_click_detail where game_id=? and ts>=? and ts<=? and matched=1 and imei_md5_upper=? limit 1"
      ps = conn.prepareStatement(slSql);
      ps.setInt(1, game_id)
      ps.setString(2, activeDate6dayBefore)
      ps.setString(3, activeDate + " 23:59:59")
      ps.setString(4, imei_md5_upper)
      val rs = ps.executeQuery()
      while (rs.next()) {
        pkg_id = rs.getString("pkg_id")
        advName = rs.getInt("adv_name")
        ideaId = rs.getString("idea_id")
        firstLevel = rs.getString("first_level")
        secondLevel = rs.getString("second_level")
      }
      rs.close()
    }
    if (ps != null) {
      ps.close();
    }
    tp10 = new Tuple10(jg, pkg_id, imei_md5_upper, "", advName, ideaId, firstLevel, secondLevel, 0, "")
    return tp10
  }

  def androidMatchClickByImeiOffLine(imei_md5_upper: String, game_id: Int, activeDate6dayBefore: String, activeDate: String, conn: Connection): Tuple10[Int, String, String, String, Int, String, String, String, Int, String] = {
    var tp10 = Tuple10(0, "", "", "", 0, "", "", "", 0, "")
    var advName = 0
    var pkg_id = ""
    var ideaId = ""
    var firstLevel = ""
    var secondLevel = ""
    var ps: PreparedStatement = null
    //jg用来标记数据被更新的行数
    var jg = 0
    //imei_md5_upper匹配
    val instSql = "update bi_new_merge_ad_click_detail set matched=1 where game_id=? and ts>=? and ts<=? and matched=0 and imei_md5_upper=? limit 1"
    ps = conn.prepareStatement(instSql)
    ps.setInt(1, game_id)
    ps.setString(2, activeDate6dayBefore)
    ps.setString(3, activeDate + " 23:59:59")
    ps.setString(4, imei_md5_upper)
    jg = ps.executeUpdate()
    //根据匹配结果返回值
    if (jg > 0) {
      logger.info("android通过 imei_md5_upper : " + imei_md5_upper + " 激活匹配成功！ game_id:" + game_id)
      val slSql = "select pkg_id,adv_name,idea_id,first_level,second_level from bi_new_merge_ad_click_detail where game_id=? and ts>=? and ts<=? and matched=1 and imei_md5_upper=? limit 1"
      ps = conn.prepareStatement(slSql);
      ps.setInt(1, game_id)
      ps.setString(2, activeDate6dayBefore)
      ps.setString(3, activeDate + " 23:59:59")
      ps.setString(4, imei_md5_upper)
      val rs = ps.executeQuery()
      while (rs.next()) {
        pkg_id = rs.getString("pkg_id")
        advName = rs.getInt("adv_name")
        ideaId = rs.getString("idea_id")
        firstLevel = rs.getString("first_level")
        secondLevel = rs.getString("second_level")
      }
      rs.close()
    }
    if (ps != null) {
      ps.close();
    }
    tp10 = new Tuple10(jg, pkg_id, imei_md5_upper, "", advName, ideaId, firstLevel, secondLevel, 0, "")
    return tp10
  }

  /**
    * android 通过game_id + androidID 激活匹配点击
    *
    * @param androidID
    * @param game_id
    * @param activeDate6dayBefore
    * @param activeDate
    * @param conn
    * @return
    */
  def androidMatchClickByAndroidID(androidID: String, game_id: Int, activeDate6dayBefore: String, activeDate: String, conn: Connection): Tuple10[Int, String, String, String, Int, String, String, String, Int, String] = {
    var tp10 = Tuple10(0, "", "", "", 0, "", "", "", 0, "")
    var advName = 0
    var pkg_id = ""
    var ideaId = ""
    var firstLevel = ""
    var secondLevel = ""
    //jg用来标记数据被更新的行数
    var jg = 0
    //imei_md5_upper匹配
    val match_sql = "update bi_new_merge_ad_click_detail set matched=1,active_time=now() where game_id=? and ts>=? and ts<=? and matched=0 and androidID=? limit 1"
    var ps = conn.prepareStatement(match_sql)
    ps.setInt(1, game_id)
    ps.setString(2, activeDate6dayBefore)
    ps.setString(3, activeDate + " 23:59:59")
    ps.setString(4, androidID)
    jg = ps.executeUpdate()
    if (jg > 0) {
      logger.info("android通过 androidid : " + androidID + " 激活匹配成功！ game_id=" + game_id)
      val select_sql = "select pkg_id,adv_name,idea_id,first_level,second_level from bi_new_merge_ad_click_detail where game_id=? and ts>=? and ts<=? and matched=1 and androidID=? limit 1"
      ps = conn.prepareStatement(select_sql)
      ps.setInt(1, game_id)
      ps.setString(2, activeDate6dayBefore)
      ps.setString(3, activeDate + " 23:59:59")
      ps.setString(4, androidID)
      val rs = ps.executeQuery()
      while (rs.next()) {
        pkg_id = rs.getString("pkg_id")
        advName = rs.getInt("adv_name")
        ideaId = rs.getString("idea_id")
        firstLevel = rs.getString("first_level")
        secondLevel = rs.getString("second_level")
      }
      rs.close()
    }
    if (ps != null) {
      ps.close();
    }
    tp10 = new Tuple10(jg, pkg_id, "", "", advName, ideaId, firstLevel, secondLevel, 0, "")
    return tp10
  }

  def androidMatchClickByAndroidIDOffLine(androidID: String, game_id: Int, activeDate6dayBefore: String, activeDate: String, conn: Connection): Tuple10[Int, String, String, String, Int, String, String, String, Int, String] = {
    var tp10 = Tuple10(0, "", "", "", 0, "", "", "", 0, "")
    var advName = 0
    var pkg_id = ""
    var ideaId = ""
    var firstLevel = ""
    var secondLevel = ""
    //jg用来标记数据被更新的行数
    var jg = 0
    //imei_md5_upper匹配
    val match_sql = "update bi_new_merge_ad_click_detail set matched=1 where game_id=? and ts>=? and ts<=? and matched=0 and androidID=? limit 1"
    var ps = conn.prepareStatement(match_sql)
    ps.setInt(1, game_id)
    ps.setString(2, activeDate6dayBefore)
    ps.setString(3, activeDate + " 23:59:59")
    ps.setString(4, androidID)
    jg = ps.executeUpdate()
    if (jg > 0) {
      logger.info("android通过 androidid : " + androidID + " 激活匹配成功！ game_id=" + game_id)
      val select_sql = "select pkg_id,adv_name,idea_id,first_level,second_level from bi_new_merge_ad_click_detail where game_id=? and ts>=? and ts<=? and matched=1 and androidID=? limit 1"
      ps = conn.prepareStatement(select_sql)
      ps.setInt(1, game_id)
      ps.setString(2, activeDate6dayBefore)
      ps.setString(3, activeDate + " 23:59:59")
      ps.setString(4, androidID)
      val rs = ps.executeQuery()
      while (rs.next()) {
        pkg_id = rs.getString("pkg_id")
        advName = rs.getInt("adv_name")
        ideaId = rs.getString("idea_id")
        firstLevel = rs.getString("first_level")
        secondLevel = rs.getString("second_level")
      }
      rs.close()
    }
    if (ps != null) {
      ps.close();
    }
    tp10 = new Tuple10(jg, pkg_id, "", "", advName, ideaId, firstLevel, secondLevel, 0, "")
    return tp10
  }


  /**
    * 对是否匹配值进行更新 -android
    *
    * @param imei_md5_upper
    * @param androidID
    * @param activeDate6dayBefore
    * @param activeDate
    */


  def matchClickAndroidByImeiAndAndoridID(imei_md5_upper: String, androidID: String, game_id: Int, activeDate6dayBefore: String, activeDate: String, conn: Connection): Tuple10[Int, String, String, String, Int, String, String, String, Int, String] = {
    var tp10 = Tuple10(0, "", "", "", 0, "", "", "", 0, "")

    var advName = 0
    var pkg_id = ""
    var ideaId = ""
    var firstLevel = ""
    var secondLevel = ""
    var ps: PreparedStatement = null
    //jg用来标记数据被更新的行数
    var jg = 0

    //imei_md5_upper匹配
    val instSql = "update bi_new_merge_ad_click_detail set matched=1,active_time=now() where game_id=? and ts>=? and ts<=? and matched=0 and imei_md5_upper=? limit 1"
    ps = conn.prepareStatement(instSql)
    ps.setInt(1, game_id)
    ps.setString(2, activeDate6dayBefore)
    ps.setString(3, activeDate + " 23:59:59")
    ps.setString(4, imei_md5_upper)
    jg = ps.executeUpdate()
    //根据匹配结果得出返回值
    if (jg > 0) {
      logger.info("android通过 imei_md5_upper : " + imei_md5_upper + " 激活匹配成功！ game_id:" + game_id)
      val slSql = "select pkg_id,adv_name,idea_id,first_level,second_level from bi_new_merge_ad_click_detail where game_id=? and ts>=? and ts<=? and matched=1 and imei_md5_upper=? limit 1"
      ps = conn.prepareStatement(slSql);
      ps.setInt(1, game_id)
      ps.setString(2, activeDate6dayBefore)
      ps.setString(3, activeDate + " 23:59:59")
      ps.setString(4, imei_md5_upper)
      val rs = ps.executeQuery()
      while (rs.next()) {
        pkg_id = rs.getString("pkg_id")
        advName = rs.getString("adv_name").toInt
        ideaId = rs.getString("idea_id")
        firstLevel = rs.getString("first_level")
        secondLevel = rs.getString("second_level")
      }
    } else {
      //androidID匹配
      val match_sql = "update bi_new_merge_ad_click_detail set matched=1,active_time=now() where game_id=? and ts>=? and ts<=? and matched=0 and androidID=? limit 1"
      ps = conn.prepareStatement(match_sql)
      ps.setInt(1, game_id)
      ps.setString(2, activeDate6dayBefore)
      ps.setString(3, activeDate + " 23:59:59")
      ps.setString(4, androidID)
      jg = ps.executeUpdate()
      if (jg > 0) {
        logger.info("android通过 androidid : " + androidID + " 激活匹配成功！ game_id=" + game_id)
        val select_sql = "select pkg_id,adv_name,idea_id,first_level,second_level from bi_new_merge_ad_click_detail where game_id=? and ts>=? and ts<=? and matched=1 and androidID=? limit 1"
        ps = conn.prepareStatement(select_sql)
        ps.setInt(1, game_id)
        ps.setString(2, activeDate6dayBefore)
        ps.setString(3, activeDate + " 23:59:59")
        ps.setString(4, androidID)
        val rs = ps.executeQuery()
        while (rs.next()) {
          pkg_id = rs.getString("pkg_id")
          advName = rs.getString("adv_name").toInt
          ideaId = rs.getString("idea_id")
          firstLevel = rs.getString("first_level")
          secondLevel = rs.getString("second_level")
        }
      }
    }

    ps.close();
    //android设备中的pkgCode，激活日志和广告点击日志是相同的，这里取的是激活日志中的
    tp10 = new Tuple10(jg, pkg_id, imei_md5_upper, "", advName, ideaId, firstLevel, secondLevel, 0, "")
    return tp10

  }

  /**
    * 通过androidID来匹配
    *
    * @param androidID
    * @param game_id
    * @param activeDate6dayBefore
    * @param activeDate
    * @param conn
    * @return
    */
  def matchClickAndroidByAndroidID(androidID: String, game_id: Int, activeDate6dayBefore: String, activeDate: String, conn: Connection): Tuple10[Int, String, String, String, Int, String, String, String, Int, String] = {
    var tp10 = Tuple10(0, "", "", "", 0, "", "", "", 0, "")
    var advName = 0
    var pkg_id = ""
    var ideaId = ""
    var firstLevel = ""
    var secondLevel = ""
    var ps: PreparedStatement = null
    //jg用来标记数据被更新的行数
    var jg = 0
    val match_sql = "update bi_new_merge_ad_click_detail set matched=1,active_time=now() where game_id=? and ts>=? and ts<=? and matched=0 and androidID=? limit 1"
    ps = conn.prepareStatement(match_sql)
    ps.setInt(1, game_id)
    ps.setString(2, activeDate6dayBefore)
    ps.setString(3, activeDate + " 23:59:59")
    ps.setString(4, androidID)
    jg = ps.executeUpdate()
    if (jg > 0) {
      logger.info("android通过 androidid : " + androidID + " 激活匹配成功！ game_id=" + game_id)
      val select_sql = "select pkg_id,adv_name,idea_id,first_level,second_level from bi_new_merge_ad_click_detail where game_id=? and ts>=? and ts<=? and matched=1 and androidID=? limit 1"
      ps = conn.prepareStatement(select_sql)
      ps.setInt(1, game_id)
      ps.setString(2, activeDate6dayBefore)
      ps.setString(3, activeDate + " 23:59:59")
      ps.setString(4, androidID)
      val rs = ps.executeQuery()
      while (rs.next()) {
        pkg_id = rs.getString("pkg_id")
        advName = rs.getString("adv_name").toInt
        ideaId = rs.getString("idea_id")
        firstLevel = rs.getString("first_level")
        secondLevel = rs.getString("second_level")
      }
    }
    ps.close();
    //android设备中的pkgCode，激活日志和广告点击日志是相同的，这里取的是激活日志中的
    tp10 = new Tuple10(jg, pkg_id, "", "", advName, ideaId, firstLevel, secondLevel, 0, "")
    return tp10
  }


  /**
    * 插入渠道点击信息
    *
    * @param pkgCode
    * @param imei
    * @param ts
    * @param os
    * @param url
    */
  def insertChannelClickDetail(pkgCode: String, imei: String, imei_md5: String, ts: String, os: String, url: String, gameId: String, adv_name: Int, ip: String, deviceType: String, systemVersion: String, channel_main_id: Int, channel_name: String, conn: Connection) = {
    val instSql = "insert into bi_new_merge_ad_click_detail(pkg_id,imei,imei_md5_upper,ts,os,callback,game_id,adv_name,ip,deviceType,systemVersion,channel_main_id,channel_name) values(?,?,?,?,?,?,?,?,?,?,?,?,?) "
    val ps: PreparedStatement = conn.prepareStatement(instSql)
    //insert
    ps.setString(1, pkgCode)
    ps.setString(2, imei)
    ps.setString(3, imei_md5)
    ps.setString(4, ts)
    ps.setString(5, os)
    ps.setString(6, url)
    ps.setString(7, gameId)
    ps.setInt(8, adv_name)
    ps.setString(9, ip)
    ps.setString(10, deviceType)
    ps.setString(11, systemVersion)
    ps.setInt(12, channel_main_id)
    ps.setString(13, channel_name)
    ps.executeUpdate()
    ps.close()
  }

  /**
    * 插入媒介点击信息
    *
    * @param pkgCode
    * @param imei
    * @param ts
    * @param os
    * @param url
    */
  def insertMediumClickDetail(pkgCode: String, imei: String, imei_md5: String, ts: String, os: String, url: String, gameId: String, adv_name: Int, ip: String, deviceType: String, systemVersion: String, androidID: String, conn: Connection) = {
    val instSql = "insert into bi_new_merge_ad_click_detail(pkg_id,imei,imei_md5_upper,ts,os,callback,game_id,adv_name,ip,deviceType,systemVersion,androidID) values(?,?,?,?,?,?,?,?,?,?,?,?) "
    val ps: PreparedStatement = conn.prepareStatement(instSql)
    //insert
    ps.setString(1, pkgCode)
    ps.setString(2, imei)
    ps.setString(3, imei_md5)
    ps.setString(4, ts)
    ps.setString(5, os)
    ps.setString(6, url)
    ps.setString(7, gameId)
    ps.setInt(8, adv_name)
    ps.setString(9, ip)
    ps.setString(10, deviceType)
    ps.setString(11, systemVersion)
    ps.setString(12, androidID)
    ps.executeUpdate()
    ps.close()
  }


  /**
    * 通过 game_id + imei 匹配更新广告监测点击信息
    *
    * @param pkg_id
    * @param imei
    * @param clickTime
    */
  def updateMediumClickByImei(pkg_id: String, game_id: Int, imei: String, clickTime: String, callback: String, conn: Connection) = {
    //在同一个游戏下，如果一个imei在多个分包都有点击而且没有激活，那么每次点击都会将之前点击的分包id更新
    val instSql = "update bi_new_merge_ad_click_detail set ts=?,callback=?,pkg_id=? where game_id=? and imei=? and matched=0"
    val ps: PreparedStatement = conn.prepareStatement(instSql)
    ps.setString(1, clickTime)
    ps.setString(2, callback)
    ps.setString(3, pkg_id)
    ps.setInt(4, game_id)
    ps.setString(5, imei)
    ps.executeUpdate()
    ps.close();
  }

  /**
    * 通过pkg_id+ip+devType+sysVersion匹配更新广告监测点击信息
    *
    * @param pkg_id
    * @param ip
    * @param devType
    * @param sysVersion
    * @param clickTime
    * @param callback
    * @param conn
    */
  def updateMomoClickByIpAndUa(pkg_id: String, ip: String, devType: String, sysVersion: String, clickTime: String, callback: String, conn: Connection) = {
    val updateClick = "update bi_new_merge_ad_click_detail set ts=?,callback=? where pkg_id=? and ip=? and deviceType=? and systemVersion=?"
    val pstmt = conn.prepareStatement(updateClick)
    pstmt.setString(1, clickTime)
    pstmt.setString(2, callback)
    pstmt.setString(3, pkg_id)
    pstmt.setString(4, ip)
    pstmt.setString(5, devType)
    pstmt.setString(6, sysVersion)
    pstmt.executeUpdate()
    pstmt.close()
  }

}
