package com.xiaopeng.bi.utils.dao

import java.sql.{Connection, PreparedStatement}
import java.util

import com.xiaopeng.bi.checkdata.MissInfo2Redis
import redis.clients.jedis.Jedis

/**
  * Created by Administrator on 2016/7/15.
  */
object GamePublicDao {

  /**
    * @param gameAccount
    * @param order_date
    * @param expand_channel
    * @param game_id
    * @param jedis
    * @return
    */
  def isCurrentDayRegiAndRecharge(gameAccount:String,order_date: String, expand_channel: String, game_id: Int, jedis: Jedis):Int =
  {
    var res = 0
    val arr = jedis.hgetAll(gameAccount)
    val reg_time = arr.get("reg_time")
    val expandChannel = arr.get("expand_channel")
    if ((reg_time.contains(order_date)) ) {
      res = 1
    } else res = 0
    return res
  }


  /**
    * 新增充值账号，是则返回1，否则0
    *
    * @param orderDate
    * @param gameAccount
    * @param pkgCode
    * @return
    */
  def isNewRechargeAccountDay(orderDate: String, gameAccount: String, pkgCode: String, gameId: Int, isCurrentDayRegiAndRecharge: Int, jedis: Jedis): Int = {
    var res = 1

    // jedis.select(3)
    if (isCurrentDayRegiAndRecharge == 1 && (!jedis.exists("isNewRechargeAccountDay|" + orderDate + "|" + gameAccount + "|" + pkgCode + "|" + gameId.toString))) {
      res = 1
    } else res = 0

    return res
  }


  /**
    * 新增充值设备，是则返回1，否则0
    *
    * @param orderDate
    * @param imei
    * @param pkgCode
    * @return
    */
  def isNewRechargeDevDay(orderDate: String, imei: String, pkgCode: String, gameId: Int, isNewRegiDevDay: Int, isNewRegiAccountDay: Int, jedis: Jedis): Int = {
    var res = 1
    // jedis.select(3)
    if (isNewRegiDevDay == 1 && isNewRegiAccountDay == 1 && (!jedis.exists("isNewRechargeDevDay|" + orderDate + "|" + imei + "|" + pkgCode + "|" + gameId.toString))) {
      res = 1
    } else res = 0
    return res
  }


  /**
    * 新增充值金额，是则返回1，否则0
    *
    * @param orderDate
    * @param gameAccount
    * @param pkgCode
    * @return
    */
  def rechargeAmountDay(orderDate: String, gameAccount: String, pkgCode: String, isCurrentDayRegiAndRecharge: Int, oriPrice: Float): Double = {
    var res: Double = 0.0
    if (isCurrentDayRegiAndRecharge== 1) {
      res = oriPrice
    }
    return res
  }


  /**
    * 是否当日注册账号
    *
    * @param gameAccount
    * @return
    */
  def isNewRegiAccountDay(gameAccount: String, regiDate: String, isNewRegiDevDay: Int, pkgId: String, Imei: String, jedis: Jedis,arr:util.Map[String, String] ): Int = {
    var res = 0
    var reg_time = arr.get("reg_time")
    val expandChannel = arr.get("expand_channel")
    val imei = arr.get("imei")
    if (reg_time == null) {
      val s = MissInfo2Redis.checkAccount(gameAccount)
      reg_time = jedis.hget(gameAccount, "reg_time")
    }
    if ((reg_time.contains(regiDate)) && isNewRegiDevDay == 1 && imei != null && expandChannel.equals(pkgId) && imei.equals(Imei)) {
      res = 1
    } else res = 0
    return res
  }

  /**
    * 判断是否为今天已经登陆过，若登陆过则不再计算
    *
    * @param pkgCode
    * @param gameAccount
    * @param publishDate
    */
  def isLoginAccountDay(pkgCode: String, gameAccount: String, publishDate: String, gameId: Int, jedis: Jedis): Int = {
    var res = 0
    if (!jedis.exists("isLoginAccountDay|" + publishDate + "|" + gameAccount + "|" + pkgCode + "|" + gameId.toString)) {
      res = 1
    }
    return res
  }

  /**
    * 判断是否为今天已经登陆过，若登陆过则不再计算
    *
    * @param pkgCode
    * @param gameAccount
    * @param publishTime
    */
  def isLoginAccountHour(pkgCode: String, gameAccount: String, publishTime: String, gameId: Int, jedis: Jedis): Int = {
    var res = 1
    // jedis.select(3)
    if (jedis.exists("isLoginAccountHour|" + publishTime + "|" + gameAccount + "|" + pkgCode + "|" + gameId.toString)) {
      res = 0
    }
    return res
  }

  /**
    * 判断是否为今天已经登陆过，若登陆过则不再计算
    *
    * @param pkgCode
    * @param imei
    * @param publishDate
    */
  def isLoginDevDay(pkgCode: String, imei: String, publishDate: String, gameId: Int, jedis: Jedis): Int = {
    var res = 1

    // jedis.select(3)
    if (jedis.exists("isLoginDevDay|" + publishDate + "|" + imei + "|" + pkgCode + "|" + gameId.toString)) {
      res = 0
    }
    return res
  }

  /**
    * 判断是否为新增注册活跃设备
    *
    * @param pkgCode
    * @param imei
    */
  def isNewActiveDevDay(pkgCode: String, imei: String, publicDate: String, gameId: Int, jedis: Jedis): Int = {
    var res = 0
    // jedis.select(3)
    var jg = jedis.get("isNewLgDev|" + publicDate + "|" + imei + "|" + pkgCode + "|" + gameId.toString)

    if (jg != null) {
      if (jg.equals("1")) {
        res = 1
      } else {
        res = 0
      }
      jg = (jg.toInt + 1).toString()
      jedis.set("isNewLgDev|" + publicDate + "|" + imei + "|" + pkgCode + "|" + gameId.toString, jg)
      jedis.expire("isNewLgDev|" + publicDate + "|" + imei + "|" + pkgCode + "|" + gameId.toString, 3600 * 24)
    }
    return res
  }

  /**
    * 从库中判断是否为新登录设备
    *
    * @param pkgCode
    * @return
    */
  def isNewLgDevDay(pkgCode: String, imei: String, publishDate: String, gameId: Int, isNewRegiDevDay: Int, isNewRegiAccountDay: Int, jedis: Jedis): Int = {
    var res = 0
    //为当天新增注册设备并且今天第一次登录（今天第二次登录等不再统计）
    if (isNewRegiDevDay == 1 && isNewRegiAccountDay == 1 && (!jedis.exists("isNewLgDev|" + publishDate + "|" + imei + "|" + pkgCode + "|" + gameId.toString)))
      res = 1
    return res
  }

  /**
    * 从库中判断是否为新注册设备
    *
    * @param pkgCode
    * @return
    */
  def isNewRegiDevDay(pkgCode: String, imei: String, publishDate: String, medium_channel: String, ad_site_channel: String, gameId: Int, conn: Connection): Int = {
    var res = 0
    val sql2Mysql = "select left(regi_hour,10) as publish_date from bi_gamepublic_regi_detail " +
      "where ad_label=? and imei=? and parent_channel=? and child_channel=? and game_id=? order by regi_hour asc limit 1"
    val ps: PreparedStatement = conn.prepareStatement(sql2Mysql)
    ps.setString(1, pkgCode)
    ps.setString(2, imei)
    ps.setString(3, medium_channel)
    ps.setString(4, ad_site_channel)
    ps.setInt(5, gameId)
    val result = ps.executeQuery()
    while (result.next()) {
      if (result.getString("publish_date").equals(publishDate)) {
        res = 1
      } else res = 0
    }
    ps.close()
    return res
  }

  /**
    * 从库中判断是否为新账号
    *
    * @param pkgCode
    * @return
    */
  def isNewLgAccountDay(pkgCode: String, gameAccount: String, publishDate: String, gameId: Int, isNewRegiDevDay: Int, isNewRegiAccountDay: Int, jedis: Jedis): Int = {
    var res = 1
    // jedis.select(3)
    //第一次当日新增注册设备，账号为当天注册，只取第一次登录（不再算多次登录）
    if (isNewRegiDevDay == 1 && isNewRegiAccountDay == 1 && (!jedis.exists("isLoginAccountDay|" + publishDate + "|" + gameAccount + "|" + pkgCode + "|" + gameId.toString))) {
      res = 1
    } else res = 0
    return res
  }

  /**
    * 加载数据到xiaopen2bi主数据库，推送数据到bi_gamepublic_actions的登录相关字段
    *
    * @param tp17
    */
  def loginActionsByDayProcessDB(tp17: (String, String, Int, String, String, String, String, String, String, String, String, Int, Int, Int, Int, Int, Int, String, String), conn: Connection, pkgid: String) = {
    val sql2Mysql = "insert into bi_gamepublic_base_day_kpi" +
      "(publish_date,parent_game_id,child_game_id,medium_channel,ad_site_channel,pkg_code,medium_account," +
      "promotion_channel,promotion_mode,head_people,os,group_id,new_login_device_num,new_login_account_num,new_active_device_num," +
      "dau_device_num,dau_account_num) " +
      "values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) " +
      "on duplicate key update new_login_device_num=new_login_device_num+?,new_login_account_num=new_login_account_num+?," +
      "new_active_device_num=new_active_device_num+?,dau_device_num=dau_device_num+?,dau_account_num=dau_account_num+?";
    val ps: PreparedStatement = conn.prepareStatement(sql2Mysql)
    /*insert*/
    ps.setString(1, tp17._1)
    ps.setString(2, tp17._2)
    ps.setInt(3, tp17._3) //game_id
    ps.setString(4, tp17._4)
    ps.setString(5, tp17._5)
    ps.setString(6, tp17._6)
    ps.setString(7, tp17._7)
    ps.setString(8, tp17._8)
    ps.setString(9, tp17._9)
    ps.setString(10, tp17._10)
    ps.setString(11, tp17._11) //os
    ps.setInt(12, tp17._12) //group_id
    ps.setInt(13, tp17._13)
    ps.setInt(14, tp17._14)
    ps.setInt(15, tp17._15)
    ps.setInt(16, tp17._16)
    ps.setInt(17, tp17._17)
    /*update*/
    ps.setInt(18, tp17._13)
    ps.setInt(19, tp17._14)
    ps.setInt(20, tp17._15)
    ps.setInt(21, tp17._16)
    ps.setInt(22, tp17._17)
    ps.executeUpdate()
    ps.close()


  }

  /**
    * 把登录数据进行打标志存放
    *
    * @param gameAccount
    * @param publicDate
    * @param pkgCode
    * @param imei
    * @param isNewLgDev
    * @param isNewLgAccount
    */
  def loginInfoToMidTb(gameAccount: String, publicDate: String, pkgCode: String, imei: String, isNewLgDev: Int, isNewLgAccount: Int, pkgid: String, gameId: Int, jedis: Jedis) = {
    // jedis.select(3)
    /*是否新设备判断是否进一步操作*/
    if (isNewLgDev == 1) {
      /*插入当天新增登录设备到redis，用来判断是否新增活跃设备*/

      jedis.set("isNewLgDev|" + publicDate + "|" + imei + "|" + pkgid + "|" + gameId.toString, "1")
      jedis.expire("isNewLgDev|" + publicDate + "|" + imei + "|" + pkgid + "|" + gameId.toString, 3600 * 50)
    }

    /*临时存储账号是否今天登录过*/
    if (!jedis.exists("isLoginAccountDay|" + publicDate + "|" + gameAccount + "|" + pkgid + "|" + gameId.toString)) {
      jedis.set("isLoginAccountDay|" + publicDate + "|" + gameAccount + "|" + pkgid + "|" + gameId.toString, pkgid)
      jedis.expire("isLoginAccountDay|" + publicDate + "|" + gameAccount + "|" + pkgid + "|" + gameId.toString, 3600 * 50)
    }

    /*临时存储设备是否今天登录过*/
    if (!jedis.exists("isLoginDevDay|" + publicDate + "|" + imei + "|" + pkgid + "|" + gameId.toString)) {
      jedis.set("isLoginDevDay|" + publicDate + "|" + imei + "|" + pkgid + "|" + gameId.toString, pkgid)
      jedis.expire("isLoginDevDay|" + publicDate + "|" + imei + "|" + pkgid + "|" + gameId.toString, 3600 * 50)
    }
  }

  /**
    * 新增充值记录
    *
    * @param gameAccount
    * @param publicDate
    * @param pkgCode
    * @param imei
    * @param isNewRechargeDevDay
    * @param isNewRechargeAccountDay
    */
  def rechargeInfoToMidTb(gameAccount: String, publicDate: String, pkgCode: String, imei: String, isNewRechargeDevDay: Int, isNewRechargeAccountDay: Int, expandChannel: String, gameId: Int, jedis: Jedis, conn: Connection) = {
    // jedis.select(3)
    /*是否新设备判断是否进一步操作*/
    if (isNewRechargeDevDay == 1) {
      /*redis 临时存放新增充值设备*/
      jedis.set("isNewRechargeDevDay|" + publicDate + "|" + imei + "|" + expandChannel + "|" + gameId.toString, expandChannel)
      jedis.expire("isNewRechargeDevDay|" + publicDate + "|" + imei + "|" + expandChannel + "|" + gameId.toString, 3600 * 48)

    }
    /*若新增注册登录账号，则进行永久存储标示处理*/
    if (isNewRechargeAccountDay == 1) {
      /*插入表来永久标示是否为历史新增注册设备*/
      jedis.set("isNewRechargeAccountDay|" + publicDate + "|" + gameAccount + "|" + expandChannel + "|" + gameId.toString, expandChannel)
      jedis.expire("isNewRechargeAccountDay|" + publicDate + "|" + gameAccount + "|" + expandChannel + "|" + gameId.toString, 3600 * 48)
    }
  }

  /**
    * 推送数据到bi_gamepublic_basekpi表的login_accounts字段
    *
    * @param tp18
    */
  def loginAccountByDayProcessDB(tp18: (String, String, Int, String, String, String, String, String, String, String, String,
    Int, Int, Int, Int, Int, Int, String, String), conn: Connection) = {
    val sql2Mysql = "insert into bi_gamepublic_basekpi(publish_time,parent_game_id,game_id,parent_channel,child_channel" +
      ",ad_label,medium_account,promotion_channel,promotion_mode,head_people,login_accounts,os,group_id) values(?,?,?,?,?,?,?,?,?,?,?,?,?) " +
      "on duplicate key update login_accounts=login_accounts+?";
    val ps: PreparedStatement = conn.prepareStatement(sql2Mysql)
    if (tp18._17 > 0) {
      //insert
      ps.setString(1, tp18._1)
      ps.setString(2, tp18._2)
      ps.setInt(3, tp18._3) //gameid
      ps.setString(4, tp18._4)
      ps.setString(5, tp18._5)
      ps.setString(6, tp18._6)
      ps.setString(7, tp18._7)
      ps.setString(8, tp18._8)
      ps.setString(9, tp18._9)
      ps.setString(10, tp18._10)
      ps.setInt(11, tp18._17) //login_accounts
      ps.setString(12, tp18._11) //os
      ps.setInt(13, tp18._12) //group_id
      //update
      ps.setInt(14, tp18._17) //login_accounts
      ps.executeUpdate()
    }
    ps.close()

  }

  /**
    * 推送数据到bi_gamepublic_basekpi表的dau_account_num字段
    *
    * @param jedis
    * @param tu14
    */
  def loginAccountDauByHourProcessDB(tu14: (String, String, Int, String, String, String, String, String, String, String, String, Int, Int, String), conn: Connection, pkgid: String) = {
    val sql2Mysql = "insert into bi_gamepublic_basekpi(publish_time,parent_game_id,game_id,parent_channel,child_channel" +
      ",ad_label,medium_account,promotion_channel,promotion_mode,head_people,os,group_id,dau_account_num) values(?,?,?,?,?,?,?,?,?,?,?,?,?) " +
      "on duplicate key update dau_account_num=dau_account_num+?";
    val ps: PreparedStatement = conn.prepareStatement(sql2Mysql)
    if (tu14._13 > 0) {
      //insert
      ps.setString(1, tu14._1)
      ps.setString(2, tu14._2)
      ps.setInt(3, tu14._3) //gameid
      ps.setString(4, tu14._4)
      ps.setString(5, tu14._5)
      ps.setString(6, tu14._6)
      ps.setString(7, tu14._7)
      ps.setString(8, tu14._8)
      ps.setString(9, tu14._9)
      ps.setString(10, tu14._10)
      ps.setString(11, tu14._11) //0s
      ps.setInt(12, tu14._12) //group_id
      ps.setInt(13, tu14._13) //dau_account_num
      ps.setInt(14, tu14._13) //dau_account_num
      ps.executeUpdate()

    }
    ps.close()
  }


  /**
    * 用户消费，推送数据到bi_gamepublic_base_day_kpi表的新增消费字段
    *
    * @param jedis
    * @param tu14
    */
  def rechargeActionsByDay(jedis: Jedis, tu14: (String, String, Int, String, String, String, String, String, String, String, Int, Int, Int, Double, String, String, String), conn: Connection, expand_channel: String) = {
    val sql2Mysql = "insert into bi_gamepublic_base_day_kpi(publish_date,parent_game_id,child_game_id,medium_channel,ad_site_channel,pkg_code" +
      ",medium_account,promotion_channel,promotion_mode,head_people,group_id,new_pay_people_num,new_pay_account_num,new_pay_money,os) " +
      "values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) " +
      "on duplicate key update new_pay_people_num=new_pay_people_num+?,new_pay_account_num=new_pay_account_num+?,new_pay_money=new_pay_money+?";
    val ps: PreparedStatement = conn.prepareStatement(sql2Mysql)

    //insert
    ps.setString(1, tu14._1)
    ps.setString(2, tu14._2)
    ps.setInt(3, tu14._3) //gameid
    ps.setString(4, tu14._4)
    ps.setString(5, tu14._5)
    ps.setString(6, tu14._6)
    ps.setString(7, tu14._7)
    ps.setString(8, tu14._8)
    ps.setString(9, tu14._9)
    ps.setString(10, tu14._10)
    ps.setInt(11, tu14._11) //group_id
    ps.setInt(12, tu14._12)
    ps.setInt(13, tu14._13)
    ps.setFloat(14, tu14._14.toFloat) //若是新增充值账号或者新增充值设备才算新增充值
    ps.setString(15, tu14._17) //os
    //update
    ps.setInt(16, tu14._12)
    ps.setInt(17, tu14._13)
    ps.setFloat(18, tu14._14.toFloat) //若是新增充值账号或者新增充值设备才算新增充值
    ps.executeUpdate()
    /*对数据进行中间表存放,game_account,public_date,pkg_code,imei,isNewRechargeDevDay,isNewRechargeAccountDay*/
    rechargeInfoToMidTb(tu14._16, tu14._1, tu14._6, tu14._15, tu14._12, tu14._13, expand_channel, tu14._3, jedis, conn)
    ps.close()
  }

}
