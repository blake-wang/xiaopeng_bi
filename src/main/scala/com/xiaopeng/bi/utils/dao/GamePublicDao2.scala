package com.xiaopeng.bi.utils.dao

import java.sql.{Connection, PreparedStatement}
import java.text.SimpleDateFormat
import java.util
import java.util.Date

import com.xiaopeng.bi.checkdata.MissInfo2Redis
import redis.clients.jedis.Jedis

/**
  * Created by Administrator on 2016/7/15.
  */

object GamePublicDao2 {

  /**
    * 加载运营小时表Dau数据
    * pudate pargameid childgameid os groupid isNewLgDev  isNewLgAccount isLoginDevDay imei gameaccount regi_login_num dau_account_num publish_time hau_account_num
    * @param tp13
    * @param conn
    */
  def loginAccountByDayHourProcessDB(tp13: (String, String, Int, String, Int, Int, Int, Int, String, String, Int, Int, String,Int), conn: Connection) =
  {
    val sql2Mysql = "insert into bi_gamepublic_base_opera_hour_kpi" +
      "(publish_time,parent_game_id,child_game_id,os,group_id,dau_account_num) " +
      "values(?,?,?,?,?,?) " +
      "on duplicate key update dau_account_num=dau_account_num+?";
    val ps: PreparedStatement = conn.prepareStatement(sql2Mysql)
    /*insert*/
    ps.setString(1, tp13._13) //dateHour
    ps.setString(2, tp13._2) //parent_game_id
    ps.setInt(3, tp13._3) //game_id
    ps.setString(4, tp13._4) //os
    ps.setInt(5, tp13._5) //gropuid
    ps.setInt(6, tp13._14)//hau_account_num
    /*update*/
    ps.setInt(7, tp13._14)//hau_account_num
    ps.executeUpdate()
    ps.close()

  }


  /**
    * 根据游戏判断是否今天每小时的登录DAU
    *
    * @param publishTime
    * @param gameId
    * @param gameAccount
    * @param jedis
    * @return
    */
  def isLoginAccDayHourByGame(publishTime: String, gameId: Int, gameAccount: String, jedis: Jedis) :Int=

  {
    var res = 1
    if (jedis.exists("isLoginAccDayHourByGame|" + publishTime + "|" + gameAccount + "|" + gameId.toString)) {
      res = 0
    }else
    {
      jedis.set("isLoginAccDayHourByGame|" + publishTime + "|" + gameAccount + "|" + gameId.toString,"");
      jedis.expire("isLoginAccDayHourByGame|" + publishTime + "|" + gameAccount + "|" + gameId.toString,3600*24);
      res=1
    }
    return res
  }


  /**
    * 根据游戏判断是否今天第一次登录
    *
    * @param publishDate
    * @param gameId
    * @param gameAccount
    * @param jedis
    * @return
    */
  def isLoginAccDayByGame(publishDate: String, gameId: Int,gameAccount:String, jedis: Jedis):Int=
  {
    var res = 1
    if (jedis.exists("isLoginAccDayByGame|" + publishDate + "|" + gameAccount + "|" + gameId.toString)) {
      res = 0
    }else
    {
      jedis.set("isLoginAccDayByGame|" + publishDate + "|" + gameAccount + "|" + gameId.toString,"");
      jedis.expire("isLoginAccDayByGame|" + publishDate + "|" + gameAccount + "|" + gameId.toString,3600*25);
      res=1
    }
    return res

  }

  /**
    * 推送留存数据到表
    *
    * @param tp9
    * @param conn
    */
  def remaindByDayProcessDB(tp9: (String, String, Int, String, Int, Int, Int, Int,Int), conn: Connection) ={
    val sql2Mysql = "update bi_gamepublic_base_opera_kpi" +
      " set retained_1day=retained_1day+?,retained_3day=retained_3day+?,retained_7day=retained_7day+?,retained_30day=retained_30day+?" +
      " where  publish_date=? and child_game_id=? ";
    val ps: PreparedStatement = conn.prepareStatement(sql2Mysql)
    ps.setInt(1, tp9._6) //retained_1day
    ps.setInt(2, tp9._7) //retained_3day
    ps.setInt(3, tp9._8) //retained_7day
    ps.setInt(4, tp9._9) //retained_30day
    ps.setString(5, tp9._1) //date
    ps.setInt(6, tp9._3) //child_game_id

    ps.executeUpdate()
    ps.close()
  }

  /**
    * 登录和注册时间差天数
    *
    * @param accountInfo
    * @param publish_date
    * @param isLoginAccountDay
    * @return
    */
  def getRemaindDays(accountInfo: util.Map[String, String], publish_date: String, isLoginAccountDay: Int):Int ={
    var jg=100
    val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    if(isLoginAccountDay==1) {
      val regTime =accountInfo.get("reg_time")
      val regDate =dateFormat.parse(if(regTime!=null&&(!regTime.equals(""))) regTime.substring(0,10) else "0000-00-00")
      val loginDate =dateFormat.parse(publish_date)
      jg=((loginDate.getTime-regDate.getTime)/(1000*3600*24)).toInt;
    }
    return jg
  }


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
    if ((reg_time.contains(order_date))) {
      res = 1
    } else res = 0
    return res
  }



  /**
    * 新增充值账号，是则返回1，否则0
    *
    * @param orderDate
    * @param gameAccount
    * @param gameId
    * @return
    */
  def isNewRechargeAccountDay(orderDate: String, gameAccount: String, gameId: Int, isCurrentDayRegiAndRecharge2: Int, jedis: Jedis): Int = {
    var res = 1

    if (isCurrentDayRegiAndRecharge2== 1 && (!jedis.exists("isNewRechargeAccountDay2|" + orderDate + "|" + gameAccount + "|" + gameId.toString))) {
      res = 1
    } else res = 0

    return res
  }


  /**
    * 新增充值设备，是则返回1，否则0
    *
    * @param orderDate
    * @param imei
    * @param gameId
    * @return
    */
  def isNewRechargeDevDay(orderDate: String, imei: String, gameId: Int, isNewRegiDevDay: Int, isNewRegiAccountDay: Int, jedis: Jedis): Int = {
    var res = 1
    // jedis.select(3)
    if (isNewRegiDevDay == 1 && isNewRegiAccountDay == 1 && (!jedis.exists("isNewRechargeDevDay2|" + orderDate + "|" + imei + "|" + gameId.toString))) {
      res = 1
    } else res = 0
    return res
  }


  /**
    * 新增充值金额，是则返回1，否则0
    *
    * @param orderDate
    * @param gameAccount
    * @param oriPrice
    * @return
    */
  def rechargeAmountDay(orderDate: String, gameAccount: String, isCurrentDayRegiAndRecharge2: Int, oriPrice: Float): Double = {
    var res: Double = 0.0
    if (isCurrentDayRegiAndRecharge2 == 1) {
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
  def isNewRegiAccountDay(gameAccount: String, regiDate: String, isNewRegiDevDay: Int, Imei: String, jedis: Jedis): Int = {
    var res = 0
    val arr = jedis.hgetAll(gameAccount)
    var reg_time = arr.get("reg_time")
    val imei = arr.get("imei")
    if (reg_time == null) {
      val s = MissInfo2Redis.checkAccount(gameAccount)
      reg_time = jedis.hget(gameAccount, "reg_time")
    }
    if ((reg_time.contains(regiDate)) && isNewRegiDevDay == 1 && imei != null && imei.equals(Imei)) {
      res = 1
    } else res = 0
    return res
  }


  /**
    * 判断是否为今天已经登陆过，若登陆过则不再计算
    *
    * @param gameId
    * @param imei
    * @param publishDate
    */
  def isLoginDevDay(imei: String, publishDate: String, gameId: Int, jedis: Jedis): Int = {
    var res = 1
    if (jedis.exists("isLoginDevDay2|" + publishDate + "|" + imei + "|" + gameId.toString)) {
      res = 0
    }
    return res
  }


  /**
    * 从库中判断是否为新登录设备
    *
    * @param gameId
    * @return
    */
  def isNewLgDevDay(imei: String, publishDate: String, gameId: Int, isNewRegiDevDay: Int, isNewRegiAccountDay: Int, jedis: Jedis): Int = {
    var res = 0
    //为当天新增注册设备并且今天第一次登录（今天第二次登录等不再统计）
    if (isNewRegiDevDay == 1 && isNewRegiAccountDay == 1 && (!jedis.exists("isNewLgDev2|" + publishDate + "|" + imei + "|" + gameId.toString)))
      res = 1
    return res
  }

  /**
    * 从库中判断是否为新注册设备
    *
    * @param gameId
    * @return
    */
  def isNewRegiDevDay(imei: String, publishDate: String, gameId: Int, conn: Connection): Int = {
    var res = 0
    val sql2Mysql = "select left(regi_hour,10) as publish_date from bi_gamepublic_regi_detail " +
      "where imei=? and  game_id=? order by regi_hour asc limit 1"
    val ps: PreparedStatement = conn.prepareStatement(sql2Mysql)
    ps.setString(1, imei)
    ps.setInt(2, gameId)
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
    * @param gameId
    * @return
    */
  def isNewLgAccountDay(gameAccount: String, publishDate: String, gameId: Int, isNewRegiDevDay: Int, isNewRegiAccountDay: Int, jedis: Jedis): Int = {
    var res = 1
    //第一次当日新增注册设备，账号为当天注册，只取第一次登录（不再算多次登录）
    if (isNewRegiDevDay == 1 && isNewRegiAccountDay == 1 && (!jedis.exists("isLoginAccountDay2|" + publishDate + "|" + gameAccount + "|" + gameId.toString))) {
      res = 1
    } else res = 0
    return res
  }

  /**
    * 加载发行运营报表bi_gamepublic_base_opera_kpi表信息数据
    * pudate pargameid childgameid os groupid isNewLgDev  isNewLgAccount isLoginDevDay imei gameaccount regi_login_num dau_account_num publish_time hau_account_num
    *
    * @param tp13
    */
  def loginActionsByDayProcessDB(tp13: (String, String, Int, String, Int, Int, Int, Int, String, String,Int,Int,String,Int), conn: Connection) = {
    val sql2Mysql = "insert into bi_gamepublic_base_opera_kpi" +
      "(publish_date,parent_game_id,child_game_id,os,group_id,new_login_device_num,new_login_account_num,dau_device_num,regi_login_num,dau_account_num) " +
      "values(?,?,?,?,?,?,?,?,?,?) " +
      "on duplicate key update new_login_device_num=new_login_device_num+?,new_login_account_num=new_login_account_num+?,dau_device_num=dau_device_num+?,regi_login_num=regi_login_num+?,dau_account_num=dau_account_num+?";
    val ps: PreparedStatement = conn.prepareStatement(sql2Mysql)
    /*insert*/
    ps.setString(1, tp13._1) //date
    ps.setString(2, tp13._2) //parent_game_id
    ps.setInt(3, tp13._3) //game_id
    ps.setString(4, tp13._4) //os
    ps.setInt(5, tp13._5) //gropuid
    ps.setInt(6, tp13._6) //isNewLgDev
    ps.setInt(7, tp13._7) //isNewLgAccount
    ps.setInt(8, tp13._8) //isLoginDevDay
    ps.setInt(9, tp13._11)//regi_login_num
    ps.setInt(10, tp13._12)//dau_account_num
    /*update*/
    ps.setInt(11, tp13._6) //isNewLgDev
    ps.setInt(12, tp13._7) //isNewLgAccount
    ps.setInt(13, tp13._8) //isLoginDevDay
    ps.setInt(14, tp13._11)//regi_login_num
    ps.setInt(15, tp13._12)//dau_account_num
    ps.executeUpdate()
    ps.close()

  }

  /**
    * 把登录数据进行打标志存放
    *
    * @param gameAccount
    * @param publicDate
    * @param imei
    * @param isNewLgDev
    * @param isNewLgAccount
    */
  def loginInfoToMidTb(gameAccount: String, publicDate: String, imei: String, isNewLgDev: Int, isNewLgAccount: Int, gameId: Int, jedis: Jedis) = {
    // jedis.select(3)
    /*是否新设备判断是否进一步操作*/
    if (isNewLgDev == 1) {
      /*插入当天新增登录设备到redis，用来判断是否新增活跃设备*/

      jedis.set("isNewLgDev2|" + publicDate + "|" + imei + "|" + gameId.toString, "1")
      jedis.expire("isNewLgDev2|" + publicDate + "|" + imei + "|" + gameId.toString, 3600 * 50)
    }

    /*临时存储账号是否今天登录过*/
    if (!jedis.exists("isLoginAccountDay2|" + publicDate + "|" + gameAccount + "|" + gameId.toString)) {
      jedis.set("isLoginAccountDay2|" + publicDate + "|" + gameAccount + "|" + gameId.toString, gameId.toString)
      jedis.expire("isLoginAccountDay2|" + publicDate + "|" + gameAccount + "|" + gameId.toString, 3600 * 50)
    }

    /*临时存储设备是否今天登录过*/
    if (!jedis.exists("isLoginDevDay2|" + publicDate + "|" + imei + "|" + gameId.toString)) {
      jedis.set("isLoginDevDay2|" + publicDate + "|" + imei + "|" + gameId.toString, gameId.toString)
      jedis.expire("isLoginDevDay2|" + publicDate + "|" + imei + "|" + gameId.toString, 3600 * 50)
    }
  }

  /**
    * 新增充值记录
    *
    * @param gameAccount
    * @param publicDate
    * @param gameId
    * @param imei
    * @param isNewRechargeDevDay
    * @param isNewRechargeAccountDay
    */
  def rechargeInfoToMidTb(gameAccount: String, publicDate: String, imei: String, isNewRechargeDevDay: Int, isNewRechargeAccountDay: Int, gameId: Int, jedis: Jedis, conn: Connection) = {
    // jedis.select(3)
    /*是否新设备判断是否进一步操作*/
    if (isNewRechargeDevDay == 1) {
      /*redis 临时存放新增充值设备*/
      jedis.set("isNewRechargeDevDay2|" + publicDate + "|" + imei + "|" + gameId.toString, gameId.toString)
      jedis.expire("isNewRechargeDevDay2|" + publicDate + "|" + imei + "|" + gameId.toString, 3600 * 26)

    }
    /*若新增注册登录账号，则进行永久存储标示处理*/
    if (isNewRechargeAccountDay == 1) {
      /*插入表来永久标示是否为历史新增注册设备*/
      jedis.set("isNewRechargeAccountDay2|" + publicDate + "|" + gameAccount + "|" + gameId.toString, gameAccount)
      jedis.expire("isNewRechargeAccountDay2|" + publicDate + "|" + gameAccount + "|" + gameId.toString, 3600 * 26)
    }
  }


  /**
    * 用户消费，推送数据到bi_gamepublic_base_day_kpi表的新增消费字段
    * date, parent_game_id, game_id,group_id, isNewRechargeDevDay, isNewRechargeAccountDay ,rechargeAmountDay, imei, game_account, os
    *
    * @param jedis
    * @param tu9
    */

  def rechargeActionsByDay(jedis: Jedis, tu9: (String, String, Int, Int, Int, Int, Float, String, String, String), conn: Connection) = {
    val sql2Mysql = "insert into bi_gamepublic_base_opera_kpi(publish_date,parent_game_id,child_game_id," +
      "group_id,new_pay_people_num,new_pay_account_num,new_pay_money,os) " +
      "values(?,?,?,?,?,?,?,?) " +
      "on duplicate key update new_pay_people_num=new_pay_people_num+?,new_pay_account_num=new_pay_account_num+?,new_pay_money=new_pay_money+?";
    val ps: PreparedStatement = conn.prepareStatement(sql2Mysql)

    //insert
    ps.setString(1, tu9._1) //publish_date
    ps.setString(2, tu9._2) //parent_game_id
    ps.setInt(3, tu9._3) //gameid
    ps.setInt(4, tu9._4) //groupid
    ps.setInt(5, tu9._5) //isNewRechargeDevDay
    ps.setInt(6, tu9._6) //isNewRechargeAccountDay
    ps.setFloat(7, tu9._7) //new_pay_money
    ps.setString(8, tu9._10) //os
    //update
    ps.setInt(9, tu9._5) //isNewRechargeDevDay
    ps.setInt(10, tu9._6) //isNewRechargeAccountDay
    ps.setFloat(11, tu9._7.toFloat) //若是新增充值账号或者新增充值设备才算新增充值
    ps.executeUpdate()

    ps.close()
  }

}
