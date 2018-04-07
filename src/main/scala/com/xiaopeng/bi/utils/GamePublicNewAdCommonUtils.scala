package com.xiaopeng.bi.utils

import java.sql.Connection
import java.text.SimpleDateFormat
import java.util.Date

import redis.clients.jedis.Jedis

/**
  * Created by bigdata on 18-3-8.
  */
object GamePublicNewAdCommonUtils {
  /**
    * 计算留存，按设备统计
    *
    * @param imei_first_reg_time
    * @param login_time

    * @return
    */
  def getLoginRetainDayDiff(imei_first_reg_time: String, login_time: String): Int = {
    var retainedDay = 0
    val simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val reg_date = simpleDateFormat.parse(imei_first_reg_time)
    val login_date = simpleDateFormat.parse(login_time)
    //计算时间天数差
    retainedDay = ((login_date.getTime - reg_date.getTime) / (1000 * 3600 * 24)).toInt

    retainedDay
  }

//  def main(args: Array[String]): Unit = {
//    getLoginRetainDayDiff()
//  }


  /**
    * 缓存imei在当天的登录次数
    *
    * @param login_date
    * @param game_id
    * @param pkg_id
    * @param imei
    * @param jedis12
    * @return
    */
  def getImeiTodayLoginNum(login_date: String, game_id: Int, pkg_id: String, imei: String, jedis12: Jedis): Int = {
    //默认登录次数为1
    val login_num = 1
    var before_login_num = 0
    if (!jedis12.exists("NewAdLoginNum|" + login_date + "|" + game_id + "|" + pkg_id + "|" + imei)) {
      jedis12.set("NewAdLoginNum|" + login_date + "|" + game_id + "|" + pkg_id + "|" + imei, login_num + "")
    } else {
      //查询之前的登录次数
      before_login_num = jedis12.get("NewAdLoginNum|" + login_date + "|" + game_id + "|" + pkg_id + "|" + imei).toInt

      jedis12.set("NewAdLoginNum|" + login_date + "|" + game_id + "|" + pkg_id + "|" + imei, before_login_num + login_num + "")
    }
    jedis12.expire("NewAdLoginNum|" + login_date + "|" + game_id + "|" + pkg_id + "|" + imei, 3600 * 25)
    before_login_num + login_num
  }


}
