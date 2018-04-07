package com.xiaopeng.bi.utils

import java.sql.{Connection, Statement}
import java.text.SimpleDateFormat
import java.util

import org.apache.spark.sql.hive.HiveContext
import redis.clients.jedis.Jedis

/**
  * Created by bigdata on 18-2-26.
  */
object AllianceUtils {

  def isSandBoxOrderId(order_id: String, stat: Statement): Boolean = {
    var isSandBoxOrderId = false
    val sql = "select channel_order_sn from common_sdk_order_tran where pyw_order_sn = '" + order_id + "' limit 1"
    val rs = stat.executeQuery(sql)
    if (rs.next()) {
      val channel_order_sn = rs.getString("channel_order_sn")
      val channel_order_sn_sql = "select is_sandbox_pay from common_sdk_order_pay where channel_order_sn = '" + channel_order_sn + "'"
      val payRs = stat.executeQuery(channel_order_sn_sql)
      if (payRs.next()) {
        val is_sandbox_pay = payRs.getString("is_sandbox_pay").toInt
        // 1 : 沙河订单
        if (is_sandbox_pay == 1) {
          isSandBoxOrderId = true
        }
      }
      payRs.close()
    }
    rs.close()
    isSandBoxOrderId
  }

  def main(args: Array[String]): Unit = {

  }


  def getAllianceAccountDiffDay(login_time: String, reg_time: String): Int = {
    var retainedDay = 0
    val simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val reg_date = simpleDateFormat.parse(reg_time)
    val login_date = simpleDateFormat.parse(login_time)
    retainedDay = ((login_date.getTime - reg_date.getTime) / (1000 * 3600 * 24)).toInt
    retainedDay
  }


  def newPayAccountNum(pay_account_num: Int, game_account: String, order_time: String, reg_time: String, game_id: Int): Int = {
    var new_pay_account_num = 0
    val reg_date = reg_time.substring(0, 10)
    if (pay_account_num == 1) {
      val order_date = order_time.substring(0, 10)
      if (order_date.equals(reg_date)) {
        new_pay_account_num = 1
      }
    }
    new_pay_account_num
  }

  def payDeviceNum(imei: String, order_time: String, game_id: Int, alliance_id: String, jedis10: Jedis): Int = {
    var pay_device_num = 0
    val order_date = order_time.substring(0, 10)
    if (!jedis10.exists("allianceOrderDevice|" + game_id + "|" + alliance_id + "|" + imei + "|" + order_date)) {
      pay_device_num = 1
    }
    pay_device_num
  }


  def cacheAllianceTodayOrderDevice(game_id: Int, alliance_id: String, imei: String, order_time: String, jedis10: Jedis) = {
    val order_date = order_time.substring(0, 10)
    jedis10.set("allianceOrderDevice|" + game_id + "|" + alliance_id + "|" + imei + "|" + order_date, order_date)
    jedis10.expire("allianceOrderDevice|" + game_id + "|" + alliance_id + "|" + imei + "|" + order_date, 3600 * 25)
  }

  def cacheAllianceTodayOrderAccount(game_id: Int, alliance_id: String, game_account: String, order_time: String, jedis10: Jedis) = {
    val order_date = order_time.substring(0, 10)
    jedis10.set("allianceOrderAccount|" + game_id + "|" + alliance_id + "|" + game_account + "|" + order_date, order_date)
    jedis10.expire("allianceOrderAccount|" + game_id + "|" + alliance_id + "|" + game_account + "|" + order_date, 3600 * 25)
  }

  def payAccountNum(game_account: String, order_time: String, game_id: Int, alliance_id: String, jedis10: Jedis): Int = {
    var pay_account_num = 0
    val order_date = order_time.substring(0, 10)
    if (!jedis10.exists("allianceOrderAccount|" + game_id + "|" + alliance_id + "|" + game_account + "|" + order_date)) {
      pay_account_num = 1
    }
    pay_account_num

  }


  /**
    * 缓存当天登录帐号到redis 25个小时
    *
    * @param game_id
    * @param game_account
    * @param login_time
    * @param alliance_id
    * @param jedis10
    * @return
    */
  def cacheAllianceTodayLoginAccount(game_id: Int, game_account: String, login_time: String, alliance_id: String, jedis10: Jedis) = {
    val login_date = login_time.substring(0, 10)
    jedis10.set("allianceLoginAccount|" + game_id + "|" + alliance_id + "|" + game_account + "|" + login_date, game_account)
    jedis10.expire("allianceLoginAccount|" + game_id + "|" + alliance_id + "|" + game_account + "|" + login_date, 3600 * 25)
  }


  /**
    * 缓存当天登录设备到redis 25个小时
    *
    * @param game_id
    * @param imei
    * @param login_time
    * @param alliance_id
    * @param jedis10
    * @return
    */
  def cacheAllianceTodayLoginDevice(game_id: Int, imei: String, login_time: String, alliance_id: String, jedis10: Jedis) = {
    val login_date = login_time.substring(0, 10)
    jedis10.set("allianceLoginDevice|" + game_id + "|" + alliance_id + "|" + imei + "|" + login_date, imei)
    jedis10.expire("allianceLoginDevice|" + game_id + "|" + alliance_id + "|" + imei + "|" + login_date, 3600 * 25)
  }


  def dauDeviceNum(game_id: Int, imei: String, login_time: String, alliance_id: String, jedis10: Jedis): Int = {
    var dau_device_num = 0
    val login_date = login_time.substring(0, 10)
    if (!jedis10.exists("allianceLoginDevice|" + game_id + "|" + alliance_id + "|" + imei + "|" + login_date)) {
      dau_device_num = 1
    }
    dau_device_num
  }


  def dauAccountNum(game_id: Int, game_account: String, login_time: String, alliance_id: String, jedis10: Jedis): Int = {
    var dau_account_num = 0
    val login_date = login_time.substring(0, 10)
    if (!jedis10.exists("allianceLoginAccount|" + game_id + "|" + alliance_id + "|" + game_account + "|" + login_date)) {
      dau_account_num = 1
    }
    dau_account_num
  }


  /**
    * 查询设备第一次注册时间
    *
    * @param pay_device_num
    * @param imei
    * @param order_time
    * @param game_id

    * @return
    */
  def newPayDeviceNum(pay_device_num: Int, imei: String, order_time: String, game_id: Int, stmt:Statement): Int = {
    var new_pay_device_num = 0
    if (pay_device_num == 1) {
      val sql = "select regi_time from bi_gamepublic_alliance_regi_detail where imei = '" + imei + "' and game_id = '" + game_id + "' order by regi_time asc limit 1"
      val rs = stmt.executeQuery(sql)
      if (rs.next()) {
        val first_reg_time = rs.getString("regi_time")
        if (first_reg_time.substring(0, 10).equals(order_time.substring(0, 10))) {
          new_pay_device_num = 1
        }
      }
      rs.close()
    }
    new_pay_device_num
  }


  def getAllianceDimInfo(hiveContext: HiveContext, alliance_id: String) = {
    println("ccc:" + hiveContext)
    //获取联运游戏的维度信息
    val dimDF = hiveContext.sql("select parent_game_id,child_game_id,terrace_name,terrace_type,alliance_bag_id,os,head_people from alliance_dim where alliance_bag_id = '" + alliance_id + "' limit 1")
    val dimArr = dimDF.take(1)
    val dimRow = dimArr(0)
    val parent_game_id = dimRow.getAs[Int]("parent_game_id")
    val child_game_id = dimRow.getAs[Int]("child_game_id")
    val terrace_name = dimRow.getAs[String]("terrace_name")
    val terrace_type = dimRow.getAs[String]("terrace_type")
    val os = dimRow.getAs[Int]("os")
    val head_people = dimRow.getAs[String]("head_people")
    (parent_game_id, child_game_id, terrace_name, terrace_type, os, head_people)
  }


}
