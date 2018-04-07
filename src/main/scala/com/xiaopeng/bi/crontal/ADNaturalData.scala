package com.xiaopeng.bi.crontal

import java.sql.{PreparedStatement, ResultSet}
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import com.xiaopeng.bi.utils.{FileUtil, JdbcUtil}
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat
import org.apache.log4j.Logger
import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}


/**
  * Created by denglh on 2016/8/24.
  * 对游戏发布数据报表统计-基本指标
  */
object ADNaturalData {
  //val logger = Logger.getLogger(TbVsHdfs.getClass)

  def main(args: Array[String]) {
    var currentday = args(0)
    val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val date: Date = dateFormat.parse(currentday)
    val cal: Calendar = Calendar.getInstance()
    cal.setTime(date)
    cal.add(Calendar.DATE, -1)
    val yesterday = dateFormat.format(cal.getTime())
    val d: Date = new Date();
    val hours = d.getHours

    //  0:00-2:59和8:00-8:59 执行昨天的数据
    //  6:30-8:00在执行kpi_offline的脚本，跑离线数据，等离线数据跑完后，执行此程序
    if (hours <= 2 || (hours >= 8 && hours < 9)) {
      currentday = yesterday
    }

    //处理ios数据
    var tp2 = getMedPkgGameInfo(2)
    if (!tp2.equals("0")) //对找不到的游戏过滤
    {
      doADADataPay(tp2, 2, currentday)
      doADADataActive(tp2, 2, currentday)
    }

    //处理android数据
    tp2 = getMedPkgGameInfo(1)
    if (!tp2.equals("0")) //对找不到的游戏过滤
    {
      doADADataPay(tp2, 1, currentday)
      doADADataActive(tp2, 1, currentday)
    }

  }

  /**
    * 获取媒介投放包哪些游戏有监控连接
    *
    * @return
    */

  def getMedPkgGameInfo(os: Int): String = {
    var jg = "0"
    val conn = JdbcUtil.getXiaopeng2FXConn();
    var stmt: PreparedStatement = null
    val sql: String = " select GROUP_CONCAT(DISTINCT game_id) gs  from medium_package where feedbackurl!='' and os=? group by os limit 1"
    stmt = conn.prepareStatement(sql)
    stmt.setInt(1, os)
    val rs: ResultSet = stmt.executeQuery()
    while (rs.next) {
      jg = rs.getString("gs");
    }
    stmt.close()
    return jg;
  }


  /**
    * 处理广告监测平台安卓自然量 导充值（投放基础表）
    *
    * @param games
    */
  def doADADataPay(games: String, os: Int, currentday: String) = {
    val sql = "select kpi.publish_date,kpi.game_id,group_id,\nkpi.regi_num-IFNULL(rs.regi_num,0) as regi_num,\nkpi.pay_price-IFNULL(rs.pay_price,0) as pay_price,\nkpi.pay_accounts-IFNULL(rs.pay_accounts,0) as pay_accounts\n from\n(\nselect publish_date,child_game_id game_id,group_id,\nsum(regi_account_num) regi_num,\nsum(pay_money)*100 pay_price,\nsum(pay_account_num) pay_accounts\nfrom bi_gamepublic_base_day_kpi kpi\nwhere kpi.publish_date='" + currentday + "' and child_game_id in(" + games + ") group by publish_date,child_game_id,group_id\n) kpi\nleft join \n(select publish_date,game_id,\nsum(regi_num) regi_num,\nsum(pay_price) pay_price,\nsum(pay_accounts) pay_accounts\n from bi_ad_channel_stats kpi where kpi.pkg_id!='' and kpi.publish_date='" + currentday + "' and game_id in(" + games + ") group by publish_date,game_id) rs on kpi.publish_date=rs.publish_date and kpi.game_id=rs.game_id"
    println(sql)
    val sql2Mysql = "insert into bi_ad_channel_stats" +
      "(publish_date,game_id,pkg_id,medium,regi_num,pay_price,pay_accounts,group_id)" +
      " values(?,?,?,?,?,?,?,?) " +
      " on duplicate key update regi_num=?,pay_price=?,pay_accounts=?,group_id=?"
    val conn = JdbcUtil.getConn()
    val connHip = JdbcUtil.getBiHippoConn()
    val psHip = connHip.prepareStatement(sql)
    val ps = conn.prepareStatement(sql2Mysql)
    val rs = psHip.executeQuery()
    while (rs.next()) {
      ps.setString(1, rs.getString("publish_date"))
      ps.setString(2, rs.getString("game_id"))
      ps.setString(3, "")
      ps.setInt(4, 0)
      ps.setInt(5, rs.getString("regi_num").toInt)
      ps.setInt(6, rs.getString("pay_price").toInt)
      ps.setInt(7, rs.getString("pay_accounts").toInt)
      ps.setInt(8, rs.getString("group_id").toInt)
      //update
      ps.setInt(9, rs.getString("regi_num").toInt)
      ps.setInt(10, rs.getString("pay_price").toInt)
      ps.setInt(11, rs.getString("pay_accounts").toInt)
      ps.setInt(12, rs.getString("group_id").toInt)
      ps.executeUpdate()
    }
    ps.close()
    psHip.close()
    conn.close()
    connHip.close()


  }

  /**
    * 导激活等数据（运营基础表）
    * active_num,regi_dev_num,new_pay_price,new_pay_accounts,new_regi_dev_num
    * @param games
    * @param os
    * @param currentday
    */
  def doADADataActive(games: String, os: Int, currentday: String) = {

    //    val sql = "select kpi.publish_date,kpi.game_id,kpi.group_id,\nkpi.active_num-IFNULL(rs.active_num,0) as active_num,\nkpi.regi_dev_num-IFNULL(rs.regi_dev_num,0) as regi_dev_num,\nkpi.new_pay_price-IFNULL(rs.new_pay_price,0) as new_pay_price,\nkpi.new_pay_accounts-IFNULL(rs.new_pay_accounts,0) as new_pay_accounts\n from\n(\nselect publish_date,child_game_id game_id,\nkpi.group_id,\nsum(active_num) active_num,\nsum(regi_device_num) regi_dev_num,\nsum(new_pay_money)*100 new_pay_price,\nsum(new_pay_account_num) new_pay_accounts\nfrom bi_gamepublic_base_opera_kpi kpi\nwhere kpi.publish_date='" + currentday + "' and child_game_id in(" + games + ")\ngroup by publish_date,child_game_id,group_id\n) kpi\nleft join \n(select publish_date,game_id,sum(active_num) active_num,\nsum(regi_dev_num) regi_dev_num,\nsum(new_pay_price) new_pay_price,\nsum(new_pay_accounts) new_pay_accounts\nfrom bi_ad_channel_stats kpi where pkg_id!='' and kpi.publish_date='" + currentday + "' and game_id in(" + games + ") group by publish_date,game_id) rs on kpi.publish_date=rs.publish_date and kpi.game_id=rs.game_id"
    val sql = "select \nkpi.publish_date,\nkpi.game_id,\nkpi.group_id, \nkpi.active_num-IFNULL(rs.active_num,0) as active_num, \nkpi.regi_dev_num-IFNULL(rs.regi_dev_num,0) as regi_dev_num, \nkpi.new_pay_price-IFNULL(rs.new_pay_price,0) as new_pay_price, \nkpi.new_pay_accounts-IFNULL(rs.new_pay_accounts,0) as new_pay_accounts,\nkpi.new_regi_dev_num-IFNULL(rs.new_regi_dev_num,0) as new_regi_dev_num\nfrom \n( select publish_date,child_game_id game_id, kpi.group_id, sum(active_num) active_num, sum(regi_device_num) regi_dev_num, sum(new_pay_money)*100 new_pay_price, sum(new_pay_account_num) new_pay_accounts,sum(new_regi_device_num) new_regi_dev_num \nfrom bi_gamepublic_base_opera_kpi kpi where kpi.publish_date='" + currentday + "' and child_game_id in(" + games + ") \ngroup by publish_date,child_game_id,group_id ) kpi \nleft join  \n(select publish_date,game_id,sum(active_num) active_num, sum(regi_dev_num) regi_dev_num, sum(new_pay_price) new_pay_price, sum(new_pay_accounts) new_pay_accounts,sum(new_regi_dev_num) new_regi_dev_num\nfrom bi_ad_channel_stats kpi where pkg_id!='' and kpi.publish_date='" + currentday + "' and game_id in(" + games + ") \ngroup by publish_date,game_id) rs on kpi.publish_date=rs.publish_date and kpi.game_id=rs.game_id"
    println(sql)
    val sql2Mysql = "insert into bi_ad_channel_stats" +
      "(publish_date,game_id,pkg_id,medium,active_num,regi_dev_num,new_pay_price,new_pay_accounts,group_id,new_regi_dev_num)" +
      " values(?,?,?,?,?,?,?,?,?,?) " +
      " on duplicate key update active_num=?,regi_dev_num=?,new_pay_price=?,new_pay_accounts=?,group_id=?,new_regi_dev_num=?"
    val conn = JdbcUtil.getConn()
    val connHip = JdbcUtil.getBiHippoConn()
    val psHip = connHip.prepareStatement(sql)
    val ps = conn.prepareStatement(sql2Mysql)
    val rs = psHip.executeQuery()
    while (rs.next()) {
      ps.setString(1, rs.getString("publish_date"))
      ps.setString(2, rs.getString("game_id"))
      ps.setString(3, "")
      ps.setInt(4, 0)
      ps.setInt(5, rs.getString("active_num").toInt)
      ps.setInt(6, rs.getString("regi_dev_num").toInt)
      ps.setInt(7, rs.getString("new_pay_price").toInt)
      ps.setInt(8, rs.getString("new_pay_accounts").toInt)
      ps.setInt(9, rs.getString("group_id").toInt)
      ps.setInt(10, rs.getString("new_regi_dev_num").toInt)
      //update
      ps.setInt(11, rs.getString("active_num").toInt)
      ps.setInt(12, rs.getString("regi_dev_num").toInt)
      ps.setInt(13, rs.getString("new_pay_price").toInt)
      ps.setInt(14, rs.getString("new_pay_accounts").toInt)
      ps.setInt(15, rs.getString("group_id").toInt)
      ps.setInt(16, rs.getString("new_regi_dev_num").toInt)
      ps.executeUpdate()
    }
    ps.close()
    psHip.close()
    conn.close()
    connHip.close()
  }


}
