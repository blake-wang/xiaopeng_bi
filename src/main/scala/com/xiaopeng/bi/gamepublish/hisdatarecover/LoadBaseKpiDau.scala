package com.xiaopeng.bi.gamepublish.hisdatarecover

import java.sql.PreparedStatement

import com.xiaopeng.bi.utils.{JdbcUtil, SparkUtils}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.hive.HiveContext

/**
  * dau 离线原本存在，这里进行汇总
  * 分包维度
  * 小时表
  * LoadBaseKpiDauByHour 整理到这个类
  * 天表
  * LoadBaseDayKpi2Q
  *
  * 游戏维度
  * 小时表 (只有这个不存在 需要加)
  * LoadBaseKpiDau
  * 天表
  * LoadBaseKpiDau
  *
  * bi_gamepublic_basekpi 的 login_accounts 字段 --整理到这个类
  */
object LoadBaseKpiDau {
  def main(args: Array[String]): Unit = {
    //日志输出模式
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    val sparkConf = new SparkConf().setAppName(this.getClass.getName.replace("$", ""))
    SparkUtils.setMaster(sparkConf);
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)
    // 分包维度小时表 -放开,屏蔽 LoadBaseKpiDauByHour
//    val hiveSql_pkg_hour = "select child_game_id game_id,medium_channel parent_channel,ad_site_channel child_channel,pkg_code ad_label,publish_date publish_time,parent_game_id,group_id,os,dau_account_num_hour dau_account_num from default.basekpi_hour"
//    val dataf_pkg_hour = sqlContext.sql(hiveSql_pkg_hour)
//    val sql_pkg_hour = "insert into bi_gamepublic_basekpi(game_id,parent_channel,child_channel,ad_label,publish_time,parent_game_id,group_id,os,dau_account_num) values(?,?,?,?,?,?,?,?,?) on duplicate key update dau_account_num=?,os=?,group_id=?,parent_game_id=?"
//    processDbFct(dataf_pkg_hour, sql_pkg_hour);
//
//    // 分包维度天表 -LoadBaseDayKpi2Q
//    val hiveSql_pkg_day = "select \nlz.game_id child_game_id,\nlz.parent_channel medium_channel,\nlz.child_channel ad_site_channel,\nlz.ad_label pkg_code,\nlz.publish_time publish_date,\ngs.game_id parent_game_id,\ngs.group_id group_id,\ngs.system_type os,\nlz.dau_account_num dau_account_num\nfrom \n(select game_id,parent_channel,child_channel,ad_label,publish_time,dau_account_num from default.daykpi_rhy)lz\njoin (select distinct game_id,old_game_id,group_id,system_type from default.game_sdk) gs on gs.old_game_id=lz.game_id"
//    val dataf_pkg_day = sqlContext.sql(hiveSql_pkg_day)
//    val sql_pkg_day = "insert into bi_gamepublic_base_day_kpi(child_game_id,medium_channel,ad_site_channel,pkg_code,publish_date,parent_game_id,group_id,os,dau_account_num)\nvalues(?,?,?,?,?,?,?,?,?)  on duplicate key update dau_account_num=?,os=?,group_id=?,parent_game_id=?"
//    processDbFct(dataf_pkg_day, sql_pkg_day);

//    // bi_gamepublic_basekpi 的 login_accounts 字段 -放开  屏蔽 LoadBaseKpiLoginAccountByDay
//    val hiveSql_pkg_day_base = "select \nlz.game_id game_id,\nlz.parent_channel parent_channel,\nlz.child_channel child_channel,\nlz.ad_label ad_label,\nlz.publish_time publish_time,\ngs.game_id parent_game_id,\ngs.group_id group_id,\ngs.system_type os,\nlz.dau_account_num login_accounts\nfrom \n(select game_id,parent_channel,child_channel,ad_label,publish_time,dau_account_num from default.daykpi_rhy)lz\njoin (select distinct game_id,old_game_id,group_id,system_type from default.game_sdk) gs on gs.old_game_id=lz.game_id"
//    val dataf_pkg_day_base = sqlContext.sql(hiveSql_pkg_day_base)
//    val mysql_game_day_base = "insert into bi_gamepublic_basekpi(game_id,parent_channel,child_channel,ad_label,publish_time,parent_game_id,group_id,os,login_accounts) values(?,?,?,?,?,?,?,?,?)  on duplicate key update login_accounts=?,os=?,group_id=?,parent_game_id=?"
//    processDbFct(dataf_pkg_day_base, mysql_game_day_base);

    // 游戏维度小时表 -放开
    val hiveSql_game_hour = "select child_game_id,parent_game_id,group_id,os,publish_time,dau_account_num_hour dau_account_num from default.basekpi_rhy3"
    val dataf_game_hour = sqlContext.sql(hiveSql_game_hour)
    val mysql_game_hour = "insert into bi_gamepublic_base_opera_hour_kpi(child_game_id,parent_game_id,group_id,os,publish_time,dau_account_num) values (?,?,?,?,?,?) on duplicate key update dau_account_num=?,os=?,group_id=?,parent_game_id=?"
    processDbFct(dataf_game_hour, mysql_game_hour);

    // 游戏维度天表 -放开
    val hiveSql_game_day = "select \nlz.game_id child_game_id,\ngs.game_id parent_game_id,\ngs.group_id group_id,\ngs.system_type os,\nlz.publish_date publish_date,\nlz.dau_account_num dau_account_num\nfrom \n(select game_id, publish_time publish_date, dau_account_num from default.daykpi_rhy3)lz\njoin (select distinct game_id,old_game_id,group_id,system_type from default.game_sdk) gs on gs.old_game_id=lz.game_id"
    val dataf_game_day = sqlContext.sql(hiveSql_game_day)
    val mysql_game_day = "insert into bi_gamepublic_base_opera_kpi(child_game_id,parent_game_id,group_id,os,publish_date,dau_account_num) values(?,?,?,?,?,?) on duplicate key update dau_account_num=?,os=?,group_id=?,parent_game_id=?"
    processDbFct(dataf_game_day, mysql_game_day);



    System.clearProperty("spark.driver.port")
    sc.stop()
    sc.clearCallSite()

  }

  /**
    * 推送data数据到数据库
    *
    * @param dataf
    * @param insertSql
    */
  def processDbFct(dataf: DataFrame, insertSql: String) = {
    //全部转为小写，后面好判断
    val sql2Mysql = insertSql.replace("|", " ").toLowerCase
    //获取values（）里面有多少个?参数，有利于后面的循环
    val startValuesIndex = sql2Mysql.indexOf("(?") + 1
    val endValuesIndex = sql2Mysql.indexOf("?)") + 1
    //values中的个数
    val valueArray: Array[String] = sql2Mysql.substring(startValuesIndex, endValuesIndex).split(",")
    //两个（？？）中间的值
    //条件中的参数个数
    val wh: Array[String] = sql2Mysql.substring(sql2Mysql.indexOf("update") + 6).split(",")

    //找update后面的字符串再判断
    //查找需要insert的字段
    val cols_ref = sql2Mysql.substring(0, sql2Mysql.lastIndexOf("(?"))
    //获取（?特殊字符前的字符串，然后再找字段
    val cols: Array[String] = cols_ref.substring(cols_ref.lastIndexOf("(") + 1, cols_ref.lastIndexOf(")")).split(",")

    /** ******************数据库操作 *******************/
    dataf.foreachPartition((rows: Iterator[Row]) => {
      val conn = JdbcUtil.getConn()
      val ps: PreparedStatement = conn.prepareStatement(sql2Mysql)
      for (x <- rows) {

        if (x.get(1).toString.length <= 10 && x.get(2).toString.length <= 10 && x.get(3).toString.length <= 15) {
          // 过滤掉渠道长度不符合规则的
          //补充value值
          for (rs <- 0 to valueArray.length - 1) {
            ps.setString(rs.toInt + 1, x.get(rs).toString)
          }
          //补充条件
          for (i <- 0 to wh.length - 1) {
            val rs = wh(i).trim.substring(0, wh(i).trim.lastIndexOf("="))
            for (ii <- 0 to cols.length - 1) {
              if (cols(ii).trim.equals(rs)) {
                ps.setString(i.toInt + valueArray.length + 1, x.get(ii).toString)
              }
            }

          }
          ps.executeUpdate()
        }
      }
      conn.close()
    }
    )
  }

}
