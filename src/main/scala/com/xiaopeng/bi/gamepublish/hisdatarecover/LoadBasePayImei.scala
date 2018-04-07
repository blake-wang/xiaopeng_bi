package com.xiaopeng.bi.gamepublish.hisdatarecover

import java.io.File
import java.sql.PreparedStatement

import com.xiaopeng.bi.utils.JdbcUtil
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.{SparkConf, SparkContext}

object LoadBasePayImei {
  def main(args: Array[String]): Unit = {
      //日志输出模式
       Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
       Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

      val paySqlByGame="select  \nint(rs.game_id) child_game_id,\npublish_time publish_date,\nint(sdk.game_id) parent_game_id,\nint(sdk.group_id) group_id,\nint(sdk.system_type) os,\nsum(pay_people_num) pay_people_num\nfrom\n(\n\nselect game_id,publish_time,pay_people_num from default.pay_imeis\n\n) rs  join default.game_sdk sdk on sdk.old_game_id=rs.game_id where sdk.game_id !=0\ngroup by \nrs.game_id ,publish_time,\nsdk.game_id ,\nsdk.group_id,\nsdk.system_type "
      val paySqlByPkg="select  \nint(rs.game_id) child_game_id,\nparent_channel medium_channel,\nchild_channel ad_site_channel,\nad_label pkg_code,\npublish_time publish_date,\nint(sdk.game_id) parent_game_id,\nint(sdk.group_id) group_id,\nint(sdk.system_type) os,\nsum(pay_people_num) pay_people_num,\nsum(pay_account_num) pay_account_num,\nsum(pay_money) pay_money\nfrom\n(\n\nselect game_id,parent_channel,child_channel,ad_label,publish_time,pay_people_num,pay_account_num,pay_money from default.pay_imeis_pkg  \n) rs  join default.game_sdk sdk on sdk.old_game_id=rs.game_id where sdk.game_id !=0\ngroup by \nrs.game_id ,parent_channel,child_channel,ad_label,publish_time,\nsdk.game_id ,\nsdk.group_id,\nsdk.system_type "
      val paySqlByGameInsert = "insert into bi_gamepublic_base_opera_kpi(child_game_id,publish_date,parent_game_id,group_id,os,pay_people_num)" +
        " values(?,?,?,?,?,?)" +
        " on duplicate key update pay_people_num=?,parent_game_id=?"
      val  paySqlByPkgInsert= "insert into bi_gamepublic_base_day_kpi(child_game_id,medium_channel,ad_site_channel,pkg_code,publish_date,parent_game_id,group_id,os,pay_people_num,pay_account_num,pay_money)" +
      " values(?,?,?,?,?,?,?,?,?,?,?)" +
      " on duplicate key update pay_people_num=?,parent_game_id=?,pay_account_num=?,pay_money=?,os=?,group_id=?,parent_game_id=?"

    //Hadoop libariy
      val path: String = new File(".").getCanonicalPath
      System.getProperties().put("hadoop.home.dir", path)
      new File("./bin").mkdirs()
      new File("./bin/winutils.exe").createNewFile()

      val sparkConf = new SparkConf().setAppName(this.getClass.getName.replace("$", ""))
      val sc = new SparkContext(sparkConf)
      val sqlContext = new HiveContext(sc)
      sparkConf.set("spark.storage.memoryFraction","0.7").set("spark.sql.shuffle.partitions","60")
      val paySqlByGameDf= sqlContext.sql(paySqlByGame)
       processDbFct(paySqlByGameDf,paySqlByGameInsert)
      val paySqlByPkgDf= sqlContext.sql(paySqlByPkg)
      processDbFct(paySqlByPkgDf,paySqlByPkgInsert)

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
  def processDbFct(dataf: DataFrame,insertSql:String) = {
    //全部转为小写，后面好判断
    val sql2Mysql = insertSql.replace("|", " ").toLowerCase
    //获取values（）里面有多少个?参数，有利于后面的循环
    val startValuesIndex = sql2Mysql.indexOf("(?") + 1
    val endValuesIndex = sql2Mysql.indexOf("?)") + 1
    //values中的个数
    val valueArray: Array[String] = sql2Mysql.substring(startValuesIndex, endValuesIndex).split(",") //两个（？？）中间的值
    //条件中的参数个数
    val wh: Array[String] = sql2Mysql.substring(sql2Mysql.indexOf("update") + 6).split(",") //找update后面的字符串再判断
    //查找需要insert的字段
    val cols_ref = sql2Mysql.substring(0, sql2Mysql.lastIndexOf("(?")) //获取（?特殊字符前的字符串，然后再找字段
    val cols: Array[String] = cols_ref.substring(cols_ref.lastIndexOf("(") + 1, cols_ref.lastIndexOf(")")).split(",")

    /********************数据库操作 *******************/
    dataf.foreachPartition((rows: Iterator[Row]) => {
      val conn = JdbcUtil.getConn()
      val ps: PreparedStatement = conn.prepareStatement(sql2Mysql)
      for (x <- rows) {
        //补充value值
        for (rs <- 0 to valueArray.length - 1) {
          ps.setString(rs.toInt + 1, x.get(rs).toString)
        }
        //补充条件
        for (i <- 0 to wh.length - 1) {
          val rs = wh(i).trim.substring(0, wh(i).trim.lastIndexOf("="))
          for (ii <- 0 to cols.length - 1) {
            if (cols(ii).trim.equals(rs)) {
              ps.setString(i.toInt + valueArray.length.toInt + 1, x.get(ii).toString)
            }
          }
        }
        ps.executeUpdate()
      }
      conn.close()
    }
    )
  }


}

