package com.xiaopeng.bi.gamepublish.hisdatarecover

import java.io.File
import java.sql.PreparedStatement

import com.xiaopeng.bi.utils.JdbcUtil
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.Row
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

object LoadBaseDayKpi2Q {
  def main(args: Array[String]): Unit = {
      //日志输出模式
       Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
       Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

      val hivesql="select child_game_id,medium_channel,ad_site_channel,pkg_code,publish_date,parent_game_id,group_id,os,dau_account_num,dau_device_num,new_login_account_num,new_login_device_num,new_active_device_num,new_pay_people_num,new_pay_account_num,new_pay_money from default.day_kpi"
      val mysqlsql = "insert into bi_gamepublic_base_day_kpi(child_game_id,medium_channel,ad_site_channel,pkg_code,publish_date,parent_game_id,group_id,os,dau_account_num,dau_device_num,new_login_account_num,new_login_device_num,new_active_device_num,new_pay_people_num,new_pay_account_num,new_pay_money)" +
        " values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)" +
        " on duplicate key update dau_account_num=?,dau_device_num=?,new_login_account_num=?,new_login_device_num=?,new_active_device_num=?,new_pay_people_num=?,new_pay_account_num=?,new_pay_money=?,os=?,group_id=?,parent_game_id=?"
      //全部转为小写，后面好判断
      val execSql = hivesql //hive sql
      val sql2Mysql = mysqlsql.replace("|", " ").toLowerCase

      //Hadoop libariy
      val path: String = new File(".").getCanonicalPath
      System.getProperties().put("hadoop.home.dir", path)
      new File("./bin").mkdirs()
      new File("./bin/winutils.exe").createNewFile()

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

      /** ******************hive库操作 *******************/
      val sparkConf = new SparkConf().setAppName(this.getClass.getName.replace("$", ""))
      val sc = new SparkContext(sparkConf)
      val sqlContext = new HiveContext(sc)
      sqlContext.sql("use yyft")
      val dataf = sqlContext.sql(execSql).cache()//执行hive sql
      //dataf.show()

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
      System.clearProperty("spark.driver.port")
      sc.stop()
      sc.clearCallSite()

    }

  }

