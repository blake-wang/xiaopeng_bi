package com.xiaopeng.bi.gamepublish.hisdatarecover

import java.io.File
import java.sql.PreparedStatement
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import com.xiaopeng.bi.utils.JdbcUtil
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.Row
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

object LoadBaseKpiLoginAccountByDay {
  def main(args: Array[String]): Unit = {
      //日志输出模式
       Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
       Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
      val currentday = args(0)
      val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
      val date: Date = dateFormat.parse(currentday)
      val cal: Calendar = Calendar.getInstance()
      cal.setTime(date)
      cal.add(Calendar.DATE, -15)

      val hivesql="select  \n if(a.game_id is null,0,a.game_id) game_id\n,if((split(channel_expand,'_')[0] is null or split(channel_expand,'_')[0]=''),'21',split(channel_expand,'_')[0]) as parent_channel\n,if(split(channel_expand,'_')[1] is null,'',split(channel_expand,'_')[1]) as child_channel\n,if(split(channel_expand,'_')[2] is null,'',split(channel_expand,'_')[2]) as ad_label\n,'currentday' publish_time\n,count(distinct game_account) as login_accounts\nfrom yyft.ods_login a\njoin (select distinct game_id from yyft.ods_publish_game) gm on gm.game_id=a.game_id\nwhere to_date(login_time)='currentday' \ngroup by a.game_id,if((split(channel_expand,'_')[0] is null or split(channel_expand,'_')[0]=''),'21',split(channel_expand,'_')[0]),if(split(channel_expand,'_')[1] is null,'',split(channel_expand,'_')[1]),\n\nif(split(channel_expand,'_')[2] is null,'',split(channel_expand,'_')[2])"
      val mysqlsql = "insert into bi_gamepublic_basekpi(game_id,parent_channel,child_channel,ad_label,publish_time,login_accounts) values(?,?,?,?,?,?)  on duplicate key update login_accounts=?"
      //全部转为小写，后面好判断
      val execSql = hivesql.replace("currentday", currentday) //hive sql
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
      dataf.show()

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

