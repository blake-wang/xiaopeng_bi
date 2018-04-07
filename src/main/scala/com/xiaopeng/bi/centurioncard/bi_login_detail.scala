package com.xiaopeng.bi.centurioncard

import java.io.File
import java.sql.PreparedStatement
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import com.xiaopeng.bi.utils.JdbcUtil
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

object bi_login_detail {

  def main(args: Array[String]): Unit = {

    if (args.length < 1) {
      System.err.println("Usage: <currentday> ")
      System.exit(1)
    }
    if (args.length > 1) {
      System.err.println("参数个数传入太多，固定为1个： <currentday>  ")
      System.exit(1)
    }
    //日志输出警告
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
    //跑数日期
    val currentday=args(0)
    //昨天
    val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val date:Date  = dateFormat.parse(args(0))
    val cal:Calendar=Calendar.getInstance()
    cal.setTime(date)
    cal.add(Calendar.DATE, -1)
    val yesterday=dateFormat.format(cal.getTime())
    //月第一天
    val monthfirstday=args(0).substring(0,7)+"-01"

    val hivesql="select \ndistinct\nif(pu.member_id is null,0,pu.member_id) member_id,\nif(pu.username is null,'',pu.username) user_account,\nif(regi.game_account is null,'',regi.game_account) game_account,\nif(regi.game_id is null,0,regi.game_id) game_id,\nif(bg.name is null,'',bg.name) game_name,\nif(regi.channel_id is null,0,regi.channel_id) channel_id,\nif(bc.name is null,'',bc.name) channel_name,\nif(to_date(lg.login_time) is null,'0000-00-00 00:00:00',to_date(lg.login_time)) login_date,\nif(regi.reg_os_type is null,'UNKOWN',regi.reg_os_type) reg_os_type,\nif(mainid is null,0,mainid) mainid,\nif(maingname is null,'',maingname) maingname\n\nfrom ods_login lg\n join ods_regi regi on regi.game_account=lg.game_account\n join  gameinfo bg on bg.id=regi.game_id \n join promo_user pu on pu.member_id=regi.owner_id and pu.status=0 and pu.member_grade=1\n join bchannel bc on bc.id=regi.channel_id and bc.status=0\nwhere to_date(lg.login_time)='currentday' "
    val mysqlsql="insert into bi_login_detail(member_id,user_account,game_account,game_id,game_name,channel_id,channel_name,login_date,os,main_game_id,main_game_name)" +
                 " values(?,?,?,?,?,?,?,?,?,?,?)" +
                 " on duplicate key update user_account=?,game_name=?,channel_name=?,login_date=?,os=?,main_game_id=?,main_game_name=?"
    //全部转为小写，后面好判断
    val execSql=hivesql.replace("currentday",currentday).replace("yesterday",yesterday).replace("monthfirstday",monthfirstday)  //hive sql
    val sql2Mysql=mysqlsql.replace("|"," ") .toLowerCase

    //Hadoop libariy
    val path: String = new File(".").getCanonicalPath
    System.getProperties().put("hadoop.home.dir", path)
    new File("./bin").mkdirs()
    new File("./bin/winutils.exe").createNewFile()

    //获取values（）里面有多少个?参数，有利于后面的循环
    val startValuesIndex=sql2Mysql.indexOf("(?")+1
    val endValuesIndex=sql2Mysql.indexOf("?)")+1
    //values中的个数
    val valueArray:Array[String]=sql2Mysql.substring(startValuesIndex,endValuesIndex).split(",")  //两个（？？）中间的值
    //条件中的参数个数
    val wh:Array[String]=sql2Mysql.substring(sql2Mysql.indexOf("update")+6).split(",")  //找update后面的字符串再判断
    //查找需要insert的字段
    val cols_ref=sql2Mysql.substring(0,sql2Mysql.lastIndexOf("(?"))  //获取（?特殊字符前的字符串，然后再找字段
    val cols:Array[String]=cols_ref.substring(cols_ref.lastIndexOf("(")+1,cols_ref.lastIndexOf(")")).split(",")

    /********************hive库操作*******************/
    val sparkConf = new SparkConf().setAppName(this.getClass.getName.replace("$",""))
    val sc = new SparkContext(sparkConf)
    val sqlContext=new HiveContext(sc)
    sqlContext.sql("use yyft")
    val dataf = sqlContext.sql(execSql)//执行hive sql
    dataf.show()

    /********************数据库操作***************   ****/

    dataf.foreachPartition(rows=> {
      val conn = JdbcUtil.getConn()
      conn.setAutoCommit(false)
      val ps: PreparedStatement = conn.prepareStatement(sql2Mysql)
      ps.clearBatch()
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
        ps.addBatch()
      }
      ps.executeBatch()
      conn.commit()
      conn.close()
    }
    )

    System.clearProperty("spark.driver.port")
    sc.stop()

  }

}