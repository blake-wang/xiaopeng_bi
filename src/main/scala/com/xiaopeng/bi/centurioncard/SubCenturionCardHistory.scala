package com.xiaopeng.bi.centurioncard

import java.sql.PreparedStatement
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import com.xiaopeng.bi.utils.{Hadoop, JdbcUtil}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

object SubCenturionCardHistory {
  def main(args: Array[String]): Unit = {
        Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
        Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
    if (args.length < 1) {
      System.err.println("Usage: <currentday> ")
      System.exit(1)
    }
    if (args.length > 1) {
      System.err.println("参数个数传入太多，固定为1个： <currentday>  ")
      System.exit(1)
    }
    val currentday = args(0)
    val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val date: Date = dateFormat.parse(currentday)
    val cal: Calendar = Calendar.getInstance()
    cal.setTime(date)
    cal.add(Calendar.DATE, -60)
    val day_30 = dateFormat.format(cal.getTime())

    //current
   // val hiveregiinsert="select imei,expand_code,reg_time,'0000-00-00' as wd from default.fximei1"

    val hiveloginupdate="select regexp_replace(imei,'&','|') imei,max(to_date(login_time)) last_login_date from yyft.ods_login rz\njoin (select game_id from yyft.ods_publish_game) gm on gm.game_id=rz.game_id\n where to_date(login_time)>='2017-05-01' \nand imei is not null\ngroup by regexp_replace(imei,'&','|')  "


//    val hiveorderupdate="select regexp_replace(imei,'&','|') imei,max(to_date(order_time)) last_recharge_date,sum(ori_price) as recharge_amount from \n(select distinct game_account,order_no,order_time,imei,ori_price,total_amt,game_id from yyft.ods_order where order_status=4 and prod_type=6 and imei is not null) rs\njoin (select game_id from yyft.ods_publish_game) gm on gm.game_id=rs.game_id\ngroup by  regexp_replace(imei,'&','|') "

    //Hadoop libariy
    Hadoop.hd

    val sparkConf = new SparkConf().setAppName(this.getClass.getName.replace("$",""))
    val sc = new SparkContext(sparkConf)
    val sqlContext=new HiveContext(sc)
    sqlContext.sql("use yyft")
    //val dataf=sqlContext.sql(hiveregiinsert)
//    /********************数据库操作***************   ****/
//    dataf.foreachPartition(rows=> {
//      val conn = JdbcUtil.getConn()
//      val mysqlsql="insert into bi_sub_centurion_devstatus(imei,pkg_code,last_regi_date,worthiness_date) values(?,?,?,?) on duplicate key update last_regi_date=?"
//      val ps: PreparedStatement = conn.prepareStatement(mysqlsql)
//      for (x <- rows) {
//        ps.setString(1,x.get(0).toString)
//        ps.setString(2,x.get(1).toString)
//        ps.setString(3,x.get(2).toString)
//        ps.setString(4,x.get(3).toString)
//        ps.setString(5,x.get(2).toString)
//        ps.executeUpdate()
//      }
//      conn.close()
//    }
//    )


    val datalogin=sqlContext.sql(hiveloginupdate)
    /********************数据库操作***************   ****/
    datalogin.foreachPartition(rows=> {
      val conn = JdbcUtil.getConn()
      val mysqlsql="update bi_sub_centurion_devstatus set last_login_date=? where imei=?"
      val ps: PreparedStatement = conn.prepareStatement(mysqlsql)
      for (x <- rows) {
        ps.setString(1,x.get(1).toString)
        ps.setString(2,x.get(0).toString)
        ps.executeUpdate()
      }
      conn.close()
    }
    )


//    val dataorder=sqlContext.sql(hiveorderupdate)
//    /********************数据库操作***************   ****/
//    dataorder.foreachPartition(rows=> {
//      val conn = JdbcUtil.getConn()
//      val mysqlsql="update bi_sub_centurion_devstatus set last_recharge_date=?,recharge_amount=? where imei=?"
//      val ps: PreparedStatement = conn.prepareStatement(mysqlsql)
//      for (x <- rows) {
//        ps.setString(1,x.get(1).toString)
//        ps.setString(2,x.get(2).toString)
//        ps.setString(3,x.get(0).toString)
//        ps.executeUpdate()
//      }
//      conn.close()
//    }
//    )

    System.clearProperty("spark.driver.port")
    sc.stop()
  }
}
