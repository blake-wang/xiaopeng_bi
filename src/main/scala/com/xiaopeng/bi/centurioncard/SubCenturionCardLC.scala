package com.xiaopeng.bi.centurioncard

import java.sql.PreparedStatement
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import com.xiaopeng.bi.utils.{Hadoop, JdbcUtil}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

object SubCenturionCardLC {
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
    cal.add(Calendar.DATE, -30)
    val day_30 = dateFormat.format(cal.getTime())

    //current
    val hivesql="select game_id,expand_code promo_code,to_date(reg_time) statistics_date,\ncount(distinct case when datediff(login_time,reg_time)=1 then lj.game_account else null end) as lc2,\ncount(distinct case when datediff(login_time,reg_time)=2 then lj.game_account else null end) as lc3,\ncount(distinct case when datediff(login_time,reg_time)=3 then lj.game_account else null end) as lc4,\ncount(distinct case when datediff(login_time,reg_time)=4 then lj.game_account else null end) as lc5,\ncount(distinct case when datediff(login_time,reg_time)=5 then lj.game_account else null end) as lc6,\ncount(distinct case when datediff(login_time,reg_time)=6 then lj.game_account else null end) as lc7,\ncount(distinct case when datediff(login_time,reg_time)=14 then lj.game_account else null end) as lc15,\ncount(distinct case when datediff(login_time,reg_time)=29 then lj.game_account else null end) as lc30\nfrom \ndefault.subcenturion lj \ngroup by game_id,to_date(reg_time),expand_code"


    //Hadoop libariy
    Hadoop.hd

    val sparkConf = new SparkConf().setAppName(this.getClass.getName.replace("$",""))
    val sc = new SparkContext(sparkConf)
    val sqlContext=new HiveContext(sc)
    sqlContext.sql("use yyft")
    sqlContext.sql("drop table default.subcenturion")
    sqlContext.sql("\n\ncreate table default.subcenturion as\nselect distinct regi.game_account,regi.game_id,to_date(reg_time) reg_time,to_date(lg.login_time) login_time,expand_code \nfrom yyft.ods_regi_rz regi join \n(select game_account,login_time from yyft.ods_login)\n lg on lower(trim(regi.game_account))=lower(trim(lg.game_account)) \nwhere expand_code like '%\\~%'  and to_date(login_time)>='day_30' and to_date(login_time)<='currentday' and to_date(login_time)>='2017-06-01'".replace("currentday",currentday).replace("day_30",day_30))
    val dataf=sqlContext.sql(hivesql)
    /********************数据库操作***************   ****/
    dataf.foreachPartition(rows=> {
      val conn = JdbcUtil.getConn()

      val mysqlsql="\nupdate bi_sub_centurition_pkgstats set login_accounts_2day=?,login_accounts_3day=?,login_accounts_4day=?,login_accounts_5day=?,login_accounts_6day=?,login_accounts_7day=?,login_accounts_15day=?,login_accounts_30day=?\nwhere game_id=? and pkg_code=? and statistics_date=?"

      val ps: PreparedStatement = conn.prepareStatement(mysqlsql)
      for (x <- rows) {
        ps.setString(1,x.get(3).toString)
        ps.setString(2,x.get(4).toString)
        ps.setString(3,x.get(5).toString)
        ps.setString(4,x.get(6).toString)
        ps.setString(5,x.get(7).toString)
        ps.setString(6,x.get(8).toString)
        ps.setString(7,x.get(9).toString)
        ps.setString(8,x.get(10).toString)
        //where
        ps.setString(9,x.get(0).toString)
        ps.setString(10,x.get(1).toString)
        ps.setString(11,x.get(2).toString)

        ps.executeUpdate()
      }
      conn.close()
    }
    )
    System.clearProperty("spark.driver.port")
    sc.stop()
  }
}
