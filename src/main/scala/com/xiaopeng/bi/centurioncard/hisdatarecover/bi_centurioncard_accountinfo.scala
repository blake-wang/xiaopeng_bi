package com.xiaopeng.bi.centurioncard.hisdatarecover

import java.io.File
import java.sql.PreparedStatement
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import com.xiaopeng.bi.utils.JdbcUtil
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

object bi_centurioncard_accountinfo {

  def main(args: Array[String]): Unit = {

    if (args.length < 2) {
      System.err.println("Usage: <currentday> startday ")
      System.exit(1)
    }
    if (args.length > 2) {
      System.err.println("参数个数传入太多，固定为2个： <currentday> startday ")
      System.exit(1)
    }

    //跑数日期
    val currentday=args(0)
    val startday=args(1)
    //昨天
    val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val date:Date  = dateFormat.parse(args(0))
    val cal:Calendar=Calendar.getInstance()
    cal.setTime(date)
    cal.add(Calendar.DATE, -1)
    val yesterday=dateFormat.format(cal.getTime())
    //月第一天
    val monthfirstday=args(0).substring(0,7)+"-01"


    //账号明细
    //val hivesql="select  \nregi.game_account,\nregi.owner_id as uid,\nif(mainid is null,0,mainid) as game_id,\nif(maingname is null,'',maingname) as game_name,\ncase when reg_resource=1 then 1 else 2 end as user_type,\nif(relate_time is null,'0000-00-00',relate_time) regi_time,\ncase when regi.bind_uid_time is null then 0 else 1 end as is_recharge,\nif(pu.username is null,'',pu.username) user_account,\nif(reg_os_type is null,'UNKNOW',reg_os_type) platform,\nif(lg.login_time is null,'0000-00-00',lg.login_time) last_login_time\nfrom ods_regi regi join promo_user pu on pu.member_id=regi.owner_id \njoin gameinfo gf on gf.id=regi.game_id\nleft join (select game_account,max(login_time) login_time from ods_login group by game_account) lg\n where to_date(regi.relate_time)>='startday' and to_date(regi.relate_time)<='currentday' and regi.owner_id!=0 and regi.owner_id is not null and regi.game_account is not null "
    val hivesql="select  \nregi.game_account,\nregi.owner_id as uid,\nif(mainid is null,0,mainid) as game_id,\nif(maingname is null,'',maingname) as game_name,\ncase when reg_resource=1 then 1 else 2 end as user_type,\nif(relate_time is null,'0000-00-00',relate_time) regi_time,\ncase when regi.bind_uid_time is null then 0 else 1 end as is_recharge,\nif(pu.username is null,'',pu.username) user_account,\nif(reg_os_type is null,'UNKNOW',reg_os_type) platform,\nif(lg.login_time is null,'0000-00-00',lg.login_time) last_login_time\nfrom ods_regi regi join promo_user pu on pu.member_id=regi.owner_id \njoin gameinfo gf on gf.id=regi.game_id join (select game_account,max(login_time) login_time from ods_login group by game_account) lg on lg.game_account=regi.game_account\n where to_date(lg.login_time)>='startday' and to_date(lg.login_time)<='currentday' and regi.owner_id!=0 and regi.owner_id is not null and regi.game_account is not null"
    val mysqlsql=" insert into bi_centurioncard_accountinfo(game_account,uid,game_id,game_name,user_type,regi_time,is_recharge,user_account,platform,last_login_time)" +
      " values(?,?,?,?,?,?,?,?,?,?)" +
      " on duplicate key update game_name=?,user_type=?,regi_time=?,is_recharge=?,user_account=?,platform=?,last_login_time=?"
    //全部转为小写，后面好判断
    val execSql=hivesql.replace("currentday",currentday).replace("startday",startday)  //hive sql
    val sql2Mysql=mysqlsql.replace("|"," ").toLowerCase

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
     // conn.setAutoCommit(false)
      val ps: PreparedStatement = conn.prepareStatement(sql2Mysql)
      //ps.clearBatch()
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
       // ps.addBatch()
        ps.executeUpdate()
      }
     // ps.executeBatch()
     // conn.commit()
      conn.close()
    }
    )
    System.clearProperty("spark.driver.port")
    sc.stop()
  }
}
