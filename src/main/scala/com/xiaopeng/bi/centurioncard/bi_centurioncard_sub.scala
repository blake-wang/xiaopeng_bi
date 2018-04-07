package com.xiaopeng.bi.centurioncard

import java.io.File
import java.sql.PreparedStatement
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import com.xiaopeng.bi.utils.JdbcUtil
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

object bi_centurioncard_sub {

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
    //2017-10-30 not need modify
    val hivesql="select \n'currentday' statistics_date ,\ncase when promo.user_account is null then '' else user_account end user_account ,\npromo.sed,\ncase when acc_create_time is null then '0000-00-00 00:00:00' else acc_create_time end acc_create_time,\ncase when promo.sub_user_account is null then '' else sub_user_account end sub_user_account,\ncase when sum(subpkgs) is null then 0 else sum(subpkgs) end subpackages,\ncase when sum(total_gameacc) is null then 0 else sum(total_gameacc) end game_accounts,\ncase when sum(add_users) is null then 0 else sum(add_users) end add_users,\ncase when sum(total_income) is null then 0 else  sum(total_income) end income,\nchannel_id\nfrom\n(\n select a.member_id,a.username sub_user_account,\n case when a.owner_id=0 then a.username else b.username end as user_account ,\n case when a.owner_id=0 then 0 else row_number() over(partition by b.member_id order by b.member_id) end as sed ,\n a.create_time acc_create_time\nfrom promo_user a left join promo_user b on a.owner_id=b.member_id where a.status in(0,1) and a.member_grade=1\n ) promo join\n(\n--分包数\n\n  select to_date(subpkg_time) as  dt,sub.member_id,channel_id,count(promo_file_name) subpkgs,0 total_gameacc,0 add_users,0 total_income from  ods_subpkg sub\n  where  to_date(subpkg_time)='currentday'\n  group by sub.member_id,to_date(subpkg_time),channel_id\n\n--新增数\nunion all\nselect to_date(bind_uid_time) as dt,regi.owner_id member_id,channel_id,0 as subpkgs,0 as total_gameacc,count(regi.game_account) add_users,0 as total_income from \n    ods_regi regi \n    where  to_date(bind_uid_time)='currentday'\n    group by regi.owner_id,to_date(bind_uid_time),channel_id  \n--总流水\nunion all \nselect to_date(order_time)  as dt,reap_uid member_id,channel_id,0 subpkgs,0 total_gameacc,0 add_users,sum(ori_price) total_income from v_ods_order \nwhere order_status in(4,8) and to_date(order_time)='currentday' group by reap_uid,to_date(order_time),channel_id\n  ) rs\n  on rs.member_id=promo.member_id\n  where promo.sed!=0 \n  group by\n  dt,user_account,sub_user_account,sed,acc_create_time,channel_id"
    val mysqlsql="insert into bi_centurioncard_sub(statistics_date,user_account,seq,acc_create_time,sub_user_account,subpackages,game_accounts,add_users,income,channel_id)" +
                  " values(?,?,?,?,?,?,?,?,?,?)" +
                  " on duplicate key update seq=?,acc_create_time=?,add_users=?,subpackages=?,game_accounts=?,add_users=?,income=?"

    //全部转为小写，后面好判断
    val execSql=hivesql.replace("currentday",currentday).replace("yesterday",yesterday).replace("monthfirstday",monthfirstday)  //hive sql

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
    sparkConf.set("spark.memory.useLegacyMode","true").set("spark.shuffle.memoryFraction","0.6").set("spark.storage.memoryFraction","0.2")
    val sc = new SparkContext(sparkConf)
    val sqlContext=new HiveContext(sc)

    sqlContext.sql("use yyft")
    val dataf = sqlContext.sql(execSql) //执行hive sql

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
