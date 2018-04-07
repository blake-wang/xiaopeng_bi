package com.xiaopeng.bi.centurioncard

import java.sql.PreparedStatement
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import com.xiaopeng.bi.utils.{Hadoop, JdbcUtil}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

object bi_centurioncard_operacurrent {
  def main(args: Array[String]): Unit = {

    if (args.length < 1) {
      System.err.println("Usage: <currentday> ")
      System.exit(1)
    }
    if (args.length > 1) {
      System.err.println("参数个数传入太多，固定为1个： <currentday>  ")
      System.exit(1)
    }
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


    //current
    val hivesql="select \n'currentday' statistics_date,\ncase when pu.username is null then '' else pu.username end user_account,\ncase when ra is null then 0 else ra end day_profit,\ncase when pa is null then 0 else pa end day_income,\ncase when add_user is null then 0 else add_user end day_add_users\nfrom promo_user pu \n--当日流水和返利\nleft join\n  (select reap_uid member_id,sum(ori_price) pa,sum(rebate_amount) ra from v_ods_order where to_date(order_time)='currentday'  and order_status in(4,8) group by reap_uid\n  )  curday  \n  on pu.member_id=curday.member_id\n\n--当日新增用户数\n\nleft join\n (\n  select reap_uid owner_id,count(distinct game_account) add_user from \n  \n   (select order_time,reap_uid,game_account,row_number() \n    over(partition by lower(game_account),reap_uid order by order_time asc) as rw from v_ods_order where order_status=4) b\n  where rw=1 and to_date(order_time)='currentday'\n   group by reap_uid\n   )  add_u\n on pu.member_id=add_u.owner_id\n where pu.status in(0,1) and pu.member_grade=1"

    val mysqlsql="insert into bi_centurioncard_opera(statistics_date,user_account,day_profit,day_income,day_add_users) values(?,?,?,?,?) " +
                                                    " on duplicate key update day_profit=?,day_income=?,day_add_users=?"
    //全部转为小写，后面好判断
    val execSql=hivesql.replace("currentday",currentday).replace("yesterday",yesterday).replace("monthfirstday",monthfirstday)  //hive sql
    val sql2Mysql=mysqlsql.toLowerCase

    //Hadoop libariy
    Hadoop.hd

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
    System.setProperty("spark.scheduler.mode", "FAIR")
    sc.setLocalProperty("spark.schedule.pool","fair")
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
