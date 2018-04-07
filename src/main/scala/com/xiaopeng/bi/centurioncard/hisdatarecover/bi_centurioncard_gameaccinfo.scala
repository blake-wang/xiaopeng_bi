package com.xiaopeng.bi.centurioncard.hisdatarecover

import java.io.File
import java.sql.PreparedStatement
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import com.xiaopeng.bi.utils.JdbcUtil
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

object bi_centurioncard_gameaccinfo {

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

    val hivesql="select \ncase when pu.member_id is null then 0 else pu.member_id end member_id,\ncase when pu.username is null then '' else pu.username end user_account,\n\ncase when orders.game_id is null  then 0 else orders.game_id end game_id,\n\ncase when bg.name is null  then '' else bg.name end game_name,\n\ncase when regi.game_account is null then '' else regi.game_account end game_account,\n\ncase when regi.reg_resource=1 then '分包注册' else '代充账号' end resource,\n\ncase when regi.reg_time is null then '0000-00-00 00:00:00' else regi.reg_time end gameacc_create_time,\n--直充\ncase when sum(case when orders.prod_type=6 then orders.ori_price else 0 end) is null then  0 else sum(case when orders.prod_type=6 then orders.ori_price else 0 end) end direct_income,\n\n--首充\ncase when sum(case when orders.prod_type in(1,3) then orders.ori_price else 0 end) is null then 0 else sum(case when orders.prod_type in(1,3) then orders.ori_price else 0 end) end as first_income,\n--续充(所有续充-玩家续充)\ncase when sum(case when orders.prod_type in(2,4) then orders.ori_price else 0 end) is null then 0 else sum(case when orders.prod_type in(2,4) then orders.ori_price else 0 end) end\n-case when sum(case when (orders.reap_uid!=orders.userid and orders.prod_type in(2,4) and reap_uid!=0 and orders.userid!=0 ) then orders.ori_price else 0 end) is null then 0 \n     else sum(case when (orders.reap_uid!=orders.userid and orders.prod_type in(2,4)  and reap_uid!=0 and orders.userid!=0) then orders.ori_price else 0 end) end conti_income,\n--玩家续充\ncase when sum(case when (orders.reap_uid!=orders.userid and orders.prod_type in(2,4) and reap_uid!=0 and orders.userid!=0 ) then orders.ori_price else 0 end) is null then 0 \n     else sum(case when (orders.reap_uid!=orders.userid and orders.prod_type in(2,4)  and reap_uid!=0 and orders.userid!=0) then orders.ori_price else 0 end) end wj_conti_income,\n     \n--累计返利\ncase when sum(orders.rebate_amount) is null then 0 else sum(orders.rebate_amount) end as  tocurr_rebate,\n--总流水\ncase when sum(orders.ori_price) is null then 0 else sum(orders.ori_price) end as total_income\n\nfrom ods_regi regi \n\n join v_ods_order orders on orders.game_account=regi.game_account \n  join bgame bg on bg.id=orders.game_id  \n join promo_user pu on pu.member_id=orders.reap_uid  and  pu.status in(0,1) and pu.member_grade=1\nwhere orders.order_status in(4,8) and prod_type in(1,2,3,4,6) \ngroup by\npu.member_id, pu.username,orders.game_id,bg.name,reg_resource,regi.reg_time,regi.game_account"

    val mysqlsql="insert into bi_centurioncard_gameaccinfo(member_id,user_account,game_id,game_name,game_account,resource,gameacc_create_time,direct_income,first_income" +
      ",conti_income,wj_conti_income,tocurr_rebate,total_income)" +
                  " values(?,?,?,?,?,?,?,?,?,?,?,?,?)" +
                  " on duplicate key update direct_income=?,first_income=?,conti_income=?,wj_conti_income=?,tocurr_rebate=?,total_income=?"


    val execSql=hivesql.replace("currentday",currentday).replace("yesterday",yesterday).replace("monthfirstday",monthfirstday)  //hive sql
    //全部转为小写，后面好判断
    val sql2Mysql=mysqlsql.toLowerCase

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
