package com.xiaopeng.bi.centurioncard.hisdatarecover

import java.io.File
import java.sql.PreparedStatement
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import com.xiaopeng.bi.utils.JdbcUtil
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

object bi_centurioncard_gdeta {

  def main(args: Array[String]): Unit = {

    if (args.length < 2) {
      System.err.println("Usage: <currentday><startday> ")
      System.exit(1)
    }
    if (args.length > 2) {
      System.err.println("参数个数传入太多，固定为2个： <currentday><startday>  ")
      System.exit(1)
    }
    //跑数日期
    val currentday=args(0)
    //开始日期
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
    //current
    val hivesql="select \nif(dt is null,'0000-00-00',dt) statistics_date,\ncase when pu.member_id is null then 0 else pu.member_id end member_id,\ncase when pu.username is null then ' ' else pu.username end user_account,\ncase when rs.game_id is null then 0 else rs.game_id end game_id,\ncase when rs.channel_id is null then 0 else rs.channel_id end channel_id,\ncase when rs.os is null then 'UNKNOW' else rs.os end os,\ncase when bg.name is null then ' ' else bg.name end game_name,\ncase when bc.name is null then ' ' else bc.name end channel_name,\ncase when sum(ori_price) is null then 0 else sum(ori_price) end rec_amount,\ncase when sum(rebate_amount) is null then 0 else sum(rebate_amount) end reb_amount,\ncase when sum(deal_times) is null then 0 else sum(deal_times) end deal_times,\ncase when sum(add_users) is null then 0 else sum(add_users) end add_users\nfrom \n promo_user pu\n join \n(\n  --交易笔数/充值金额(流水）/返利,退款\n  select to_date(orders.order_time) as dt,orders.reap_uid member_id,orders.game_id,orders.channel_id,regi.reg_os_type os,\n              sum(ori_price) ori_price,\n              sum(rebate_amount) rebate_amount,\n              count(distinct order_no) deal_times,\n              0 as add_users\n         from v_ods_order orders \n         left join ods_regi regi on orders.game_account=regi.game_account\n         where orders.order_status in(4,8)  and to_date(order_time)<='currentday' and to_date(order_time)>='startday'\n         group by \n         to_date(orders.order_time),orders.game_id,orders.channel_id,regi.reg_os_type,reap_uid\n              \n  \n  --新增用户\n  union all\n  select to_date(bind_uid_time) as dt,regi.owner_id member_id,regi.game_id,channel_id,reg_os_type os,\n              0 ori_price,\n              0 rebate_amount,\n              0 deal_times,\n              count(regi.game_account) add_users from \n    ods_regi regi \n    where  to_date(bind_uid_time)<='currentday' and to_date(bind_uid_time)>='startday'\n    group by regi.owner_id,to_date(bind_uid_time),game_id,channel_id,reg_os_type\n  \n ) rs\n on rs.member_id=pu.member_id and pu.member_grade=1 and pu.status in(0,1)\n \n  join \n  --渠道名称\n  (select distinct id,name from bchannel ) bc on rs.channel_id=bc.id\n  join \n  --游戏名称\n  bgame bg on rs.game_id=bg.id \n\n group by\n dt,\n pu.member_id,\n pu.username,\n rs.game_id,\n rs.channel_id,\n rs.os,\n bg.name,\n bc.name"

    val mysqlsql="insert into bi_centurioncard_gdeta(statistics_date,member_id,user_account,game_id,channel_id,os,game_name,channel_name,rec_amount,reb_amount,deal_times,add_users)" +
                  " values(?,?,?,?,?,?,?,?,?,?,?,?)" +
                  " on duplicate key update rec_amount=?,reb_amount=?,deal_times=?,add_users=?"

    //全部转为小写，后面好判断
    val execSql=hivesql.replace("currentday",currentday).replace("yesterday",yesterday).replace("monthfirstday",monthfirstday).replace("startday",startday)  //hive sql

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

    /********************数据库操作***************   ****/
    var js=0
    dataf.collect().foreach(x=>
    {
      val conn = JdbcUtil.getConn()
      val ps : PreparedStatement =conn.prepareStatement(sql2Mysql)
      //补充value值
      js=js+1
      for(rs<-0 to valueArray.length-1)
      {
        ps.setString(rs.toInt+1,x.get(rs).toString)
      }
      //补充条件
      for(i<-0 to wh.length-1)
      {
        val rs=wh(i).trim.substring(0,wh(i).trim.lastIndexOf("="))
        for(ii<-0 to cols.length-1)
        {
          if(cols(ii).trim.equals(rs))
          {
            ps.setString(i.toInt + valueArray.length.toInt + 1, x.get(ii).toString)
          }
        }
      }
      ps.executeUpdate()
      conn.close()
    }
    )
    System.clearProperty("spark.driver.port")
    sc.stop()

  }

}