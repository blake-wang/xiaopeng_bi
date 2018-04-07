package com.xiaopeng.bi.centurioncard

import java.io.File
import java.sql.PreparedStatement
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import com.xiaopeng.bi.utils.JdbcUtil
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

object bi_centurioncard_game {

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
    //old modify 2017-10-30
    val hivesql="select user_account,game_id,channel_id,game_name,os,min(pic.pic_small) pic_small,sum(total_accounts) total_accounts\n,sum(deal_accounts) deal_accounts\n,sum(exchg_income) exchg_income\n,sum(direct_exchg_income) direct_exchg_income\n,sum(exchg_reb) exchg_reb\n,sum(deal_times) deal_times from \n(\nselect \ncase when pu.username is null then '' else pu.username end user_account,\ncase when mainid is null  then 0 else mainid end game_id,\ncase when regi.channel_id is null then  0 else regi.channel_id end channel_id,\ncase when bg.maingname is null  then '' else bg.maingname end game_name,\ncase when (lower(regi.reg_os_type) not in('IOS','android'))  then 'UNKNOW' else reg_os_type end os,\n0 as total_accounts,\n0 as deal_accounts,\ncase when sum(ori_price) is null  then 0 else sum(ori_price) end exchg_income,\ncase when sum(case when orders.prod_type=6 then ori_price else 0 end) is null then  0 \n     else sum(case when orders.prod_type=6 then ori_price else 0 end) end direct_exchg_income,\ncase when sum(rebate_amount) is null then  0 else sum(rebate_amount) end exchg_reb,\ncase when count(distinct order_no) is null then  0 else count(distinct order_no) end deal_times\nfrom ods_regi  regi \njoin v_ods_order orders on orders.game_account=regi.game_account and orders.order_status in(4,8) and orders.order_time is not null\njoin gameinfo bg on bg.id=orders.game_id  \njoin promo_user pu on pu.member_id=orders.reap_uid and pu.status in(0,1) and pu.member_grade=1\ngroup by\n pu.username,\n bg.mainid,\n regi.channel_id,\n bg.maingname,\n regi.reg_os_type\n\nunion all\n\nselect \ncase when pu.username is null then '' else pu.username end user_account,\ncase when mainid is null  then 0 else mainid end game_id,\ncase when regi.channel_id is null then  0 else regi.channel_id end channel_id,\ncase when bg.maingname is null  then '' else bg.maingname end game_name,\ncase when (lower(regi.reg_os_type) not in('IOS','android'))  then 'UNKNOW' else reg_os_type end os,\ncase when count(distinct regi.game_account) is null then 0 else count(distinct regi.game_account) end total_accounts,\ncase when count(distinct if(regi.bind_uid_time is null,null,regi.game_account)) is null then  0 \n     else count(distinct if(regi.bind_uid_time is null,null,regi.game_account)) end deal_accounts,\n0 as exchg_income,\n0 as direct_exchg_income,\n0 as exchg_reb,\n0 as deal_times\nfrom ods_regi  regi \njoin gameinfo bg on bg.id=regi.game_id  \njoin promo_user pu on pu.member_id=regi.owner_id  and pu.status in(0,1) and pu.member_grade=1\ngroup by\n pu.username,\n bg.mainid,\n regi.channel_id,\n bg.maingname,\n regi.reg_os_type\n \n) rs join\n(select \n if(mainid is null,bg.id,mainid) as mainid,pic_small,row_number() over(partition by if(mainid is null,bg.id,mainid) order by status) rw\n from\n bgame bg  join game_mapping gm \n on gm.deputyid=bg.id\n join game_main ga on \n ga.id=gm.mainid\n ) pic\non pic.mainid=rs.game_id where rw=1\ngroup by user_account,game_id,channel_id,game_name,os \n"
    val mysqlsql="insert into bi_centurioncard_game(user_account,game_id,channel_id,game_name,os,game_icon,game_accounts,deal_accounts,exchg_income,direct_exchg_income,exchg_reb,deal_times)" +
                  " values(?,?,?,?,?,?,?,?,?,?,?,?)" +
                  " on duplicate key update  os=?,game_accounts=?,deal_accounts=?,exchg_income=?,direct_exchg_income=?,exchg_reb=?,deal_times=?"
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
    val sc = new SparkContext(sparkConf)
    val sqlContext=new HiveContext(sc)
    sqlContext.sql("use yyft")
    val dataf: DataFrame = sqlContext.sql(execSql)//执行hive sql

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
//    var js=0
//    dataf.collect().foreach(x=>
//    {
//      var conn=new com.org.proj.util.util().getConn()
//      var ps : PreparedStatement =conn.prepareStatement(sql2Mysql)
//      //补充value值
//      js=js+1
//      for(rs<-0 to valueArray.length-1)
//      {
//        ps.setString(rs.toInt+1,x.get(rs).toString)
//      }
//      //补充条件
//      for(i<-0 to wh.length-1)
//      {
//        val rs=wh(i).trim.substring(0,wh(i).trim.lastIndexOf("="))
//        for(ii<-0 to cols.length-1)
//        {
//          if(cols(ii).trim.equals(rs))
//          {
//            ps.setString(i.toInt + valueArray.length.toInt + 1, x.get(ii).toString)
//          }
//        }
//      }
//      ps.executeUpdate()
//      conn.close()
//    }
//    )

    System.clearProperty("spark.driver.port")
    sc.stop()

  }

}

