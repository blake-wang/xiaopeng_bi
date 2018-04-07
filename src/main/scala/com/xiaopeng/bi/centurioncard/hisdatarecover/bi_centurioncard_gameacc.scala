package com.xiaopeng.bi.centurioncard.hisdatarecover

import java.io.File
import java.sql.PreparedStatement
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import com.xiaopeng.bi.utils.JdbcUtil
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

object bi_centurioncard_gameacc {
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
    //年第一天
    val yearfirstday=args(0).substring(0,4)+"-01-01"

    val hivesql="select \nif(dt is null,'0000-00-00',dt) statistics_date,\ncase when pu.username is null then '' else pu.username end user_account,\ncase when regi.game_account is null then '' else regi.game_account end game_account ,\n--历史没有分包注册，只有账号卖出（1-分包注册 2-代充账号 0-新增用户）\ncase when regi.reg_resource =1 then 1 else 2 end reg_type ,\n--2-直充、平台1-续充、平台0-首充三种\ncase when orders.prod_type in(1,3) then 0 \n     when orders.prod_type in(2,4) then 1\n     when orders.prod_type=6 then 2\n     else 100\n     end rec_type,\ncase when orders.ori_price is null then 0 else orders.ori_price end rec_amount,\ncase when orders.rebate_amount is null then 0 else orders.rebate_amount end reb_amount,\ncase when orders.order_no is null then '' else orders.order_no end order_no ,\ncase when orders.order_status is null then 0 else orders.order_status end order_status ,\ncase when bg.maingname is null then '' else bg.maingname end game_name,\ncase when bg.pic_small is null then '' else bg.pic_small end game_icon,\ncase when regi.bind_uid_time is null then '0000-00-00 00:00:00' else regi.bind_uid_time end game_regi_date,\ncase when reg_os_type  is null then 'UNKNOW' else reg_os_type end os ,\ncase when mainid is null then 0 else mainid end game_id,\ncase when regi.channel_id is null then 0 else regi.channel_id end channel_id,\ncase when regi.bind_uid_time is null then 0 \n     when regi.bind_uid_time<order_time then 0 \n     when regi.bind_uid_time>=order_time and regi.owner_id!=reap_uid then 0 else 1 end is_user_add,\nif(order_time is null,'0000-00-00',order_time) order_time,\nif(regi.bind_uid_time is null,'0000-00-00',bind_uid_time) game_regi_time\n\nfrom ods_regi regi \n--订单信息\n join\n (\n  select to_date(ors.order_time) as dt,ors.order_no,game_id,ors.game_account,\n    case \n         when order_status=1 then '已付款'\n         when order_status=4 then '已完成'\n         when order_status=8 then '已退款'\n     end as order_status,\n     ors.prod_type,rebate_amount,ori_price,reap_uid,order_time from v_ods_order ors where order_status in(4,8)  and to_date(order_time)<='currentday' and to_date(order_time)>='startday'\n  ) orders\n  on orders.game_account=regi.game_account\n   join gameinfo bg on bg.id=orders.game_id \n   join promo_user pu on pu.member_id=orders.reap_uid and pu.status in(0,1) and pu.member_grade=1"
    val mysqlsql="insert into bi_centurioncard_gameacc(statistics_date,user_account,game_account,reg_type,rec_type,rec_amount,reb_amount,order_no,order_status,game_name,game_icon,game_regi_date,os,game_id,channel_id,is_user_add,order_time,game_regi_time)" +
                  " values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)" +
                  " on duplicate key update reg_type=?,rec_type=?,rec_amount=?,reb_amount=?,order_status=?,game_icon=?,is_user_add=?,order_time=?,game_regi_time=?"

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
    dataf.show()

    /********************数据库操作***************   ****/

    dataf.collect().foreach(x=>
    {
      val conn = JdbcUtil.getConn()
      val ps : PreparedStatement =conn.prepareStatement(sql2Mysql)
      //补充value值
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