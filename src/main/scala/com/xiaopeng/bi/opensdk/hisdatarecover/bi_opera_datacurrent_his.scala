package com.xiaopeng.bi.opensdk.hisdatarecover

import java.io.File
import java.sql.PreparedStatement

import com.xiaopeng.bi.utils.{ConfigurationUtil, JdbcUtil}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

object bi_opera_datacurrent_his {

  def main(args: Array[String]): Unit = {

    if (args.length < 2) {
      System.err.println("Usage: <startday><currentday> ")
      System.exit(1)
    }
    if (args.length > 2) {
      System.err.println("参数个数传入太多，固定为2个： <startday><currentday>  ")
      System.exit(1)
    }
    //跑数日期
    val currentday=args(1)
    //start
    val startdday=args(0)

    //val hivesql="\nselect \nif(dt is null,'0000-00-00',dt) statistics_date\n,if(ch_game_id is null ,0,ch_game_id) as game_key\n,if(channel_id is null ,0,channel_id) channel_id\n,if(sum(hys) is null ,0,sum(hys))  active_users\n,if(sum(add_user) is null ,0,sum(add_user))  add_users\n,if(sum(add_user_pay) is null ,0,sum(add_user_pay)) add_pay_users\n,if(sum(pay_users) is null ,0,sum(pay_users)) pay_users\n,if(sum(amount) is null ,0,sum(amount)) pay_amount\n,case when (sum(add_user) is null or sum(add_user)=0) then 0 else sum(add_user_pay)/sum(add_user) end as  add_pay_rate --新增用户付费率\n,case when (sum(hys) is null or sum(hys)=0) then 0 else sum(pay_users)/sum(hys) end as  active_pay_rate        --活跃用户付费率\n,case when (sum(hys) is null or sum(hys)=0) then 0 else sum(amount)/sum(hys) end as   pay_active_per          --平均每用户付费金额 付费金额/活跃用户数\n,case when (sum(pay_users) is null or sum(pay_users)=0) then 0 else sum(amount)/sum(pay_users) end as  pay_amount_per      --人均付款金额\nfrom \n(\n  \n --所有的数据都要有\n  select  to_date(dd.date_value) as dt,bgc.game_id,bgc.channel_id,ch_game_id\n          ,0 as hys \n          ,0 add_user\n          ,0 as add_user_pay  \n          ,0 as pay_users  \n          ,0.0 as amount   \n  from (select date_value from dim_day where to_date (date_value)>='startdday' and to_date (date_value)<='currentday' and to_date (date_value)>='2016-09-01') dd join bgame_channel bgc on 1=1 \n  where bgc.status=0\n\n\n  union all\n  \n  \n--新增用户数，新增用户付费数,活跃数\n  \nSELECT to_date(lg.login_time) as dt,regi.game_id,regi.channel_id,gc.ch_game_id \n,count(distinct lg.game_account) as hys \n,count(distinct case when lg.rw=1 then lg.game_account else null end) add_user\n,count(distinct case when lg.rw=1 then ord.account else null end) as add_user_pay \n,0 as pay_users \n,0.0 as amount \nFROM \n(\nselect login_time,game_account,rw from \n       (select game_account,to_date(login_time) login_time,row_number() over(PARTITION by game_account order by login_time ) rw  from \n       (select * from ods_login union all select * from archive.ods_login_archive) c\n       ) lg \n     where \n     to_date (login_time)>='startdday' and to_date (login_time)<='currentday' and to_date (login_time)>='2016-09-01'\n)\n lg \n   join ods_regi regi on lg.game_account=regi.game_account \njoin bgame_channel gc on gc.game_id=regi.game_id and gc.channel_id=regi.channel_id and gc.status=0\nleft join pywsdk_cp_req ord on ord.account=lg.game_account and ord.status=1 \n  and to_date (ord.req_time)>='startdday' and to_date (ord.req_time)<='currentday' and to_date (ord.req_time)>='2016-09-01'\n  and cp_order_no is not null --cp订单号不能为空\ngroup by \nto_date (lg.login_time),regi.game_id,regi.channel_id,gc.ch_game_id  \n\nunion ALL \n--付费金额，付费人数\nselect to_date (od.req_time) dt,regi.game_id,regi.channel_id,bgc.ch_game_id \n,0 as hys\n,0 as add_user\n,0 as add_user_pay \n,count(distinct od.account) as users  \n,sum(od.amount) amount  \nfrom pywsdk_cp_req od\njoin ods_regi regi on regi.game_account=od.account\njoin bgame_channel bgc ON regi.game_id=bgc.game_id\nand regi.channel_id=bgc.channel_id and bgc.status=0\nwhere \n  to_date (od.req_time)>='startdday' and to_date (od.req_time)<='currentday'  and to_date (od.req_time)>='2016-09-01'\n  and od.status=1\n  and cp_order_no is not null  --cp订单号不能为空\ngroup by \nto_date (od.req_time),regi.game_id,regi.channel_id,bgc.ch_game_id \n\n  \n) mid\ngroup by\ndt,game_id,channel_id,ch_game_id"
    val hivesql="select \nif(dt is null,'0000-00-00',dt) statistics_date\n,if(ch_game_id is null ,0,ch_game_id) as game_key\n,if(channel_id is null ,0,channel_id) channel_id\n,if(sum(pay_users) is null ,0,sum(pay_users)) pay_users\n,if(sum(amount) is null ,0,sum(amount)) pay_amount\n,case when (sum(pay_users) is null or sum(pay_users)=0) then 0 else sum(amount)/sum(pay_users) end as  pay_amount_per \nfrom \n(\n --付费金额，付费人数\nselect to_date (od.req_time) dt,regi.gameid game_id,regi.account_channel channel_id,bgc.ch_game_id ,count(distinct od.account) as pay_users ,sum(od.amount) amount  \nfrom pywsdk_cp_req od\njoin bgameaccount regi on lower(trim(regi.account))=od.account\njoin (select distinct game_id,channel_id,ch_game_id from bgame_channel) bgc ON regi.gameid=bgc.game_id\nand regi.account_channel=bgc.channel_id \nwhere \n  to_date (od.req_time)>='startdday' and to_date (od.req_time)<='currentday'  and to_date (od.req_time)>='2016-09-01'\n  and od.status=1\ngroup by \nto_date (od.req_time),regi.gameid,regi.account_channel,bgc.ch_game_id \n) mid\ngroup by\ndt,channel_id,ch_game_id"
    val mysqlsql=" insert into bi_opera_data(statistics_date,game_key,channel_id,pay_users,pay_amount,pay_amount_per)" +
                  " values(?,?,?,?,?,?)" +
                  " on duplicate key update pay_users=?,pay_amount=?,pay_amount_per=?"


    //全部转为小写，后面好判断
    val execSql=hivesql.replace("startdday",startdday).replace("currentday",currentday)  //hive sql
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
    sqlContext.sql(ConfigurationUtil.getProperty("hivedb"))
    val dataf = sqlContext.sql(execSql)//执行hive sql
    dataf.show()

    /********************数据库操作***************   ****/
    dataf.collect().foreach(x=>
    {
      val conn=JdbcUtil.getConn()
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