package com.xiaopeng.bi.opensdk

import java.io.File

import com.xiaopeng.bi.utils.JdbcUtil
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

object bi_opera_datacurrent {

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
    val currentday = args(0)
    val hivesql = "select \n'currentday' statistics_date\n,if(ch_game_id is null ,0,ch_game_id) as game_key\n,if(channel_id is null ,0,channel_id) channel_id\n,if(sum(hys) is null ,0,sum(hys))  active_users\n,if(sum(add_user) is null ,0,sum(add_user)) add_users\n,if(sum(add_user_pay) is null ,0,sum(add_user_pay)) add_pay_users\n,if(sum(pay_users) is null ,0,sum(pay_users)) pay_users\n,if(sum(amount) is null ,0,sum(amount)) pay_amount\n,case when (sum(add_user) is null or sum(add_user)=0) then 0 else sum(add_user_pay)/sum(add_user) end as  add_pay_rate --新增用户付费率\n,case when (sum(hys) is null or sum(hys)=0) then 0 else sum(pay_users)/sum(hys) end as  active_pay_rate        --活跃用户付费率\n,case when (sum(hys) is null or sum(hys)=0) then 0 else sum(amount)/sum(hys) end as   pay_active_per          --平均每用户付费金额 付费金额/活跃用户数\n,case when (sum(pay_users) is null or sum(pay_users)=0) then 0 else sum(amount)/sum(pay_users) end as  pay_amount_per      --人均付款金额\n\nfrom \n(\n  \n --所有的数据都要有\n  select  to_date(dd.date_value) as dt,bgc.game_id,bgc.channel_id,ch_game_id\n          ,0 as hys \n          ,0 as add_user \n          ,0 as add_user_pay  \n          ,0 as pay_users  \n          ,0.0 as amount   \n  from (select date_value from dim_day where to_date (date_value)='currentday') dd join bgame_channel bgc on 1=1 \n  where bgc.status=0\n\n  \n--活跃数\n  union all\n  \nSELECT to_date (lg.login_time) as dt,regi.game_id,regi.channel_id,gc.ch_game_id\n,count(distinct lg.game_account)  as hys \n,0 as add_user \n,0 as add_user_pay \n,0 as pay_users \n,0.0 as amount \nFROM ods_login lg join ods_regi regi\non lg.game_account=regi.game_account and regi.status in(1,2)\njoin bgame_channel gc on gc.game_id=regi.game_id and gc.channel_id=regi.channel_id and gc.status=0\nwhere \n  to_date (lg.login_time)='currentday' \ngroup by \nto_date (lg.login_time),regi.game_id,regi.channel_id,gc.ch_game_id \n\nunion ALL \n  \n--新增用户数，新增用户付费数\n  \nSELECT to_date(lg.login_time) as dt,regi.game_id,regi.channel_id,gc.ch_game_id \n,0 as hys \n,count(distinct lg.game_account)  as add_user\n,count(distinct ord.game_account) as add_user_pay \n,0 as pay_users \n,0.0 as amount \nFROM \n(\nselect login_time,game_account from \n       (select game_account,to_date(login_time) login_time,row_number() over(PARTITION by game_account order by login_time) rw  from ods_login) lg \n     where \n     lg.rw=1 and to_date (login_time)='currentday' \n)\n lg join ods_regi regi\non lg.game_account=regi.game_account and regi.status in(1,2)\njoin bgame_channel gc on gc.game_id=regi.game_id and gc.channel_id=regi.channel_id and gc.status=0\nleft join ods_order ord on ord.game_account=lg.game_account \n  and to_date (ord.order_time)=='currentday' \n  and ord.order_status=4 and ord.order_resource=2  --只取sdk来源订单\ngroup by \nto_date (lg.login_time),regi.game_id,regi.channel_id,gc.ch_game_id \n\nunion ALL \n--付费金额，付费人数\nselect to_date (od.order_time) dt,od.game_id,od.channel_id,bgc.ch_game_id \n,0 as hys \n,0 as add_user \n,0 as add_user_pay \n,count(distinct od.game_account) as users  \n,sum(od.cp_payment_amoumt) amount  \nfrom ods_order od\njoin bgame_channel bgc ON\nod.game_id=bgc.game_id\nand od.channel_id=bgc.channel_id and bgc.status=0\nwhere \n  to_date (od.order_time)='currentday' \n  and od.cp_status = 1 and od.order_status=4 and od.order_resource=2  --只取sdk来源订单\ngroup by \nto_date (od.order_time),od.game_id,od.channel_id,bgc.ch_game_id \n\n  \n) mid\ngroup by\ndt,game_id,channel_id,ch_game_id"
    val execSql = hivesql.replace("currentday", currentday) //hive sql

    setHadoopLibariy()

    /** ******************hive库操作 *******************/
    val sparkConf = new SparkConf().setAppName(this.getClass.getName.replace("$", ""))
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)
    sqlContext.sql("use yyft")
    val dataf = sqlContext.sql(execSql) //执行hive sql

    /** ******************数据库操作 *******************/
    dataf.foreachPartition(rows => {
      val conn = JdbcUtil.getConn()

      val sqlText = " insert into bi_opera_data(statistics_date,game_key,channel_id,active_users,add_users,add_pay_users,pay_users,pay_amount,add_pay_rate,active_pay_rate,pay_active_per,pay_amount_per)" +
        " values(?,?,?,?,?,?,?,?,?,?,?,?)" +
        " on duplicate key update active_users=?,add_users=?,add_pay_users=?,pay_users=?,pay_amount=?," +
        "add_pay_rate=?,active_pay_rate=?,pay_active_per=?,pay_amount_per=?"
      val params = new ArrayBuffer[Array[Any]]()
      for (insertedRow <- rows) {
        params.+=(Array[Any](insertedRow.get(0), insertedRow.get(1), insertedRow.get(2),
          insertedRow.get(3), insertedRow.get(4), insertedRow.get(5), insertedRow.get(6)
          , insertedRow.get(7), insertedRow.get(8), insertedRow.get(9), insertedRow.get(10)
          , insertedRow.get(11), insertedRow.get(3), insertedRow.get(4), insertedRow.get(5)
          , insertedRow.get(6), insertedRow.get(7), insertedRow.get(8), insertedRow.get(9)
          , insertedRow.get(10), insertedRow.get(11)))
      }
      JdbcUtil.doBatch(sqlText, params, conn)
      conn.close
    })

    System.clearProperty("spark.driver.port")
    sc.stop()
  }

  def setHadoopLibariy(): Unit = {
    //Hadoop libariy
    val path: String = new File(".").getCanonicalPath
    System.getProperties().put("hadoop.home.dir", path)
    new File("./bin").mkdirs()
    new File("./bin/winutils.exe").createNewFile()
  }
}
