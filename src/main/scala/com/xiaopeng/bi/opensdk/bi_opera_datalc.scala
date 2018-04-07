package com.xiaopeng.bi.opensdk

import java.io.File

import com.xiaopeng.bi.utils.JdbcUtil
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

object bi_opera_datalc {

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
    val hivesql = "select \nif(allrs.dt is null,'0000-00-00 00:00:00',allrs.dt) statistics_date\n,if(allrs.ch_game_id is null ,0,allrs.ch_game_id) as game_key\n,if(allrs.channel_id is null ,0,allrs.channel_id) channel_id\n,if(sum(d1) is null,0,sum(d1)) retained_1day\n,if(sum(d2) is null,0,sum(d2)) retained_2day\n,if(sum(d3) is null,0,sum(d3)) retained_3day\n,if(sum(d4) is null,0,sum(d4)) retained_4day\n,if(sum(d5) is null,0,sum(d5)) retained_5day\n,if(sum(d6) is null,0,sum(d6)) retained_6day\n,if(sum(d14) is null,0,sum(d14)) retained_14day\n,if(sum(d29) is null,0,sum(d29)) retained_29day\n\nfrom \n(\n  \n --所有的数据都要有\n  select  to_date(dd.date_value) as dt,bgc.game_id,bgc.channel_id,ch_game_id\n  from (select date_value from dim_day where to_date (date_value)<='currentday' and to_date (date_value)>date_add(\"currentday\",-30)) dd join bgame_channel bgc on 1=1 \n  where bgc.status=0\n) allrs left join\n\n\n--存留率\n(\nselect \ndt,\nrs.game_id as game_id,\nchannel_id,\nch_game_id,\nsum(CASE dur WHEN 1 THEN 1 ELSE 0 END ) as d1,\nsum(CASE dur WHEN 2 THEN 1 ELSE 0 END ) as d2,\nsum(CASE dur WHEN 3 THEN 1 ELSE 0 END ) as d3,\nsum(CASE dur WHEN 4 THEN 1 ELSE 0 END ) as d4,\nsum(CASE dur WHEN 5 THEN 1 ELSE 0 END ) as d5,\nsum(CASE dur WHEN 6 THEN 1 ELSE 0 END ) as d6,\nsum(CASE dur WHEN 14 THEN 1 ELSE 0 END ) as d14,\nsum(CASE dur WHEN 29 THEN 1 ELSE 0 END ) as d29\nFROM\n(\nselect distinct mid.game_account,bacc.game_id,bacc.channel_id,bgc.ch_game_id,mid.dt,lg.login_time,datediff(login_time,dt) as dur from \n (\n  select login_time dt,game_account from \n       (select game_account,to_date(login_time) login_time,row_number() over(PARTITION by game_account order by login_time ) rw  from ods_login) lg\n      where \n      to_date (lg.login_time)<='currentday' \n      and to_date (lg.login_time)>date_add(\"currentday\",-30)\n      and lg.rw=1\n  ) mid \njoin (select distinct game_account,to_date(f.login_time) as login_time from ods_login f) lg\n        on mid.game_account=lg.game_account \njoin ods_regi bacc on bacc.game_account =mid.game_account and bacc.status in(1,2) \njoin bgame_channel bgc on bgc.channel_id=bacc.channel_id and bgc.game_id=bacc.game_id and bgc.status=0 \nwhere to_date(login_time)>mid.dt \nand datediff(login_time,dt) in(1,2,3,4,5,6,14,29)\n) rs \ngroup BY game_id,channel_id,ch_game_id,dt\n  \n) lc\non lc.dt=allrs.dt and lc.channel_id=allrs.channel_id and lc.game_id=allrs.game_id and lc.ch_game_id=allrs.ch_game_id\ngroup by\nallrs.dt,allrs.game_id,allrs.channel_id,allrs.ch_game_id"
    val execSql = hivesql.replace("currentday", currentday) //hive sql

    setHadoopLibariy()

    /** ******************hive库操作 *******************/
    val sparkConf = new SparkConf().setAppName(this.getClass.getName.replace("$", ""))
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)
    sqlContext.sql("use yyft")
    val dataf = sqlContext.sql(execSql) //执行hive sql

    /** ******************数据库操作***************   ****/
    dataf.foreachPartition(rows => {
      val conn = JdbcUtil.getConn()

      val sqlText = " insert into bi_opera_data(statistics_date,game_key,channel_id,retained_1day,retained_2day,retained_3day,retained_4day,retained_5day,retained_6day,retained_14day,retained_29day)" +
        " values(?,?,?,?,?,?,?,?,?,?,?)" +
        " on duplicate key update retained_1day=?,retained_2day=?,retained_3day=?,retained_4day=?,retained_5day=?,retained_6day=?,retained_14day=?,retained_29day=?"
      val params = new ArrayBuffer[Array[Any]]()
      for (insertedRow <- rows) {
        params.+=(Array[Any](insertedRow.get(0), insertedRow.get(1), insertedRow.get(2),
          insertedRow.get(3), insertedRow.get(4), insertedRow.get(5), insertedRow.get(6)
          , insertedRow.get(7), insertedRow.get(8), insertedRow.get(9), insertedRow.get(10)
          , insertedRow.get(3), insertedRow.get(4), insertedRow.get(5)
          , insertedRow.get(6), insertedRow.get(7), insertedRow.get(8), insertedRow.get(9)
          , insertedRow.get(10)))
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