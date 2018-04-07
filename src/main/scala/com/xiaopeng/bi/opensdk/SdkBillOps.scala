package com.xiaopeng.bi.opensdk

import java.io.File

import com.xiaopeng.bi.utils.JdbcUtil
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

/**
 * Sdk 对账表
 */
object SdkBillOps {
  def main(args: Array[String]): Unit = {

    //跑数日期
    val startdday = args(0)
    val currentday = args(1)

    val hivesql = "select to_date (od.req_time) month_date,od.game_id,if(regi.account_channel is null ,0,regi.account_channel) account_channel,\nif(regi.os is null,'',regi.os) os,sum(od.amount) amount,\nif(bg.corporation_name is null ,'',bg.corporation_name) corporation_name\nfrom pywsdk_cp_req od\nleft join bgameaccount regi on lower(trim(regi.account))=lower(trim(od.account))\nleft join bgame_channel bgc ON regi.gameid=bgc.game_id\nand regi.account_channel=bgc.channel_id and bgc.status=0\nleft join bgame bg on od.game_id = bg.id\nwhere to_date(od.req_time) >='startdday' and  to_date(od.req_time) <'currentday'\n  and od.status=1\n  and cp_order_no is not null\ngroup by \nto_date (od.req_time),od.game_id,regi.account_channel,regi.os,bg.corporation_name"
    val execSql = hivesql.replace("startdday", startdday).replace("currentday", currentday) //hive sql

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

      val sqlText = " insert into bi_sdk_bills(month_date,game_id,channel_id,os,amount,corporation_name)" +
        " values(?,?,?,?,?,?) on duplicate key update amount=?,corporation_name=?"
      val params = new ArrayBuffer[Array[Any]]()
      for (insertedRow <- rows) {
        params.+=(Array[Any](insertedRow.get(0),insertedRow.get(1),insertedRow.get(2),
                      if(insertedRow.get(3)==null) "" else insertedRow.get(3),insertedRow.get(4),insertedRow.get(5),insertedRow.get(4),insertedRow.get(5)))
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