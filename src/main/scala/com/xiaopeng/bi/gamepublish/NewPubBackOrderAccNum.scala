package com.xiaopeng.bi.gamepublish


import com.xiaopeng.bi.utils.{JdbcUtil, SparkUtils}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.DataFrame
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext

import scala.collection.mutable.ArrayBuffer

/**
  * Created by bigdata on 17-8-30.
  * 8月24日新需求
  *  第n日注册充值帐号数   ->   第n日注册充值率
  */
object NewPubBackOrderAccNum {

  var yesterday = ""

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
    if (args.length > 0) {
      yesterday = args(0)
    }
    // 创建上下文
    val sparkConf = new SparkConf().setAppName(this.getClass.getName.replace("$", ""))
      .set("spark.default.parallelism", "60")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.shuffle.consolidateFiles", "true")
      .set("spark.storage.memoryFraction", "0.4")
      .set("spark.streaming.stopGracefullyOnShutdown", "true").setMaster("local[*]")
//    SparkUtils.setMaster(sparkConf)

    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)

    sqlContext.sql("use yyft")

    //------->0826新增加代码
    //第n日充值帐号数    统计维度game_account  --运营报表    插入数据表 bi_gamepublic_opera_regi_pay
    val regiAccSqloper = "select\nrz2.reg_time reg_time,\nrz2.parent_game_id parent_game_id,\nrz2.game_id game_id,\nrz2.system_type os,\nrz2.group_id group_id,\nsum(case when rz2.dur = 0 then 1 else 0 end) as pay_account_num_1day,\nsum(case when rz2.dur = 1 then 1 else 0 end) as pay_account_num_2day,\nsum(case when rz2.dur = 2 then 1 else 0 end) as pay_account_num_3day,\nsum(case when rz2.dur = 3 then 1 else 0 end) as pay_account_num_4day,\nsum(case when rz2.dur = 4 then 1 else 0 end) as pay_account_num_5day,\nsum(case when rz2.dur = 5 then 1 else 0 end) as pay_account_num_6day,\nsum(case when rz2.dur = 6 then 1 else 0 end) as pay_account_num_7day,\nsum(case when rz2.dur = 7 then 1 else 0 end) as pay_account_num_8day,\nsum(case when rz2.dur = 8 then 1 else 0 end) as pay_account_num_9day,\nsum(case when rz2.dur = 9 then 1 else 0 end) as pay_account_num_10day,\nsum(case when rz2.dur = 10 then 1 else 0 end) as pay_account_num_11day,\nsum(case when rz2.dur = 11 then 1 else 0 end) as pay_account_num_12day,\nsum(case when rz2.dur = 12 then 1 else 0 end) as pay_account_num_13day,\nsum(case when rz2.dur = 13 then 1 else 0 end) as pay_account_num_14day,\nsum(case when rz2.dur = 14 then 1 else 0 end) as pay_account_num_15day,\nsum(case when rz2.dur = 29 then 1 else 0 end) as pay_account_num_30day,\nsum(case when rz2.dur = 44 then 1 else 0 end) as pay_account_num_45day,\nsum(case when rz2.dur = 59 then 1 else 0 end) as pay_account_num_60day,\nsum(case when rz2.dur = 89 then 1 else 0 end) as pay_account_num_90day,\nsum(case when rz2.dur = 119 then 1 else 0 end) as pay_account_num_120day,\nsum(case when rz2.dur = 149 then 1 else 0 end) as pay_account_num_150day,\nsum(case when rz2.dur = 179 then 1 else 0 end) as pay_account_num_180day\nfrom\n(select rz.reg_time,gs.parent_game_id,rz.game_id,gs.system_type,gs.group_id,rz.game_account,datediff(oz.order_time,rz.reg_time) dur from\n(select distinct to_date(reg_time) reg_time,game_id,lower(trim(game_account)) game_account from ods_regi_rz where game_id is not null and game_account is not null and reg_time is not null and to_date(reg_time)>=date_add('yesterday',-179) and to_date(reg_time)<='yesterday') rz \njoin\n(select distinct game_id as parent_game_id,old_game_id,system_type,group_id from game_sdk where state = 0) gs on rz.game_id = gs.old_game_id \nleft join   \n(select distinct to_date(order_time) order_time,lower(trim(game_account)) game_account,game_id from ods_order where order_status =4 and prod_type=6) oz on rz.game_account=oz.game_account and to_date(oz.order_time)>=to_date(rz.reg_time) and datediff(oz.order_time,rz.reg_time) in (0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,29,44,59,89,119,149,179)) rz2\ngroup by rz2.reg_time,rz2.parent_game_id,rz2.game_id,rz2.system_type,rz2.group_id"
    val regiAccexecSqloper = regiAccSqloper.replace("yesterday", yesterday)
    val regiAccPayNumDataFrame = sqlContext.sql(regiAccexecSqloper)
    operaRegiAccPayNumForeachPartition(regiAccPayNumDataFrame)



    sc.stop()

  }
  def operaRegiAccPayNumForeachPartition(regiAccPayNumDataFrame: DataFrame) = {
    regiAccPayNumDataFrame.foreachPartition(rows => {
      val conn = JdbcUtil.getConn()
      val sqlText = "insert into bi_gamepublic_opera_regi_pay (\npublish_date,\nparent_game_id,\nchild_game_id,\nos,\ngroup_id,\npay_account_num_1day,\npay_account_num_2day,\npay_account_num_3day,\npay_account_num_4day,\npay_account_num_5day,\npay_account_num_6day,\npay_account_num_7day,\npay_account_num_8day,\npay_account_num_9day,\npay_account_num_10day,\npay_account_num_11day,\npay_account_num_12day,\npay_account_num_13day,\npay_account_num_14day,\npay_account_num_15day,\npay_account_num_30day,\npay_account_num_45day,\npay_account_num_60day,\npay_account_num_90day,\npay_account_num_120day,\npay_account_num_150day,\npay_account_num_180day) \nvalues (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) \non duplicate key update \nparent_game_id = values(parent_game_id),\nos = values(os),\ngroup_id = values(group_id),\npay_account_num_1day = values(pay_account_num_1day),\npay_account_num_2day = values(pay_account_num_2day),\npay_account_num_3day = values(pay_account_num_3day),\npay_account_num_4day = values(pay_account_num_4day),\npay_account_num_5day = values(pay_account_num_5day),\npay_account_num_6day = values(pay_account_num_6day),\npay_account_num_7day = values(pay_account_num_7day),\npay_account_num_8day = values(pay_account_num_8day),\npay_account_num_9day = values(pay_account_num_9day),\npay_account_num_10day = values(pay_account_num_10day),\npay_account_num_11day = values(pay_account_num_11day),\npay_account_num_12day = values(pay_account_num_12day),\npay_account_num_13day = values(pay_account_num_13day),\npay_account_num_14day = values(pay_account_num_14day),\npay_account_num_15day = values(pay_account_num_15day),\npay_account_num_30day = values(pay_account_num_30day),\npay_account_num_45day = values(pay_account_num_45day),\npay_account_num_60day = values(pay_account_num_60day),\npay_account_num_90day = values(pay_account_num_90day),\npay_account_num_120day = values(pay_account_num_120day),\npay_account_num_150day = values(pay_account_num_150day),\npay_account_num_180day = values(pay_account_num_180day)"
      val params = new ArrayBuffer[Array[Any]]()
      for (row <- rows) {
        params.+=(Array[Any](row.get(0), row.get(1), row.get(2), row.get(3), row.get(4),
          row.get(5), row.get(6), row.get(7), row.get(8), row.get(9),
          row.get(10), row.get(11), row.get(12), row.get(13), row.get(14),
          row.get(15), row.get(16), row.get(17), row.get(18), row.get(19),
          row.get(20), row.get(21), row.get(22), row.get(23), row.get(24),
          row.get(25), row.get(26)))
      }

      try {
        JdbcUtil.doBatch(sqlText, params, conn)
      } finally {
        conn.close()
      }
    })
  }

}
