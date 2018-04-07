package com.xiaopeng.bi.gamepublish

import com.xiaopeng.bi.utils._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by root on 5/12/17.
  * 只跑一天登录的留存数据，提高效率，但是数据出问题的时候，全量更新数据，还是要跑 NewPubBackRetained 任务
  */
object NewPubBackRetainedDay {
  var startDay = "";
  var endDay = ""
  
  def main(args: Array[String]): Unit = {

    //跑数日期
    startDay = args(0)
    endDay = args(1)
    
    val sparkConf = new SparkConf().setAppName(this.getClass.getName.replace("$", ""))
      .set("spark.memory.storageFraction", ConfigurationUtil.getProperty("spark.memory.storageFraction"))
      .set("spark.sql.shuffle.partitions", ConfigurationUtil.getProperty("spark.sql.shuffle.partitions")) //.setMaster("local[4]"
    SparkUtils.setMaster(sparkConf)

    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)

    sqlContext.read.parquet(ConfigurationUtil.getProperty("fxdim.parquet")).registerTempTable("fxdim")
    sqlContext.sql("use yyft")

    val sql_regi="select distinct to_date(reg_time) reg_time,game_id,if(expand_channel is null,'no_acc',if(expand_channel='','21',expand_channel)) expand_channel,imei,game_account from  ods_regi_rz  where to_date(reg_time)<='endDay' and to_date (reg_time) >= date_add('startDay',-179) and game_id is not null and game_account is not null"
      .replace("startDay", startDay).replace("endDay", endDay)
    sqlContext.sql(sql_regi).persist().registerTempTable("rz_regi")

    val sql_login="select distinct to_date(login_time) login_time,lower(trim(game_account)) game_account,imei,game_id,if(channel_expand is null,'no_acc',if(channel_expand='','21',channel_expand)) channel_expand from ods_login  where to_date(login_time)<='endDay'  and to_date(login_time)>='startDay'"
      .replace("startDay", startDay).replace("endDay", endDay)
    sqlContext.sql(sql_login).persist().registerTempTable("lz_login")

    //----------按设备统计留存   投放报表 到包维度 ----------------------------
    val sql ="select    rs.reg_time reg_time,   rs.game_id game_id, rs.expand_channel expand_channel,   \nsum(CASE rs.dur WHEN 1 THEN 1 ELSE 0 END ) as retained_2day,\nsum(CASE rs.dur WHEN 2 THEN 1 ELSE 0 END ) as retained_3day,\nsum(CASE rs.dur WHEN 3 THEN 1 ELSE 0 END ) as retained_4day,\nsum(CASE rs.dur WHEN 4 THEN 1 ELSE 0 END ) as retained_5day,\nsum(CASE rs.dur WHEN 5 THEN 1 ELSE 0 END ) as retained_6day,\nsum(CASE rs.dur WHEN 6 THEN 1 ELSE 0 END ) as retained_7day,\nsum(CASE rs.dur WHEN 7 THEN 1 ELSE 0 END ) as retained_8day,\nsum(CASE rs.dur WHEN 8 THEN 1 ELSE 0 END ) as retained_9day,\nsum(CASE rs.dur WHEN 9 THEN 1 ELSE 0 END ) as retained_10day,\nsum(CASE rs.dur WHEN 10 THEN 1 ELSE 0 END ) as retained_11day,\nsum(CASE rs.dur WHEN 11 THEN 1 ELSE 0 END ) as retained_12day,\nsum(CASE rs.dur WHEN 12 THEN 1 ELSE 0 END ) as retained_13day,\nsum(CASE rs.dur WHEN 13 THEN 1 ELSE 0 END ) as retained_14day,\nsum(CASE rs.dur WHEN 14 THEN 1 ELSE 0 END ) as retained_15day,\nsum(CASE rs.dur WHEN 29 THEN 1 ELSE 0 END ) as retained_30day \n FROM (\n select distinct rz.reg_time,rz.game_id,rz.expand_channel expand_channel,datediff(odsl.login_time,rz.reg_time) dur,rz.imei \n from \n (select distinct reg_time,game_id,expand_channel,imei from rz_regi  where to_date(reg_time)<='endDay' and to_date (reg_time) >= date_add('startDay',-29))\n rz\n join (select distinct old_game_id  from yyft.game_sdk where state=0) sdk on sdk.old_game_id=rz.game_id\n join (select distinct game_id,channel_expand,imei,login_time from lz_login) odsl on rz.game_id = odsl.game_id  and rz.expand_channel = odsl.channel_expand  and rz.imei = odsl.imei  \n where datediff(odsl.login_time,rz.reg_time) in(1,2,3,4,5,6,7,8,9,10,11,12,13,14,29) \n ) rs  \n group BY rs.reg_time,rs.game_id,rs.expand_channel"
      .replace("startDay", startDay).replace("endDay", endDay)
    sqlContext.sql(sql).registerTempTable("result_retained_tmp")
    val deviceRetainedDf = sqlContext.sql("select rrt.*,f.promotion_channel,f.medium_account,f.promotion_mode,f.head_people,gs.game_id parent_game_id,gs.system_type os,gs.group_id from result_retained_tmp rrt left join fxdim f on split(rrt.expand_channel,'_')[2] = f.pkg_code and rrt.reg_time>=f.mstart_date and rrt.reg_time<=f.mend_date and rrt.reg_time>=f.astart_date and rrt.reg_time<=f.aend_date join game_sdk gs on rrt.game_id = gs.old_game_id where gs.state=0 ")
    deviceRetainedForeachPartition(deviceRetainedDf)

    //----------按设备统计留存   运营报表 到游戏维度----------------------------
    val devRetainedexecSql ="select    \nrs.reg_time,   \nrs.game_id, \nrs.parent_game_id,\nrs.system_type,\nrs.group_id,\nsum(CASE rs.dur WHEN 0 THEN 1 ELSE 0 END ) as retained_1day,\nsum(CASE rs.dur WHEN 1 THEN 1 ELSE 0 END ) as retained_2day,\nsum(CASE rs.dur WHEN 2 THEN 1 ELSE 0 END ) as retained_3day,\nsum(CASE rs.dur WHEN 3 THEN 1 ELSE 0 END ) as retained_4day,\nsum(CASE rs.dur WHEN 4 THEN 1 ELSE 0 END ) as retained_5day,\nsum(CASE rs.dur WHEN 5 THEN 1 ELSE 0 END ) as retained_6day,\nsum(CASE rs.dur WHEN 6 THEN 1 ELSE 0 END ) as retained_7day,\nsum(CASE rs.dur WHEN 7 THEN 1 ELSE 0 END ) as retained_8day,\nsum(CASE rs.dur WHEN 8 THEN 1 ELSE 0 END ) as retained_9day,\nsum(CASE rs.dur WHEN 9 THEN 1 ELSE 0 END ) as retained_10day,\nsum(CASE rs.dur WHEN 10 THEN 1 ELSE 0 END ) as retained_11day,\nsum(CASE rs.dur WHEN 11 THEN 1 ELSE 0 END ) as retained_12day,\nsum(CASE rs.dur WHEN 12 THEN 1 ELSE 0 END ) as retained_13day,\nsum(CASE rs.dur WHEN 13 THEN 1 ELSE 0 END ) as retained_14day,\nsum(CASE rs.dur WHEN 14 THEN 1 ELSE 0 END ) as retained_15day,\nsum(CASE rs.dur WHEN 29 THEN 1 ELSE 0 END ) as retained_30day,\nsum(CASE rs.dur WHEN 44 THEN 1 ELSE 0 END ) as retained_45day, \nsum(CASE rs.dur WHEN 59 THEN 1 ELSE 0 END ) as retained_60day, \nsum(CASE rs.dur WHEN 89 THEN 1 ELSE 0 END ) as retained_90day, \nsum(CASE rs.dur WHEN 119 THEN 1 ELSE 0 END ) as retained_120day, \nsum(CASE rs.dur WHEN 149 THEN 1 ELSE 0 END ) as retained_150day,\nsum(CASE rs.dur WHEN 179 THEN 1 ELSE 0 END ) as retained_180day  \n FROM (  \n select distinct sdk.parent_game_id,sdk.system_type,sdk.group_id,filterOdsR.reg_time,filterOdsR.game_id, datediff(odsl.login_time,filterOdsR.reg_time) dur,filterOdsR.imei from \n (select distinct reg_time,game_id,imei from rz_regi)\n filterOdsR    \n join (select  distinct game_id as parent_game_id ,old_game_id,system_type,group_id from game_sdk  where state=0) sdk on sdk.old_game_id=filterOdsR.game_id       \n join (select distinct login_time,imei,game_id from lz_login) odsl on filterOdsR.game_id = odsl.game_id  and filterOdsR.imei = odsl.imei \n where  datediff(odsl.login_time,filterOdsR.reg_time) in(0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,29,44,59,89,119,149,179) \n ) rs  \n group BY rs.reg_time,rs.game_id,rs.parent_game_id,rs.system_type,rs.group_id"
    val operaDeviceRetainedDf = sqlContext.sql(devRetainedexecSql)
    operaDeviceRetainedForeachPartition(operaDeviceRetainedDf)

    //----------按帐号统计留存    投放报表  到包的维度----------------------------
    val accountRetainedSql ="select  rs.reg_time reg_time, rs.game_id game_id, rs.expand_channel expand_channel,\nsum(CASE rs.dur WHEN 0 THEN 1 ELSE 0 END ) as retained_1day, \nsum(CASE rs.dur WHEN 1 THEN 1 ELSE 0 END ) as retained_2day, \nsum(CASE rs.dur WHEN 2 THEN 1 ELSE 0 END ) as retained_3day, \nsum(CASE rs.dur WHEN 3 THEN 1 ELSE 0 END ) as retained_4day, \nsum(CASE rs.dur WHEN 4 THEN 1 ELSE 0 END ) as retained_5day, \nsum(CASE rs.dur WHEN 5 THEN 1 ELSE 0 END ) as retained_6day, \nsum(CASE rs.dur WHEN 6 THEN 1 ELSE 0 END ) as retained_7day, \nsum(CASE rs.dur WHEN 7 THEN 1 ELSE 0 END ) as retained_8day, \nsum(CASE rs.dur WHEN 8 THEN 1 ELSE 0 END ) as retained_9day, \nsum(CASE rs.dur WHEN 9 THEN 1 ELSE 0 END ) as retained_10day, \nsum(CASE rs.dur WHEN 10 THEN 1 ELSE 0 END ) as retained_11day, \nsum(CASE rs.dur WHEN 11 THEN 1 ELSE 0 END ) as retained_12day, \nsum(CASE rs.dur WHEN 12 THEN 1 ELSE 0 END ) as retained_13day, \nsum(CASE rs.dur WHEN 13 THEN 1 ELSE 0 END ) as retained_14day, \nsum(CASE rs.dur WHEN 14 THEN 1 ELSE 0 END ) as retained_15day,   \nsum(CASE rs.dur WHEN 29 THEN 1 ELSE 0 END ) as retained_30day,\nsum(CASE rs.dur WHEN 44 THEN 1 ELSE 0 END ) as retained_45day, \nsum(CASE rs.dur WHEN 59 THEN 1 ELSE 0 END ) as retained_60day, \nsum(CASE rs.dur WHEN 89 THEN 1 ELSE 0 END ) as retained_90day, \nsum(CASE rs.dur WHEN 119 THEN 1 ELSE 0 END ) as retained_120day, \nsum(CASE rs.dur WHEN 149 THEN 1 ELSE 0 END ) as retained_150day,\nsum(CASE rs.dur WHEN 179 THEN 1 ELSE 0 END ) as retained_180day   \nFROM \n(\nselect distinct rz.reg_time,rz.game_id,rz.expand_channel expand_channel, datediff(odsl.login_time,rz.reg_time) dur,rz.game_account from    \n(select distinct reg_time,game_id,expand_channel,game_account from rz_regi)\nrz\njoin (select distinct old_game_id from yyft.game_sdk where state=0) gs on gs.old_game_id=rz.game_id\njoin (select distinct login_time,game_account from lz_login) odsl on rz.game_account = odsl.game_account   where  datediff(odsl.login_time,rz.reg_time) in(0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,29,44,59,89,119,149,179) \n) rs\ngroup BY rs.reg_time,rs.game_id,rs.expand_channel"
    val execSql = accountRetainedSql
    sqlContext.sql(execSql).persist().registerTempTable("account_retained_tmp")
    val accountRetainedDf = sqlContext.sql("select art.*,f.promotion_channel,f.medium_account,f.promotion_mode,f.head_people,gs.game_id parent_game_id,gs.system_type os,gs.group_id from account_retained_tmp art left join fxdim f on split(art.expand_channel,'_')[2] = f.pkg_code and art.reg_time>=f.mstart_date and art.reg_time<=f.mend_date and art.reg_time>=f.astart_date and art.reg_time<=f.aend_date left join game_sdk gs on art.game_id = gs.old_game_id where gs.state=0")
    accountRetainedForeachPartition(accountRetainedDf)

    //--------注册登录账户数 运营报表  其实就是0日留存 ---------------
    val operDevaccSql = "select art.reg_time,gs.game_id,art.game_id,gs.system_type,gs.group_id,sum(retained_1day) regi_login_num \nfrom \n(select distinct  reg_time,game_id,expand_channel,retained_1day from account_retained_tmp where retained_1day>0)\nart join (select distinct game_id,old_game_id,system_type,group_id from yyft.game_sdk where state=0) gs on gs.old_game_id=art.game_id\ngroup by  art.reg_time,gs.game_id,art.game_id,gs.system_type,gs.group_id"
    val operDevaccDf = sqlContext.sql(operDevaccSql)
    operDevaccForeachPartition(operDevaccDf)

    System.clearProperty("spark.driver.port")
    sc.stop()
  }


  private def deviceRetainedForeachPartition(deviceRetainedDf: DataFrame) = {
    deviceRetainedDf.foreachPartition(rows => {

      val conn = JdbcUtil.getConn()
      val statement = conn.createStatement

      val sqlText ="insert into bi_gamepublic_actions(publish_date,child_game_id,medium_channel,ad_site_channel,pkg_code,dev_retained_2day,dev_retained_3day,dev_retained_4day,dev_retained_5day,dev_retained_6day,dev_retained_7day,dev_retained_8day,dev_retained_9day,dev_retained_10day,dev_retained_11day,dev_retained_12day,dev_retained_13day,dev_retained_14day,dev_retained_15day,dev_retained_30day,promotion_channel,medium_account,promotion_mode,head_people,parent_game_id,os,group_id)values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)\non duplicate key update \ndev_retained_2day=if(values(dev_retained_2day)>0,values(dev_retained_2day),dev_retained_2day),\ndev_retained_3day=if(values(dev_retained_3day)>0,values(dev_retained_3day),dev_retained_3day),\ndev_retained_4day=if(values(dev_retained_4day)>0,values(dev_retained_4day),dev_retained_4day),\ndev_retained_5day=if(values(dev_retained_5day)>0,values(dev_retained_5day),dev_retained_5day),\ndev_retained_6day=if(values(dev_retained_6day)>0,values(dev_retained_6day),dev_retained_6day),\ndev_retained_7day=if(values(dev_retained_7day)>0,values(dev_retained_7day),dev_retained_7day),\ndev_retained_8day=if(values(dev_retained_8day)>0,values(dev_retained_8day),dev_retained_8day),\ndev_retained_9day=if(values(dev_retained_9day)>0,values(dev_retained_9day),dev_retained_9day),\ndev_retained_10day=if(values(dev_retained_10day)>0,values(dev_retained_10day),dev_retained_10day),\ndev_retained_11day=if(values(dev_retained_11day)>0,values(dev_retained_11day),dev_retained_11day),\ndev_retained_12day=if(values(dev_retained_12day)>0,values(dev_retained_12day),dev_retained_12day),\ndev_retained_13day=if(values(dev_retained_13day)>0,values(dev_retained_13day),dev_retained_13day),\ndev_retained_14day=if(values(dev_retained_14day)>0,values(dev_retained_14day),dev_retained_14day),\ndev_retained_15day=if(values(dev_retained_15day)>0,values(dev_retained_15day),dev_retained_15day),\ndev_retained_30day=if(values(dev_retained_30day)>0,values(dev_retained_30day),dev_retained_30day)"
      val params = new ArrayBuffer[Array[Any]]()
      for (insertedRow <- rows) {
        if (insertedRow.get(2) != null) {
          val channelArray = StringUtils.getArrayChannel(insertedRow.get(2).toString)
          if (channelArray(0).length <= 10 && channelArray(1).length <= 10 && channelArray(2).length <= 15) {
            params.+=(Array[Any](insertedRow.get(0), insertedRow.get(1), channelArray(0), channelArray(1), channelArray(2), insertedRow.get(3),
              insertedRow.get(4), insertedRow.get(5), insertedRow.get(6), insertedRow.get(7), insertedRow.get(8), insertedRow.get(9), insertedRow.get(10),
              insertedRow.get(11), insertedRow.get(12), insertedRow.get(13), insertedRow.get(14), insertedRow.get(15), insertedRow.get(16), insertedRow.get(17),
              if (insertedRow(18) == null) "" else insertedRow(18), if (insertedRow(19) == null) "" else insertedRow(19), if (insertedRow(20) == null) "" else insertedRow(20),
              if (insertedRow(21) == null) "" else insertedRow(21), if (insertedRow(22) == null) "0" else insertedRow(22), if (insertedRow(23) == null) "1" else insertedRow(23), if (insertedRow(24) == null) "0" else insertedRow(24)
            ))
          }
        } else {
          println("expand_channel is null: " + insertedRow.get(0) + " - " + insertedRow.get(2) + " - " + insertedRow.get(1))
        }
      }
      try {
        JdbcUtil.doBatch(sqlText, params, conn)
      } finally {
        statement.close()
        conn.close
      }
    })
  }

  private def accountRetainedForeachPartition(accountRetainedDf: DataFrame) = {
    accountRetainedDf.foreachPartition(rows => {

      val conn = JdbcUtil.getConn()
      val statement = conn.createStatement

      val sqlText = " insert into bi_gamepublic_actions(publish_date,child_game_id,medium_channel,ad_site_channel,pkg_code,acc_retained_2day,acc_retained_3day,acc_retained_4day,acc_retained_5day,acc_retained_6day,acc_retained_7day,acc_retained_8day,acc_retained_9day,acc_retained_10day,acc_retained_11day,acc_retained_12day,acc_retained_13day,acc_retained_14day,acc_retained_15day,acc_retained_30day,acc_retained_45day,acc_retained_60day,acc_retained_90day,acc_retained_120day,acc_retained_150day,acc_retained_180day,promotion_channel,medium_account,promotion_mode,head_people,parent_game_id,os,group_id)values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)\n on duplicate key update \n acc_retained_2day=if(values(acc_retained_2day)>0,values(acc_retained_2day),acc_retained_2day),\n acc_retained_3day=if(values(acc_retained_3day)>0,values(acc_retained_3day),acc_retained_3day),\n acc_retained_4day=if(values(acc_retained_4day)>0,values(acc_retained_4day),acc_retained_4day),\n acc_retained_5day=if(values(acc_retained_5day)>0,values(acc_retained_5day),acc_retained_5day),\n acc_retained_6day=if(values(acc_retained_6day)>0,values(acc_retained_6day),acc_retained_6day),\n acc_retained_7day=if(values(acc_retained_7day)>0,values(acc_retained_7day),acc_retained_7day),\n acc_retained_8day=if(values(acc_retained_8day)>0,values(acc_retained_8day),acc_retained_8day),\n acc_retained_9day=if(values(acc_retained_9day)>0,values(acc_retained_9day),acc_retained_9day),\n acc_retained_10day=if(values(acc_retained_10day)>0,values(acc_retained_10day),acc_retained_10day),\n acc_retained_11day=if(values(acc_retained_11day)>0,values(acc_retained_11day),acc_retained_11day),\n acc_retained_12day=if(values(acc_retained_12day)>0,values(acc_retained_12day),acc_retained_12day),\n acc_retained_13day=if(values(acc_retained_13day)>0,values(acc_retained_13day),acc_retained_13day),\n acc_retained_14day=if(values(acc_retained_14day)>0,values(acc_retained_14day),acc_retained_14day),\n acc_retained_15day=if(values(acc_retained_15day)>0,values(acc_retained_15day),acc_retained_15day),\n acc_retained_30day=if(values(acc_retained_30day)>0,values(acc_retained_30day),acc_retained_30day),\n acc_retained_45day=if(values(acc_retained_45day)>0,values(acc_retained_45day),acc_retained_45day),\n acc_retained_60day=if(values(acc_retained_60day)>0,values(acc_retained_60day),acc_retained_60day),\n acc_retained_90day=if(values(acc_retained_90day)>0,values(acc_retained_90day),acc_retained_90day),\n acc_retained_120day=if(values(acc_retained_120day)>0,values(acc_retained_120day),acc_retained_120day),\n acc_retained_150day=if(values(acc_retained_150day)>0,values(acc_retained_150day),acc_retained_150day),\n acc_retained_180day=if(values(acc_retained_180day)>0,values(acc_retained_180day),acc_retained_180day)"

      val params = new ArrayBuffer[Array[Any]]()
      for (insertedRow <- rows) {
        if (insertedRow.get(2) != null) {
          val channelArray = StringUtils.getArrayChannel(insertedRow.get(2).toString)
          if (channelArray(0).length <= 10 && channelArray(1).length <= 10 && channelArray(2).length <= 15) {
            params.+=(Array[Any](insertedRow.get(0), insertedRow.get(1), channelArray(0), channelArray(1), channelArray(2),
              insertedRow.get(4), insertedRow.get(5), insertedRow.get(6), insertedRow.get(7), insertedRow.get(8), insertedRow.get(9), insertedRow.get(10), insertedRow.get(11),
              insertedRow.get(12), insertedRow.get(13), insertedRow.get(14), insertedRow.get(15), insertedRow.get(16), insertedRow.get(17), insertedRow.get(18),
              insertedRow.get(19), insertedRow.get(20), insertedRow.get(21), insertedRow.get(22), insertedRow.get(23), insertedRow.get(24),
              if (insertedRow(25) == null) "" else insertedRow(25), if (insertedRow(26) == null) "" else insertedRow(26), if (insertedRow(27) == null) "" else insertedRow(27),
              if (insertedRow(28) == null) "" else insertedRow(28), if (insertedRow(29) == null) "0" else insertedRow(29), if (insertedRow(30) == null) "1" else insertedRow(30),
              if (insertedRow(31) == null) "0" else insertedRow(31)))
          }
        } else {
          println("expand_channel is null: " + insertedRow.get(0) + " - " + insertedRow.get(2) + " - " + insertedRow.get(1))
        }
      }
      try {
        JdbcUtil.doBatch(sqlText, params, conn)
      } finally {
        statement.close()
        conn.close
      }
    })
  }


  private def operaDeviceRetainedForeachPartition(deviceRetainedDf: DataFrame) = {
    deviceRetainedDf.foreachPartition(rows => {
      val conn = JdbcUtil.getConn()
      val statement = conn.createStatement
      val sqlText ="insert into bi_gamepublic_opera_actions(\npublish_date,\nchild_game_id,\nparent_game_id,\nos,\ngroup_id,\nregi_login_dev_num,\ndev_retained_2day,\ndev_retained_3day,\ndev_retained_4day,\ndev_retained_5day,\ndev_retained_6day,\ndev_retained_7day,\ndev_retained_8day,\ndev_retained_9day,\ndev_retained_10day,\ndev_retained_11day,\ndev_retained_12day,\ndev_retained_13day,\ndev_retained_14day,\ndev_retained_15day,\ndev_retained_30day,dev_retained_45day,dev_retained_60day,dev_retained_90day,dev_retained_120day,dev_retained_150day,dev_retained_180day)\nvalues(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)\non duplicate key update \nparent_game_id=VALUES(parent_game_id),\nos=VALUES(os),\ngroup_id=VALUES(group_id),\nregi_login_dev_num=VALUES(regi_login_dev_num),\ndev_retained_2day=if(values(dev_retained_2day)>0,values(dev_retained_2day),dev_retained_2day),\ndev_retained_3day=if(values(dev_retained_3day)>0,values(dev_retained_3day),dev_retained_3day),\ndev_retained_4day=if(values(dev_retained_4day)>0,values(dev_retained_4day),dev_retained_4day),\ndev_retained_5day=if(values(dev_retained_5day)>0,values(dev_retained_5day),dev_retained_5day),\ndev_retained_6day=if(values(dev_retained_6day)>0,values(dev_retained_6day),dev_retained_6day),\ndev_retained_7day=if(values(dev_retained_7day)>0,values(dev_retained_7day),dev_retained_7day),\ndev_retained_8day=if(values(dev_retained_8day)>0,values(dev_retained_8day),dev_retained_8day),\ndev_retained_9day=if(values(dev_retained_9day)>0,values(dev_retained_9day),dev_retained_9day),\ndev_retained_10day=if(values(dev_retained_10day)>0,values(dev_retained_10day),dev_retained_10day),\ndev_retained_11day=if(values(dev_retained_11day)>0,values(dev_retained_11day),dev_retained_11day),\ndev_retained_12day=if(values(dev_retained_12day)>0,values(dev_retained_12day),dev_retained_12day),\ndev_retained_13day=if(values(dev_retained_13day)>0,values(dev_retained_13day),dev_retained_13day),\ndev_retained_14day=if(values(dev_retained_14day)>0,values(dev_retained_14day),dev_retained_14day),\ndev_retained_15day=if(values(dev_retained_15day)>0,values(dev_retained_15day),dev_retained_15day),\ndev_retained_30day=if(values(dev_retained_30day)>0,values(dev_retained_30day),dev_retained_30day),\ndev_retained_45day=if(values(dev_retained_45day)>0,values(dev_retained_45day),dev_retained_45day),\ndev_retained_60day=if(values(dev_retained_60day)>0,values(dev_retained_60day),dev_retained_60day),\ndev_retained_90day=if(values(dev_retained_90day)>0,values(dev_retained_90day),dev_retained_90day),\ndev_retained_120day=if(values(dev_retained_120day)>0,values(dev_retained_120day),dev_retained_120day),\ndev_retained_150day=if(values(dev_retained_150day)>0,values(dev_retained_150day),dev_retained_150day),\ndev_retained_180day=if(values(dev_retained_180day)>0,values(dev_retained_180day),dev_retained_180day)"
      val params = new ArrayBuffer[Array[Any]]()
      for (insertedRow <- rows) {
        params.+=(Array[Any](insertedRow.get(0), insertedRow.get(1), insertedRow.get(2),
          insertedRow.get(3), insertedRow.get(4), insertedRow.get(5), insertedRow.get(6),
          insertedRow.get(7), insertedRow.get(8), insertedRow.get(9), insertedRow(10),
          insertedRow(11), insertedRow(12), insertedRow(13), insertedRow(14), insertedRow(15),
          insertedRow(16), insertedRow(17), insertedRow(18), insertedRow(19),
          insertedRow(20), insertedRow(21), insertedRow(22), insertedRow(23),
          insertedRow(24), insertedRow(25), insertedRow(26)
        ))
      }
      try {
        JdbcUtil.doBatch(sqlText, params, conn)
      } finally {
        statement.close()
        conn.close
      }
    })
  }

  def operDevaccForeachPartition(accountRetainedDf: DataFrame) = {
    accountRetainedDf.foreachPartition(rows => {

      val conn = JdbcUtil.getConn()
      val statement = conn.createStatement

      val sqlText = " insert into bi_gamepublic_opera_actions(publish_date,parent_game_id,child_game_id,os,group_id,regi_login_num) values(?,?,?,?,?,?) on duplicate key update regi_login_num=values(regi_login_num)"
      val params = new ArrayBuffer[Array[Any]]()
      for (insertedRow <- rows) {
        params.+=(Array[Any](insertedRow.get(0), insertedRow.get(1), insertedRow.get(2), insertedRow.get(3), insertedRow.get(4), insertedRow.get(5)))
      }
      try {
        JdbcUtil.doBatch(sqlText, params, conn)
      } finally {
        statement.close()
        conn.close
      }
    })
  }
}

