package com.xiaopeng.bi.gamepublish


import java.sql.{Connection, PreparedStatement}

import com.xiaopeng.bi.udf.ChannelUDF
import com.xiaopeng.bi.utils.{JdbcUtil, SparkUtils}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by bigdata on 17-8-30.
  * 8月24日新需求    投放成本
  */
object NewPubBackPublishCost {
  var startDay = "";
  var endDay = ""

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    if (args.length == 2) {
      startDay = args(0)
      endDay = args(1)
    } else if (args.length == 1) {
      startDay = args(0)
      endDay = startDay
    }

    // 创建上下文
    val sparkConf = new SparkConf().setAppName(this.getClass.getName.replace("$", ""))
      .set("spark.default.parallelism", "60")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.shuffle.consolidateFiles", "true")
      .set("spark.storage.memoryFraction", "0.4")
      .set("spark.driver.allowMultipleContexts", "true") //.setMaster("local[*]")
    SparkUtils.setMaster(sparkConf)

    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)

    sqlContext.sql("use yyft")
    SparkUtils.readXiaopeng2FxCostAdvTable(sc, sqlContext, startDay, endDay)
    SparkUtils.readXiaopeng2FxTable("fx_cost_advertisement_audit", sqlContext)
    SparkUtils.readXiaopeng2FxCostAgentTable(sc, sqlContext, startDay, endDay)
    SparkUtils.readXiaopeng2FxTable("fx_cost_agent_audit", sqlContext)
    SparkUtils.readXiaopeng2FxTable("game_sdk", sqlContext)

    //---------- 投放成本  到包的维度
    sqlContext.udf.register("getchannel", new ChannelUDF(), DataTypes.StringType)
    val publishCostSqloper = "select\ncost.cost_date publish_date,\ngs.game_id parent_game_id,\ncost.game_sub_id child_game_id,\ncost.medium_channel medium_channel,\ncost.ad_site_channel ad_site_channel,\ncost.pkg_code pkg_code,\nsum(cost.publish_cost) publish_cost,\nmax(cost.update_time) cost_update_time,\ngs.group_id group_id,\ngs.system_type os\nfrom\n(\n\tselect \n\tct.cost_date,\n\tct.game_sub_id,\n\tgetchannel(ct.spid,'0')  medium_channel,\n\tgetchannel(ct.spid,'1')  ad_site_channel,\n\tgetchannel(ct.spid,'2')  pkg_code,\n\tmax(ct.update_time) update_time,\n\tsum(ct.actual_cost) publish_cost\n\tfrom \n\t(select distinct ct.game_sub_id,ct.spid,ct.cost_date,ct.update_time,ct.actual_cost,ct.create_time from \n\t\t(\n\t\tselect distinct fca.game_sub_id game_sub_id,fca.spid spid,fca.cost_date cost_date,if(fcaa.audit_time is  null,if(fca.update_time is null,'0000-00-00 00:00:00',fca.update_time) ,fcaa.audit_time) update_time,fca.actual_cost actual_cost,fca.create_time create_time,fca.status status  from \n(select  distinct id,game_sub_id,spid,cost_date,if(update_time is null,create_time,update_time) update_time,actual_cost,create_time,status from  fx_cost_agent )fca\nleft join (select  cost_id,max(audit_time) audit_time from fx_cost_agent_audit group by cost_id) fcaa on fcaa.cost_id=fca.id\n\t\tUNION all\n\t\tselect distinct fca.game_sub_id game_sub_id,fca.spid spid,fca.cost_date cost_date,if(fcaa.audit_time is  null,if(fca.update_time is null,'0000-00-00 00:00:00',fca.update_time),fcaa.audit_time) update_time,fca.actual_cost actual_cost,fca.create_time create_time,fca.status status  from \n(select distinct id,game_sub_id,spid,cost_date,if(update_time is null,create_time,update_time) update_time,actual_cost,create_time,status from  fx_cost_advertisement where status=1)fca\nleft join (select  cost_id,max(audit_time) audit_time from  fx_cost_advertisement_audit where audit_status=1 group by cost_id )fcaa on fca.id=fcaa.cost_id\n\t\t)ct  where (to_date(ct.update_time)>='startDay' and to_date(ct.update_time)<='endDay') or  (to_date(ct.create_time)>='startDay' and to_date(ct.create_time)<='endDay') or  (to_date(ct.cost_date)>='startDay' and to_date(ct.cost_date)<='endDay') and ct.status=1\n\t) ct \n\tgroup by  ct.game_sub_id,ct.spid,ct.cost_date\n)\ncost\njoin  (select  distinct game_id, old_game_id ,system_type,group_id from game_sdk  where state=0) gs on cost.game_sub_id=gs.old_game_id\ngroup by gs.game_id, cost.cost_date,cost.game_sub_id,cost.medium_channel,cost.ad_site_channel,cost.pkg_code,gs.group_id,gs.system_type"
      .replace("startDay", startDay).replace("endDay", endDay)
    sqlContext.sql(publishCostSqloper).registerTempTable("rz_cost")

    sqlContext.read.parquet("/tmp/hive/fxdim.parquet").registerTempTable("tb_tmp")

    val publishCostSql = "select \ncost.publish_date,\ncost.parent_game_id,\ncost.child_game_id,\ncost.medium_channel,\ncost.ad_site_channel,\ncost.pkg_code,\ncost.publish_cost,\ncost.cost_update_time,\ncost.group_id,\ncost.os,\nif(tmp.medium_account is null,'',tmp.medium_account),\nif(tmp.promotion_channel is null,'',tmp.promotion_channel),\nif(tmp.promotion_mode is null,'',tmp.promotion_mode),\nif(tmp.head_people is null,'',tmp.head_people)\nfrom \n(select publish_date,parent_game_id,child_game_id,medium_channel,ad_site_channel,pkg_code,publish_cost,cost_update_time,group_id,os from rz_cost)  cost \nleft join (select distinct date(mstart_date) mstart_date,date(mend_date) mend_date,medium_account,promotion_channel,promotion_mode,head_people,pkg_code from tb_tmp) tmp \non  cost.publish_date >=tmp.mstart_date and   cost.publish_date<=tmp.mend_date and cost.pkg_code=tmp.pkg_code"
    val publishCostDataFrame = sqlContext.sql(publishCostSql)
    publishCostForeachPartition(publishCostDataFrame)
    sc.stop()
  }

  def publishCostForeachPartition(publishCostDataFrame: DataFrame) = {
    publishCostDataFrame.foreachPartition(iter => {
      val conn: Connection = JdbcUtil.getConn();

      val sqlText = "INSERT INTO bi_gamepublic_actions(publish_date,parent_game_id,child_game_id,medium_channel,ad_site_channel,pkg_code,publish_cost,cost_update_time,group_id,os,medium_account,promotion_channel,promotion_mode,head_people) \nVALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?) \nON DUPLICATE KEY update \npublish_cost=values(publish_cost),\ncost_update_time=values(cost_update_time),\ngroup_id=values(group_id),\nos=values(os),medium_account=values(medium_account),promotion_channel=values(promotion_channel),promotion_mode=values(promotion_mode),head_people=values(head_people)"
      val ps: PreparedStatement = conn.prepareStatement(sqlText);
      val params = new ArrayBuffer[Array[Any]]()

      val sqlText_kpi = "INSERT INTO bi_gamepublic_base_day_kpi(publish_date,parent_game_id,child_game_id,medium_channel,ad_site_channel,pkg_code,group_id,os,medium_account,promotion_channel,promotion_mode,head_people) \nVALUES (?,?,?,?,?,?,?,?,?,?,?,?) \nON DUPLICATE KEY update \ngroup_id=values(group_id),\nos=values(os),medium_account=values(medium_account),promotion_channel=values(promotion_channel),promotion_mode=values(promotion_mode),head_people=values(head_people)"
      val ps_kpi: PreparedStatement = conn.prepareStatement(sqlText_kpi);
      val params_kpi = new ArrayBuffer[Array[Any]]()

      for (row <- iter) {
        var cost_update_time = row.get(7).toString;
        if (row.get(7) == null) {
          cost_update_time = "0000-00-00 00:00:00";
        }
        params.+=(Array[Any](row.get(0), row.get(1), row.get(2), row.get(3), row.get(4), row.get(5), row.get(6).toString.toInt, cost_update_time, row.get(8), row.get(9), row.get(10), row.get(11), row.get(12), row.get(13)))
        params_kpi.+=(Array[Any](row.get(0), row.get(1), row.get(2), row.get(3), row.get(4), row.get(5), row.get(8), row.get(9), row.get(10), row.get(11), row.get(12), row.get(13)))
      }

      JdbcUtil.executeUpdate(ps, params, conn)
      JdbcUtil.executeUpdate(ps_kpi, params_kpi, conn)
      ps.close()
      ps_kpi.close()
      conn.close()
    })
  }


}