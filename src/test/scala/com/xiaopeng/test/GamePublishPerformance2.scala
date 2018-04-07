package com.xiaopeng.test

import com.xiaopeng.bi.utils.{ConfigurationUtil, FileUtil, SparkUtils}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

/**
  * ltv: 用户价值分析，注册和支付
  */
object GamePublishPerformance2 {

  def main(args: Array[String]): Unit = {
    // 创建各种上下文
    val sparkConf = new SparkConf().setAppName(this.getClass.getName.replace("$", ""))
      .set("spark.sql.shuffle.partitions", ConfigurationUtil.getProperty("spark.sql.shuffle.partitions"))
    SparkUtils.setMaster(sparkConf);
    val sc = new SparkContext(sparkConf)
    val hiveContext = new HiveContext(sc)
    hiveContext.sql("use yyft")
    SparkUtils.readBiTable("bi_publish_back_performance", hiveContext)

    val sql = "select\nsum(if(pay_type!=5,bp.pay_water,0)) pyw_recharge,\nsum(if(pay_type=5,bp.pay_water,0)) apple_true_recharge,\nsum(regi_num) reg_quantity,\nbp.child_game_id game_sub_id,\nbp.performance_time cost_date\nfrom \n(select performance_time,child_game_id,pay_water,pay_type,regi_num from  bi_publish_back_performance where to_date(performance_time)>='2017-07-27' and to_date(performance_time)<='2017-07-27') bp\njoin  (select  distinct old_game_id  from game_sdk  where state=0 and system_type=2) gs on bp.child_game_id=gs.old_game_id\ngroup by bp.performance_time,bp.child_game_id"
    val df = hiveContext.sql(sql)
    //二.把结果存入mysql
    df.foreachPartition(rows => {

      val params = new ArrayBuffer[Array[Any]]()
      for (insertedRow <- rows) {
        val sqlText = "update fx_publish_result_base set pyw_recharge='" + insertedRow.get(0) + "',apple_true_recharge='" + insertedRow.get(1) + "',reg_quantity='" + insertedRow.get(2) + "' where  game_sub_id='" + insertedRow.get(3) + "' and cost_date='" + insertedRow.get(4) + "'"
        FileUtil.apppendTofile("/home/hduser/projs/logs/ggggg.txt", sqlText + ";")
      }
    })

    val sql2 = "select\nsum(if(pay_type!=5,bp.pay_water,0)) pyw_recharge,\nsum(regi_num) reg_quantity,\nbp.child_game_id game_sub_id,\nbp.performance_time cost_date,\nbp.pkg_code pkg_id\nfrom \n(select performance_time,child_game_id,pkg_code,pay_water,pay_type,regi_num from  bi_publish_back_performance where to_date(performance_time)>='2017-07-27' and to_date(performance_time)<='2017-07-27') bp\njoin  (select  distinct old_game_id from game_sdk  where state=0 and system_type!=2) gs on bp.child_game_id=gs.old_game_id\ngroup by bp.performance_time,bp.child_game_id,bp.pkg_code"
    val df2 = hiveContext.sql(sql2)
    //二.把结果存入mysql
    df2.foreachPartition(rows => {

      val params = new ArrayBuffer[Array[Any]]()
      for (insertedRow <- rows) {
        val sqlText = "update fx_publish_result_base set pyw_recharge='" + insertedRow.get(0) + "',reg_quantity='" + insertedRow.get(1) + "'where game_sub_id='" + insertedRow.get(2) + "'  and  cost_date='" + insertedRow.get(3) + "' and pkg_id='" + insertedRow.get(4) + "'"
        FileUtil.apppendTofile("/home/hduser/projs/logs/ggggg2.txt", sqlText + ";")
      }
    })
    sc.stop()
  }
}