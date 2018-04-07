package com.xiaopeng.bi.utils


import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by bigdata on 17-8-8.
  */
object GameBinduidUtil {


  def loadBinduidInfo(rdd: RDD[String], hiveContext: HiveContext) = {
    val binduidRdd = rdd.filter(line => {
      val fields = line.split("\\|",-1)
      //排除截断日志   只取存在订单号的数据，不存在订单号的为代金券消费 只取状态为4的数据
      fields(0).contains("bi_binduid") && fields.length >= 6
    }).map(line => {
      val fields = line.split("\\|",-1)
      //game_account(1),member_id(2),status_code(6)
      Row(fields(1).trim.toLowerCase, fields(2), fields(6))
    })

    if (!binduidRdd.isEmpty) {

      val binduidStruct = new StructType()
        .add("game_account", StringType)
        .add("member_id", StringType)
        .add("status_code", StringType)
      val bi_binduid = hiveContext.createDataFrame(binduidRdd, binduidStruct)
      bi_binduid.registerTempTable("ods_binduid")

      //帐号 绑定 会员id
      val sql_bi_binduid = "select distinct game_account,member_id from ods_binduid where status_code = '1'"
      val binduidDataFrame = hiveContext.sql(sql_bi_binduid)

      foreachBinduidDataFrame(binduidDataFrame)

      //帐号 解绑 会员id
      val sql_bi_unbinduid = "select distinct game_account,member_id from ods_binduid where status_code = '0'"
      val unBinduidDataFrame = hiveContext.sql(sql_bi_unbinduid)

      foreachUnBinduidDataFrame(unBinduidDataFrame)
    }

  }

  def foreachBinduidDataFrame(binduidDataFrame: DataFrame) = {
    binduidDataFrame.foreachPartition(iter => {
      if (!iter.isEmpty) {
        val conn = JdbcUtil.getConn()

        val member_game_account_num = "update bi_member_info set accounts = accounts + 1 where member_id = ?"
        val pstmt_member_game_account_num = conn.prepareStatement(member_game_account_num)
        val member_game_account_num_params = ArrayBuffer[Array[Any]]()

        iter.foreach(row => {
          //取出dataframe中的数据

          val member_id = row.getAs[String]("member_id")

          member_game_account_num_params.+=(Array(member_id))

          JdbcUtil.executeUpdate(pstmt_member_game_account_num, member_game_account_num_params, conn)

        })

        pstmt_member_game_account_num.close
        conn.close
      }
    })
  }

  def foreachUnBinduidDataFrame(unBinduidDataFrame: DataFrame) = {
    unBinduidDataFrame.foreachPartition(iter => {
      if (!iter.isEmpty) {
        val conn = JdbcUtil.getConn()
        val member_game_account_num = "update bi_member_info set accounts = accounts - 1 where member_id = ?"
        val pstmt_member_game_account_num = conn.prepareStatement(member_game_account_num)
        val member_game_account_num_params = ArrayBuffer[Array[Any]]()
        iter.foreach(row => {
          //取出dataframe中的数据

          val member_id = row.getAs[String]("member_id")

          member_game_account_num_params.+=(Array(member_id))

          JdbcUtil.executeUpdate(pstmt_member_game_account_num, member_game_account_num_params, conn)
        })

        pstmt_member_game_account_num.close
        conn.close

      }
    })
  }
}
