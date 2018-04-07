package com.xiaopeng.bi.utils

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import org.apache.spark.sql.{DataFrame, Row}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by bigdata on 17-8-7.
  */
object GameMemberWithRegiUtil {


  def loadRegiInfo(rdd: RDD[String], hiveContext: HiveContext): Unit = {
    val regiRdd = rdd.filter(line => {
      val fields = line.split("\\|", -1)
      //过滤被截断日志,game_account 不能为空
      fields(0).contains("bi_regi") && (!fields(3).equals("")) && fields.length >= 12
    }).map(line => {

      val fields = line.split("\\|", -1) //共16字段
      var imei = ""
      if (fields.length >= 15) {
        imei = fields(14)
      } else {
        imei = ""
      }
      //username,  game_account  game_id  reg_time  reg_origin  member_id  imei
      Row(fields(9), fields(3).trim.toLowerCase, fields(4).toInt, fields(5), fields(6), fields(9), imei)
    })
    if (!regiRdd.isEmpty) {
      val regiStruct = new StructType()
        .add("username", StringType)
        .add("game_account", StringType)
        .add("game_id", IntegerType)
        .add("reg_time", StringType)
        .add("reg_origin", StringType)
        .add("member_id", StringType)
        .add("imei", StringType)
      val regiDataFrame = hiveContext.createDataFrame(regiRdd, regiStruct)
      regiDataFrame.registerTempTable("ods_regi")

      //取的是 member_id 不为 '' 的数据
      val sql_bi_regi = "select distinct username,game_account,game_id,reg_time,member_id,imei from ods_regi where username != '' and reg_origin = '2' and member_id != ''"
      val memberRegiAccountDataFrame: DataFrame = hiveContext.sql(sql_bi_regi)


      foreachRegiDataFrame(memberRegiAccountDataFrame)


    }

  }

  def foreachRegiDataFrame(memberRegiAccountDataFrame: DataFrame) = {
    memberRegiAccountDataFrame.foreachPartition(iter => {

      if (!iter.isEmpty) {

        val conn = JdbcUtil.getConn()
        //更新 帐号数
        val member_regi_account_num = "update bi_member_info set accounts = accounts + 1 where member_id = ?"
        val pstmt_member_regi_account_num = conn.prepareStatement(member_regi_account_num)
        var member_regi_account_num_params = ArrayBuffer[Array[Any]]()

        iter.foreach(row => {
          //取出dataframe中的数据
          val member_id = row.getAs[String]("member_id")

          member_regi_account_num_params.+=(Array[Any](member_id))

          JdbcUtil.executeUpdate(pstmt_member_regi_account_num, member_regi_account_num_params, conn)

        })

        pstmt_member_regi_account_num.close()
        conn.close()
      }
    })

  }


}
