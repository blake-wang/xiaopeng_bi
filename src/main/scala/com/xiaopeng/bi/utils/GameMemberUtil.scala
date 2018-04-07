package com.xiaopeng.bi.utils


import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by bigdata on 17-8-7.
  * 处理member基础信息  member_id,手机号，注册时间，来源
  */
object GameMemberUtil {


  //处理member日志信息
  def loadMemberInfo(rdd: RDD[String], hiveContext: HiveContext) {
    val memberRdd = rdd.filter(line => {
      val fields = line.split("\\|", -1)
      //过滤被截断的日志，手机号为空的日志 ,新注册的member，change_code = 1
      fields(0).contains("bi_member") && (!fields(2).equals("")) && fields.length >= 14 && (!fields(1).equals(""))
    }).map(line => {
      val fields = line.split("\\|", -1)
      //member_id(1),username(2),regi_origin(8),change_time(14),change_code(15)    //共16
      var regi_origin = ""
      if (fields(8).equals("")) {
        regi_origin = "0"
      } else {
        regi_origin = fields(8)
      }

      var change_code = ""
      if (fields(15).equals("")) {
        change_code = "0"
      } else {
        change_code = fields(15)
      }

      Row(fields(1).toInt, fields(2), regi_origin, fields(14), change_code)
    })


    if (!memberRdd.isEmpty()) {
      val memberStruct = new StructType()
        .add("member_id", IntegerType)
        .add("username", StringType)
        .add("regi_origin", StringType)
        .add("change_time", StringType)
        .add("change_code", StringType)
      val memberDataFrame = hiveContext.createDataFrame(memberRdd, memberStruct)
      memberDataFrame.registerTempTable("ods_member")

      val bi_member = "select distinct member_id,username,regi_origin,change_time from ods_member where change_code = '1'"
      val df_bi_member = hiveContext.sql(bi_member)

      //加载member数据到bi_member_info表中
      foreachPartitionMember(df_bi_member)
    }

  }

  def foreachPartitionMember(df_bi_member: DataFrame) = {
    df_bi_member.foreachPartition(iter => {
      if (!iter.isEmpty) {
        val conn = JdbcUtil.getConn()
        //插入数据到bi_member_info
        //        val sql_member_info = "insert into bi_member_info (member_id,username,regtype,addtime) values (?,?,?,?)"
        val sql_member_info = "insert into bi_member_info (member_id,username,regtype,addtime) values (?,?,?,?) on duplicate key update username = values(username),regtype = values(regtype),addtime = values(addtime)"
        val pstmt_sql_member_info = conn.prepareStatement(sql_member_info)
        val sql_member_info_params = ArrayBuffer[Array[Any]]()

        iter.foreach(row => {

          //获取 dataframe中的数据
          val member_id = row.getAs[Int]("member_id")
          val username = row.getAs[String]("username")
          val regi_origin = row.getAs[String]("regi_origin")
          val change_time = row.getAs[String]("change_time")


          sql_member_info_params.+=(Array[Any](member_id, username, regi_origin.toInt, change_time))
          JdbcUtil.executeUpdate(pstmt_sql_member_info, sql_member_info_params, conn)

        })
        pstmt_sql_member_info.close()
        conn.close()

      }
    })
  }


}
