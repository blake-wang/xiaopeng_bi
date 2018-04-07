package com.xiaopeng.test

import java.sql

import com.xiaopeng.bi.utils.JdbcUtil

/**
  * Created by bigdata on 17-10-11.
  */
object TestSqlSelect {
  def main(args: Array[String]): Unit = {
    val conn = JdbcUtil.getConn()
    var ps: sql.PreparedStatement = null
    val instSql = "select pkg_id,regi_time,adv_name,os,idea_id,first_level,second_level from bi_ad_regi_o_detail where game_account=? limit 1"
    ps = conn.prepareStatement(instSql)
    ps.setString(1, "pyw123456")
    val rs = ps.executeQuery()
    while (rs.next()) {
      val pkg_id = rs.getString("pkg_id")
      println("pkg_id : "+pkg_id)
      if (rs.getString("pkg_id") != null) {
          println("pkg_id :  不是null")
      }else{
        println("pkg_id :  是null")
      }

    }

  }


}
