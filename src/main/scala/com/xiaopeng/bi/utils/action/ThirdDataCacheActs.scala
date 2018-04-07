package com.xiaopeng.bi.utils.action

import java.text.SimpleDateFormat
import java.util.Date

import com.xiaopeng.bi.utils.JdbcUtil
import net.sf.json.JSONObject
import org.apache.spark.streaming.dstream.DStream

import scala.collection.mutable.ArrayBuffer

/**
  * Created by bigdata on 17-12-12.
  */
object ThirdDataCacheActs {
  /**
    * 广告监测联调点击明细数据存储
    *
    * @param thirdData
    */
  def cacheClickData(thirdData: DStream[String]) = {
    val adData = thirdData.map(line => {
      //      println("原始 : " +line)
      val simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      val jsStr = JSONObject.fromObject(line)
      if (line.contains("bi_adv_momo_click")) {
        try {
          (jsStr.get("pkg_id").toString,
            jsStr.get("imei").toString,
            if (jsStr.get("ts").toString.length == 10) simpleDateFormat.format(new Date(jsStr.get("ts").toString.toLong * 1000)) else simpleDateFormat.format(new Date(jsStr.get("ts").toString.toLong)),
            jsStr.get("callback").toString, 1)
        } catch {
          case e: Exception => e.printStackTrace;
            ("", "", "0", "", 1)
        }

      } else if (line.contains("bi_adv_baidu_click")) {
        try {
          (jsStr.get("pkg_id").toString,
            jsStr.get("imei").toString,
            if (jsStr.get("ts").toString.length == 10) simpleDateFormat.format(new Date(jsStr.get("ts").toString.toLong * 1000)) else simpleDateFormat.format(new Date(jsStr.get("ts").toString.toLong)),
            jsStr.get("callback_url").toString, 2)
        } catch {
          case e: Exception => e.printStackTrace;
            ("", "", "0", "", 2)
        }

      } else if (line.contains("bi_adv_jinretoutiao_click")) {
        try {
          (jsStr.get("pkg_id").toString,
            jsStr.get("imei").toString,
            if (jsStr.get("timestamp").toString.length == 10) simpleDateFormat.format(new Date(jsStr.get("timestamp").toString.toLong * 1000)) else simpleDateFormat.format(new Date(jsStr.get("timestamp").toString.toLong)),
            jsStr.get("callback").toString, 3)
        } catch {
          case e: Exception => e.printStackTrace;
            ("", "", "0", "", 3)
        }

      } else if (line.contains("bi_adv_aiqiyi_click")) {
        try {
          (jsStr.get("pkg_id").toString,
            jsStr.get("udid").toString,
            if (jsStr.get("ts").toString.length == 10) simpleDateFormat.format(new Date(jsStr.get("ts").toString.toLong * 1000)) else simpleDateFormat.format(new Date(jsStr.get("ts").toString.toLong)),
            "", 4)
        } catch {
          case e: Exception => e.printStackTrace;
            ("", "", "0", "", 4)
        }

      } else if (line.contains("bi_adv_uc_click")) {
        try {
          (jsStr.get("pkg_id").toString,
            jsStr.get("imei").toString,
            if (jsStr.get("time").toString.length == 10) simpleDateFormat.format(new Date(jsStr.get("time").toString.toLong * 1000)) else simpleDateFormat.format(new Date(jsStr.get("time").toString.toLong)),
            jsStr.get("callback").toString, 5)
        } catch {
          case e: Exception => e.printStackTrace;
            ("", "", "0", "", 5)
        }

      } else if (line.contains("bi_adv_guangdiantong_click")) {
        try {
          (jsStr.get("pkg_id").toString,
            jsStr.get("muid").toString,
            if (jsStr.get("click_time").toString.length == 10) simpleDateFormat.format(new Date(jsStr.get("click_time").toString.toLong * 1000)) else simpleDateFormat.format(new Date(jsStr.get("click_time").toString.toLong)),
            jsStr.get("callback").toString, 6)
        } catch {
          case e: Exception => e.printStackTrace;
            ("", "", "0", "", 6)
        }

      } else if (line.contains("bi_adv_medium_click")) {
        //取出媒介编号adv_id
        val adv_id = jsStr.get("adv_id").toString.toInt
        //根据媒介编号去判断是哪家媒介
        if (adv_id == 7) {
          try {
            (jsStr.get("pkg_id").toString,
              jsStr.get("imei").toString,
              if (jsStr.get("time").toString.length == 10) simpleDateFormat.format(new Date(jsStr.get("time").toString.toLong * 1000)) else simpleDateFormat.format(new Date(jsStr.get("time").toString.toLong)),
              jsStr.get("callback").toString, 7)
          } catch {
            case e: Exception => e.printStackTrace;
              ("", "", "0", "", 7)
          }

        } else if (adv_id == 8) {
          try {
            (jsStr.get("pkg_id").toString,
              jsStr.get("imei").toString,
              if (jsStr.get("ts").toString.length == 10) simpleDateFormat.format(new Date(jsStr.get("ts").toString.toLong * 1000)) else simpleDateFormat.format(new Date(jsStr.get("ts").toString.toLong)),
              jsStr.get("callback").toString, 8)
          } catch {
            case e: Exception => e.printStackTrace;
              ("", "", "0", "", 8)
          }

        } else if (adv_id == 9) {
          try {
            (jsStr.get("pkg_id").toString,
              jsStr.get("imei").toString,
              if (jsStr.get("ts").toString.length == 10) simpleDateFormat.format(new Date(jsStr.get("ts").toString.toLong * 1000)) else simpleDateFormat.format(new Date(jsStr.get("ts").toString.toLong)),
              jsStr.get("callback").toString, 9)
          } catch {
            case e: Exception => e.printStackTrace;
              ("", "", "0", "", 9)
          }

        } else if (adv_id == 17) {
          try {
            (jsStr.get("pkg_id").toString,
              if (jsStr.get("os").toString.equals("android")) jsStr.get("imei").toString else if (jsStr.get("os").toString.equals("ios")) jsStr.get("idfa").toString else "",
              if (jsStr.get("qt").toString.length == 10) simpleDateFormat.format(new Date(jsStr.get("qt").toString.toLong * 1000)) else simpleDateFormat.format(new Date(jsStr.get("qt").toString.toLong)),
              jsStr.get("callback").toString, 17)
          } catch {
            case e: Exception => e.printStackTrace;
              ("", "", "0", "", 17)
          }

        } else if (adv_id == 18) {
          try {
            (jsStr.get("pkg_id").toString,
              jsStr.get("devid").toString,
              if (jsStr.get("ts").toString.length == 10) simpleDateFormat.format(new Date(jsStr.get("ts").toString.toLong * 1000)) else simpleDateFormat.format(new Date(jsStr.get("ts").toString.toLong)),
              jsStr.get("callback").toString, 18)
          } catch {
            case e: Exception => e.printStackTrace
              ("", "", "0", "", 18)
          }
        } else if (adv_id == -1) {
          try {
            (jsStr.get("pkg_id").toString,
              jsStr.get("imei").toString,
              if (jsStr.get("ts").toString.length == 10) simpleDateFormat.format(new Date(jsStr.get("ts").toString.toLong * 1000)) else simpleDateFormat.format(new Date(jsStr.get("ts").toString.toLong)),
              "", -1)
          } catch {
            case e: Exception => e.printStackTrace;
              ("", "", "0", "", -1)
          }

        } else {
          ("", "", "0", "", 0)
        }

      } else {
        ("", "", "0", "", 0)
      }
    })
    cacheThirdData(adData)

  }

  def cacheThirdData(adData: DStream[(String, String, String, String, Int)]) = {
    adData.foreachRDD(rdd => {
      rdd.foreachPartition(iter => {
        val conn = JdbcUtil.getConn()
        val connFX = JdbcUtil.getXiaopeng2FXConn()
        val statFX = connFX.createStatement()
        iter.foreach(line => {
          //          println("元组 ： " +line)
          val pkg_id = line._1
          val imei = line._2
          val click_time = line._3
          val callback = line._4
          val adv_id = line._5
          //目前需要联调的媒介：今日头条，广点通，UC，陌陌，百度信息流,新浪扶翼
          val arr = ArrayBuffer[Int](1, 2, 3, 5, 6, 18)
          val res = arr.contains(adv_id)
          if (res) {
            if (!click_time.equals("0") && pkg_id.matches("\\d{4,5}[MQ]\\d{4,7}[a-z]?")) {
              //媒介标识
              var medium_tag = ""
              //用户ID
              var user_id = 0
              //媒介名称
              var medium_name = ""

              val select_medium_package = "select user_id,medium_tag from medium_package where subpackage_id = '" + pkg_id + "'"
              val resultSet = statFX.executeQuery(select_medium_package)
              while (resultSet.next()) {
                medium_tag = resultSet.getString("medium_tag")
                user_id = resultSet.getInt("user_id")
              }


              val select_medium = "select medium_name from medium where medium_tag = '" + medium_tag + "' limit 1"
              val resultSetMedium = statFX.executeQuery(select_medium)
              while (resultSetMedium.next()) {
                medium_name = resultSetMedium.getString("medium_name")
              }


              val insert = "insert into bi_ad_click_detail (pkg_id,medium_tag,medium_name,imei,user_id,click_time,callback) values(?,?,?,?,?,?,?)"
              val params_buffer = new ArrayBuffer[Array[Any]]()
              params_buffer.+=(Array[Any](pkg_id, medium_tag, medium_name, imei, user_id, click_time, callback))

              JdbcUtil.doBatch(insert, params_buffer, conn);

              resultSet.close()
              resultSetMedium.close()
            }
          }
        })
        statFX.close()
        connFX.close()
        conn.close()
      })
    })
  }

}
