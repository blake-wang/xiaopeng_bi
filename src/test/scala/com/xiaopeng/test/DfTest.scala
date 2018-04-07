package com.xiaopeng.test

import com.xiaopeng.bi.utils.{FileUtil, MD5Util, SparkUtils}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by kequan on 3/28/17.
  */
object DfTest {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName(this.getClass.getName.replace("$", ""));
    SparkUtils.setMaster(sparkConf);
    val sc = new SparkContext(sparkConf);
    val hiveContext = new HiveContext(sc);

    hiveContext.sql("use yyft");

    FileUtil.apppendTofile("regi_imei_no_pay.csv", "帐号,注册日期,子游戏,系统,设备号,设备号MD5加密,充值金额")
    FileUtil.apppendTofile("regi_imei_pay.csv", "帐号,注册日期,子游戏,系统,设备号,设备号MD5加密,充值金额")
    FileUtil.apppendTofile("regi_imie_error.csv", "帐号,注册日期,子游戏,系统,设备号,充值金额")

    val rdd2 = sc.textFile("file:///home/kequan/workspace/xiaopeng_bi/src/test/scala/com/xiaopeng/test/regi_order_all.txt")


    rdd2.foreachPartition(it => {
      it.foreach(t3 => {
        val t2 = t3.replace("[", "").replace("]", "").split(",", -1);
        if (t2.length == 6 && ((t2(4) != null) && (!t2(4).equals("null"))) && (!t2(4).equals("00000000000000000000000000000000"))) {
          val game_account = t2(0)
          val regi_date = t2(1)
          val game_id = t2(2)
          val os = t2(3)
          val imei = t2(4)
          var pay_money = "0"
          if ((!t2(5).equals("null")) && (t2(5) != null)) {
            pay_money = t2(5)
          }

          if (imei.contains("&") && imei.split("&", -1)(0).length >= 15) {
            if (pay_money.equals("0")) {
              val rz = game_account + "," + regi_date + "," + game_id + "," + os + "," + imei + "," + MD5Util.string2MD5(imei.split("&", -1)(0).substring(0, 15)) + "," + pay_money
              FileUtil.apppendTofile("regi_imei_no_pay.csv", rz)
            } else {
              val rz = game_account + "," + regi_date + "," + game_id + "," + os + "," + imei + "," + MD5Util.string2MD5(imei.split("&", -1)(0).substring(0, 15)) + "," + pay_money
              FileUtil.apppendTofile("regi_imei_pay.csv", rz)
            }

          } else if ((!imei.contains("&")) && imei.length == 32) {
            val s11 = imei.substring(0, 32);
            val ss1 = s11.substring(0, 8);
            val ss2 = s11.substring(8, 12);
            val ss3 = s11.substring(12, 16);
            val ss4 = s11.substring(16, 20);
            val ss5 = s11.substring(20, s11.length());
            val ss = ss1 + "-" + ss2 + "-" + ss3 + "-" + ss4 + "-" + ss5;

            if (pay_money.equals("0")) {
              val rz = game_account + "," + regi_date + "," + game_id + "," + os + "," + imei + "," + MD5Util.string2MD5(ss) + "," + pay_money
              FileUtil.apppendTofile("regi_imei_no_pay.csv", rz)
            } else {
              val rz = game_account + "," + regi_date + "," + game_id + "," + os + "," + imei + "," + MD5Util.string2MD5(ss) + "," + pay_money
              FileUtil.apppendTofile("regi_imei_pay.csv", rz)
            }

          } else {

            FileUtil.apppendTofile("regi_imie_error.csv", game_account + "," + regi_date + "," + game_id + "," + os + "," + imei + "," + pay_money)
          }
        } else {

          val game_account = t2(0)
          val regi_date = t2(1)
          val game_id = t2(2)
          val os = t2(3)
          val imei = t2(4)
          var pay_money = "0"
          if ((!t2(5).equals("null")) && (t2(5) != null)) {
            pay_money = t2(5)
          }

          FileUtil.apppendTofile("regi_imie_error.csv", game_account + "," + regi_date + "," + game_id + "," + os + "," + imei + "," + pay_money)

        }
      })
    })


  }
}
