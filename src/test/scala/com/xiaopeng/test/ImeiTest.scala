package com.xiaopeng.test

import com.xiaopeng.bi.utils.{FileUtil, MD5Util, SparkUtils}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by kequan on 3/28/17.
  */
object ImeiTest {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName(this.getClass.getName.replace("$", ""));
    SparkUtils.setMaster(sparkConf);
    val sc = new SparkContext(sparkConf);
    val hiveContext = new HiveContext(sc);

    hiveContext.sql("use yyft");
    val rdd2 = sc.textFile("file:///home/kequan/workspace3/xiaopeng_bi/src/test/scala/com/xiaopeng/test/regi_imei.txt")

    rdd2.foreachPartition(it => {
      it.foreach(t3 => {
        val imei = t3.trim()
        if (imei.length == 32) {
          val s11 = imei.substring(0, 32);
          val ss1 = s11.substring(0, 8);
          val ss2 = s11.substring(8, 12);
          val ss3 = s11.substring(12, 16);
          val ss4 = s11.substring(16, 20);
          val ss5 = s11.substring(20, s11.length());
          val ss = ss1 + "-" + ss2 + "-" + ss3 + "-" + ss4 + "-" + ss5;

          FileUtil.apppendTofile("regi_imei.csv", ss + "," + MD5Util.string2MD5(ss))
        }
      })
    })
  }

}
