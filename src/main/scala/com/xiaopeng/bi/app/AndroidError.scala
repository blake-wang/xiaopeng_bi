package com.xiaopeng.bi.app

import java.util

import com.xiaopeng.bi.bean.AndroidErrorBean.DataBean
import com.xiaopeng.bi.utils.{AnalysisJsonUtil, ConfigurationUtil, DateUtils, SparkUtils}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by kequan on 3/6/17.
  */
object AndroidError {

  def main(args: Array[String]): Unit = {
    //创建各种上下文
    val sparkConf = new SparkConf().setAppName(this.getClass.getName.replace("$",""));
    SparkUtils.setMaster(sparkConf);
    val sc: SparkContext = new SparkContext(sparkConf);
    val hiveContext = new HiveContext(sc);
    val rddlogs: RDD[String] = sc.textFile(ConfigurationUtil.getProperty("android.error") + "log_" + args(0) + "*").cache();
    deallogs(rddlogs, hiveContext);
  }

  def deallogs(rddlogs: RDD[String], hiveContext: HiveContext) = {
    val rowRDD = rddlogs.map(x => {
      val bean = AnalysisJsonUtil.DecodingAndAnalysisAndroidError(x.toString);
      val databeanList: util.List[DataBean] = bean.getData();
      var result: String = "";
      for (x <- 0.to(databeanList.size() - 1)) {
        val databean: DataBean = databeanList.get(x);
        result = result + bean.getDevice_id + "|" + bean.getApp_versioncode + "|" + bean.getApp_versionname + "|" + bean.getChannel_number + "|" + bean.getImei + "|" + bean.getModel + "|" + bean.getPlatform + "|" + bean.getSdk_version + "|" + bean.getSigned_md_five + "|" + databean.get_id.toString + "|" + databean.isIsThrowable.toString + "|" + DateUtils.formatTime(databean.getTime) + "|" + databean.getMsg.replace("\n\t", "") + "#";
      };
      result.substring(0, result.length - 1);
    }).flatMap(_.split("\\#")).map(x => {
      val ms = x.split("\\|");
      Row(ms(0), ms(1), ms(2), ms(3), ms(4), ms(5), ms(6), ms(7), ms(8), ms(9), ms(10), ms(11), ms(12))
    })
    val schema = (new StructType).add("device_id", StringType).add("app_versioncode", StringType).add("app_versionname", StringType)
      .add("channel_number", StringType).add("imei", StringType).add("model", StringType)
      .add("platform", StringType).add("sdk_version", StringType).add("signed_md_five", StringType).add("_id", StringType).add("isThrowable", StringType).add("time", StringType).add("msg", StringType);
    val df = hiveContext.createDataFrame(rowRDD, schema);
    df.saveAsTable("yyft.app_android_errors", SaveMode.Append);
  }
}