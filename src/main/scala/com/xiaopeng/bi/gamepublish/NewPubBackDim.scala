package com.xiaopeng.bi.gamepublish

import com.xiaopeng.bi.utils.{DimensionUtil, SparkUtils}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext};

/**
  * 更新游戏的维度信息
  */
object NewPubBackDim {

  var startDay = "";
  var endDay = ""
  var mode = ""

  def main(args: Array[String]) {
    if (args.length == 3) {
      startDay = args(0)
      endDay = args(1)
      mode = args(2)
    } else if (args.length == 2) {
      startDay = args(0)
      endDay = startDay
      mode = args(1)
    }

    if (mode.equals("game")) {
      // 更新游戏的维度信息
      DimensionUtil.updateGameDim(startDay, endDay)
    } else if (mode.equals("pkg")) {
      // 更新分包的维度信息
      val sparkConf = new SparkConf().setAppName(this.getClass.getName.replace("$", ""))
        .set("spark.default.parallelism", "60")
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .set("spark.shuffle.consolidateFiles", "true")
        .set("spark.storage.memoryFraction", "0.4")
        .set("spark.streaming.stopGracefullyOnShutdown", "true");
      SparkUtils.setMaster(sparkConf);
      val sc: SparkContext = new SparkContext(sparkConf);
      val hiveContext = new HiveContext(sc);
      hiveContext.sql("use yyft");
      GamePublishe2TmpDim.creatDbDim()
      DimensionUtil.processDbDim(hiveContext,startDay,endDay)
    }
  }

}
