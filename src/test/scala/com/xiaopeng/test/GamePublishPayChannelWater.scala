package com.xiaopeng.test

import java.io.{BufferedWriter, File, FileWriter}

import com.xiaopeng.bi.utils.{DateUtils, SparkUtils}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by kequan on 3/28/17.
  */
object GamePublishPayChannelWater {

  def main(args: Array[String]): Unit = {
    // 创建各种上下文
    val sparkConf = new SparkConf().setAppName(this.getClass.getName.replace("$",""));
    SparkUtils.setMaster(sparkConf);
    val sc = new SparkContext(sparkConf);
    val hiveContext = new HiveContext(sc);
    val yesterday: String = DateUtils.getYesterDayDate();

    //分析数据
    hiveContext.sql("use yyft");
    val sql = "select to_date(o.order_time) a,pgame.group_id b,bg.name c,concat('2') d,sum(if(o.order_status = 4,o.ori_price,-o.ori_price)) e\nfrom ods_order o join (select distinct bb.game_id,bb.group_id from ods_publish_game bb where group_id in (1,2,3)) pgame on o.game_id=pgame.game_id\njoin bgame bg on o.game_id=bg.id \nwhere to_date(o.order_time) = 'yesterday' and o.payment_type in (2,11) and order_status in(4,8)\ngroup by to_date(o.order_time),pgame.group_id,bg.name\nunion\nselect to_date(o.order_time) a,pgame.group_id b,bg.name c,o.payment_type d,sum(if(o.order_status = 4,o.ori_price,-o.ori_price)) e\nfrom ods_order o join (select distinct bb.game_id,bb.group_id from ods_publish_game bb where group_id in (1,2,3)) pgame on o.game_id=pgame.game_id\njoin bgame bg on o.game_id=bg.id \nwhere to_date(o.order_time) = 'yesterday' and o.payment_type in (3,7,5)  and order_status in(4,8)\ngroup by to_date(o.order_time),pgame.group_id,o.payment_type,bg.name"
    val df: DataFrame = hiveContext.sql(sql.replace("yesterday", yesterday))

    //将数据存入文件
    val fileparent = new File("/home/hduser/crontabFiles/tecpmo0001_409")
    if (!fileparent.exists()) {
      fileparent.mkdirs()
    }
    val header = "统计周期,发行组别,发行游戏名,支付类别,流水金额"
    val file = new File("/home/hduser/crontabFiles/tecpmo0001_409/publish_water_" + yesterday.trim.replace("-", "") + ".csv")
    if (file.exists()) {
      file.delete()
    }
    file.createNewFile()
    val writer = new BufferedWriter(new FileWriter(file, true))
    writer.append(header)
    writer.newLine()
    writer.flush()
    writer.close()

    df.foreachPartition(iter => {
      val file = new File("/home/hduser/crontabFiles/tecpmo0001_409/publish_water_" + yesterday.trim.replace("-", "") + ".csv")
      val writer = new BufferedWriter(new FileWriter(file, true))
      iter.foreach(t => {
        var pay_type_deatil = ""
        val statistics_cycle = t.get(0).toString.trim.replace("-", "");
        val group_id = "发行" + t.get(1).toString + "组";
        val game_name = t.get(2).toString;
        val pay_type = t.get(3).toString;
        val order_Amount = t.get(4).toString;
        if (pay_type != null && (pay_type.equals("2") || pay_type.equals("11"))) {
          pay_type_deatil = "微信"
        } else if (pay_type != null && pay_type.equals("3")) {
          pay_type_deatil = "支付宝"
        } else if (pay_type != null && pay_type.equals("7")) {
          pay_type_deatil = "银联"
        } else if (pay_type != null && pay_type.equals("5")) {
          pay_type_deatil = "苹果内购"
        }
        writer.append(statistics_cycle + "," + group_id + "," + game_name + "," + pay_type_deatil + "," + order_Amount)
        writer.newLine()
      })
      writer.flush()
      writer.close()
    })

    //将数据以邮件形式发送出去
  }
}
