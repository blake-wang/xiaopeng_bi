package com.xiaopeng.test

import com.xiaopeng.bi.utils.JedisUtil
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructType}
import redis.clients.jedis.{Jedis, JedisPool}

/**
  * Created by bigdata on 18-1-25.
  */
object TestTwo {


  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName(this.getClass.getName.replace("$", "")).setMaster("local")
    val sparkContext = new SparkContext(sparkConf)
    val hiveContext = new HiveContext(sparkContext)
    val rdd = sparkContext.textFile("file:///home/bigdata/IdeaProjects/xiaopeng_bi/src/test/scala/com/xiaopeng/test/d_regi.txt")
    rdd.foreach(println(_))


    convertOrderToDfTmpTable(rdd, hiveContext)

    sparkContext.stop()

  }

  def convertOrderToDfTmpTable(isRDD: RDD[String], sqlContext: SQLContext): Unit = {

    println("2222222222")
    val orderRdd = isRDD.filter(line => {
      line.contains("bi_order")
    }).mapPartitions(iter => {
      val pool: JedisPool = JedisUtil.getJedisPool()
      val jedis: Jedis = pool.getResource;

      jedis.select(4)

      iter.map(line => {

        println("ling-2 :" + line)
        jedis.set("456", line)
        try {
          val splited = line.split("\\|", -1)
          var totalAmt = 0.0
          if (splited(13) != "") {
            totalAmt = java.lang.Double.parseDouble(splited(13))
          }
          if (!jedis.exists("kpi_orderexists|" + splited(2) + "|" + splited(6) + "|" + splited(5))) //检查redis缓存中是否存在已经半小时内处理的订单
          {
            println("不存在～～～～")
            jedis.set("kpi_orderexists|" + splited(2) + "|" + splited(6) + "|" + splited(5), "1")
            jedis.expire("kpi_orderexists|" + splited(2) + "|" + splited(6) + "|" + splited(5), 3600)

            Row(splited(5), java.lang.Integer.parseInt(splited(19)), splited(6).split(":")(0), splited(24),
              java.lang.Integer.parseInt(splited(7)), java.lang.Double.parseDouble(splited(10)),
              totalAmt, java.lang.Integer.parseInt(splited(22)))
            //进缓存并且存半小时

          } else {
            println("存在～～～～")
            val aaa = jedis.get("kpi_orderexists|" + splited(2) + "|" + splited(6) + "|" + splited(5))
            println("取出来的是 ：" + aaa.toString)
            Row("pyww", 0, "00-00-00 00", "", 0, 0, 0, 0)
          }
        } catch {
          case ex: Exception => {
            Row("pyww", 0, "00-00-00 00", "", 0, 0, 0, 0)
          }
        }

      })

    })
    println("4444")


    val orderStruct = (new StructType).add("game_account", StringType).add("order_status", IntegerType)
      .add("publish_time", StringType).add("imei", StringType).add("game_id", IntegerType).add("ori_price", DoubleType)
      .add("total_amt", DoubleType).add("prod_type", IntegerType)


    val orderDF = sqlContext.createDataFrame(orderRdd, orderStruct);
    orderDF.registerTempTable("ods_order_tmp")
    orderDF.show()

    //    sqlContext.sql("select o.* from ods_order_tmp o join lastPubGame pg on o.game_id = pg.game_id where o.order_status = 4 and prod_type=6").cache()
    //      .registerTempTable("ods_order")

  }

}
