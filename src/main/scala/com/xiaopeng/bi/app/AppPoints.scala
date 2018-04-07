package com.xiaopeng.bi.app

import com.xiaopeng.bi.utils._
import kafka.serializer.StringDecoder
import org.apache.log4j.Logger
import org.apache.spark.sql.Row
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{Logging, SparkConf, SparkContext}
import redis.clients.jedis.{Jedis, JedisPool}

import scala.collection.mutable.ArrayBuffer

/**
  * 积分
  */
object AppPoints extends Logging {
  val logger = Logger.getLogger(AppPoints.getClass)

  def main(args: Array[String]) {
    val Array(brokers, topics) = Array(ConfigurationUtil.getProperty("kafka.metadata.broker.list"), ConfigurationUtil.getProperty("kafka.topics.apppoints"));
    val sparkConf = new SparkConf().setAppName(this.getClass.getName.replace("$","")).setMaster("local")
      .set("spark.sql.shuffle.partitions", ConfigurationUtil.getProperty("spark.sql.shuffle.partitions"))
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //spark.streaming.kafka.maxRatePerPartition 限速

    val sparkContext = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sparkContext, Seconds(10))
    ssc.checkpoint(ConfigurationUtil.getProperty("spark.checkpoint.apppoints"));
    // Create direct kafka stream with brokers and topics
    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String](
      "metadata.broker.list" -> brokers,
      "group.id" -> "kafkatestgrouppointstest",
      "auto.offset.reset" -> "largest" //smallest largest
    )
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topicsSet)

    messages.foreachRDD(rdd => {
      if (!rdd.isEmpty()) {
        val sc = rdd.sparkContext
        val sqlContext = SQLContextSingleton.getInstance(sc)
        val valueRdd = rdd.map(line => line._2)
        // 将日志转换为 积分表 app_points_sign_detail 和 支付表 app_points_recharge_detail

        AppStreamingUtils.parsePointsSignDetail(valueRdd, sqlContext).parsePointsRechargeDetail(valueRdd, sqlContext)
        // 筛选出积分来源为：签到和充值的 数据
        val appPointsDF = sqlContext.sql("select * from app_points_sign_detail where resourse=4")
          .unionAll(sqlContext.sql("select obtain_time,obtain_id,uid,uname,game_id,game_name,points" +
            ",ori_price,resourse from app_points_recharge_detail where resourse=1 and order_state=4 and version='v2.2'"))
        appPointsDF.show()
        appPointsDF.rdd.foreachPartition(rows => {
          //创建jedis客户端
          val pool: JedisPool = JedisUtil.getJedisPool
          val jedis: Jedis = pool.getResource
          val conn = JdbcUtil.getConn()
          val statement = conn.createStatement

          val rs = statement.executeQuery("select sum(points) history_points_sum from bi_app_points")
          //插入积分到mysql之前，库中所有的积分
          var history_points_sum = 0
          if (rs.next()) {
            history_points_sum = rs.getInt("history_points_sum")
          }
          val insertedRows = new ArrayBuffer[Row]()
          for (row <- rows) {
            val filterRow = AppStreamingUtils.getPointFilterRow(row, jedis)
            val points = filterRow.getDouble(6).toInt
            history_points_sum = history_points_sum + points

            insertedRows.+=(Row(filterRow(0), filterRow(1), filterRow(2), filterRow(3), filterRow(4),
              filterRow(5), points, filterRow(7), filterRow(8), history_points_sum))
          }

          //批量插入mysql中的bi_app_points
          val pointsSqlText = "insert into bi_app_points(obtain_time,obtain_id,uid,uname,game_id,game_name,points,ori_price,resourse,real_points,create_time) " +
            "values(?,?,?,?,?,?,?,?,?,?,?)"
          val quotaSqlText = "insert into bi_app_quota(uid,uname,points,create_time) values(?,?,1,?)" +
            " on duplicate key update points=points+1"

          val pointsParams = new ArrayBuffer[Array[Any]]()
          val quotaParams = new ArrayBuffer[Array[Any]]()
          for (insertedRow <- insertedRows) {
            pointsParams.+=(Array[Any](insertedRow(0), insertedRow(1), insertedRow(2), insertedRow(3), insertedRow(4)
              , insertedRow(5), insertedRow(6), insertedRow(7), insertedRow(8), insertedRow(9), DateUtils.getTodayTime()))
            quotaParams.+=(Array[Any](insertedRow(2), insertedRow(3), DateUtils.getTodayTime()))
          }

          JdbcUtil.doBatch(pointsSqlText, pointsParams, conn)
          JdbcUtil.doBatch(quotaSqlText, quotaParams, conn)
          //关闭数据库链接
          statement.close()
          conn.close

          //关闭redis
          pool.returnResource(jedis)
          pool.destroy()
        })
      }
    })

    ssc.start()
    ssc.awaitTermination()
  }
}