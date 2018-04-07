package com.xiaopeng.test

import com.xiaopeng.bi.utils.{JedisUtil, SparkUtils}
import org.apache.spark.{SparkConf, SparkContext}
import redis.clients.jedis.Jedis

/**
  * Created by kequan on 5/11/17.
  */
object Redistest {
  def main(args: Array[String]): Unit = {
    //创建各种上下文
    val sparkConf = new SparkConf().setAppName(this.getClass.getName.replace("$", ""))
      .set("spark.default.parallelism", "60")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.shuffle.consolidateFiles", "true")
      .set("spark.sql.shuffle.partitions", "60")
    SparkUtils.setMaster(sparkConf);
    val sc = new SparkContext(sparkConf);

    val pool = JedisUtil.getJedisPool()
    val jedis: Jedis = pool.getResource;

    println(jedis.hgetAll("tc289000741"))
    println(jedis.hgetAll("tc289002415"))
    println(jedis.hgetAll("tc289001127"))
    println(jedis.hgetAll("tc289003968"))


    val rdd1 = sc.textFile("hdfs://hadoopmaster:9000/user/hive/warehouse/yyft.db/regi/logs_regi_hb_201608")
    rdd1.filter(row => {
      row.contains("bi_regi") && row.split("\\|", -1).length >= 19
    }).foreachPartition(it => {
      val pool = JedisUtil.getJedisPool()
      val jedis: Jedis = pool.getResource;
      jedis.select(13)

      it.foreach(row => {
        val arr = row.split("\\|", -1);
        val game_account = arr(3)
        val game_id = arr(4)
        val reg_time = arr(5)
        val expand_channel = arr(13)
        val alliance_bag_id = arr(18)
        jedis.hset(game_account, "game_id", game_id)
        jedis.hset(game_account, "reg_time", reg_time)
        jedis.hset(game_account, "expand_channel", expand_channel)
        jedis.hset(game_account, "alliance_bag_id", alliance_bag_id)
        jedis.expire(game_account,1800);
      })

      pool.returnResource(jedis)
      pool.destroy()
    })

    sc.stop()
  }
}
