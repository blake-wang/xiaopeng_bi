package com.xiaopeng.test

import com.xiaopeng.bi.utils._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import redis.clients.jedis.{Jedis, JedisPool}

import scala.collection.mutable.ArrayBuffer


/**
  * Created by kequan on 3/27/17.
  */
object SparkPartionTest {
  def main(args: Array[String]): Unit = {
    //创建各种上下文
    val sparkConf = new SparkConf().setAppName(this.getClass.getName.replace("$", ""))
      .set("spark.default.parallelism", "60")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.shuffle.consolidateFiles", "true")
      .set("spark.sql.shuffle.partitions", "60")
    SparkUtils.setMaster(sparkConf);
    val sc = new SparkContext(sparkConf);

    val hiveContext: HiveContext = HiveContextSingleton.getInstance(sc)
    val rdd1 = sc.textFile("file:///home/kequan/workspace/xiaopeng_bi/src/test/scala/com/xiaopeng/test/d_active.txt")
    // 把 以前的日志 bi_pubgame 和 本次实时的日志 bi_pubgame 相加
    var indexarr = new ArrayBuffer[Int]()
    var index: Broadcast[ArrayBuffer[Int]] = sc.broadcast(indexarr);
    rdd1.foreachPartition(it => {
      // redis 链接
      val pool: JedisPool = JedisUtil.getJedisPool;
      val jedis: Jedis = pool.getResource;
      jedis.select(0);
      index.value.+=(1);
      FileUtil.apppendTofile("aa.txt",index.value.size+"==="+pool+"=="+jedis+ "|"+ System.currentTimeMillis())
      it.foreach(t => {
      })

      pool.returnResource(jedis)
      pool.destroy()
    })
  }


}
