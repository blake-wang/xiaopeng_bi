package com.xiaopeng.bi.app

import java.io.File
import java.sql.{Connection, PreparedStatement}

import com.xiaopeng.bi.checkdata.MissInfo2Redis
import com.xiaopeng.bi.utils.{ConfigurationUtil, JdbcUtil, JedisUtil}
import kafka.serializer.StringDecoder
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by denglh on 2016/11/3.
  * function:实现最近在玩,累计逻辑,下载等实现
  */
object AppV2_2Login {
  def main(args: Array[String]) {
    val path: String = new File(".").getCanonicalPath
    System.getProperties().put("hadoop.home.dir", path)
    new File("./bin").mkdirs()
    new File("./bin/winutils.exe").createNewFile()
    //日志输出警告
    //val ssc=recently()
    val ssc= StreamingContext.getOrCreate(ConfigurationUtil.getProperty("checkpointdirlogin"), recently _)
    ssc.start()
    ssc.awaitTermination()
  }
   /**
    * 把数据加载到bi_app_game_recently表，记录最近数据
     * @param   loginInfo 【game_account,logintime】
    * */
  def gameRecently(loginInfo: DStream[(String, String)]) = {
    val results = loginInfo
    results.print()
    results.foreachRDD(rows => {
      println("--------->开始处理")
      rows.foreachPartition(rp => {
        val conn: Connection = JdbcUtil.getConn() //数据库连接
        //都有数据时插入语句
        val sql2Mysql = "insert into bi_app_game_recently(uid,game_id,channel_id,recently_login_time) values(?,?,?,?)  on duplicate key update recently_login_time=?"
        val ps: PreparedStatement = conn.prepareStatement(sql2Mysql)
        //获取redis
        val pool = JedisUtil.getJedisPool
        val jedis=pool.getResource
        for(row<-rp)
          {
          var  bind_member_id=jedis.hget(row._1,"bind_member_id")
          if(!jedis.exists(row._1))  //判断账号是否在表中，若没有则补充账号
          {MissInfo2Redis.checkAccount(row._1) }
          else if(bind_member_id.length>10) //有账号但是没绑定到memberid,或者memberid是手机号。也需要补数
          {MissInfo2Redis.checkAccountBindMember(row._1,bind_member_id)}
           //补充数据后再获取
          bind_member_id=jedis.hget(row._1,"bind_member_id")
          val uid=if(bind_member_id==null||bind_member_id.equals("")) "0" else bind_member_id //通过游戏账号获取通行证id
          if(!jedis.exists(uid+"_member")&&(!uid.equals("0"))&&uid.length<11){MissInfo2Redis.checkMember(uid)} //补充通行证表
          //判断是否符合条件插入表中
          if (!uid.equals("0"))
          {
           //当形成的数据没有订单数据时不更新订单日期，若形成的结果数据没有登录数据时不更新登录时间
          val game_id=jedis.hget(row._1, "game_id")  //根据账号找gameid
          val channel_id=jedis.hget(row._1, "channel_id") //根据账号找渠道id
          val recently_login_time=row._2 //最近在玩时间
          if(bind_member_id!=null&&game_id!=null&&channel_id!=null)
              {
                    ps.setString(1, bind_member_id)
                    ps.setString(2, game_id)
                    ps.setString(3, channel_id)
                    ps.setString(4, recently_login_time)
                    //update
                    ps.setString(5, recently_login_time)
                    ps.executeUpdate();
              }
           else {
           println("--------->gameRecently不符合规定的数据")
           }
          } else {println("--------->gameRecently通行证id不符合要求，可能不存在或者账号没绑定："+row._1)}
        }
        //关闭mysql连接
        conn.close()
        //redis close
        pool.returnResource(jedis)
        pool.destroy()
      }
      )
      println("--------->处理结束")
    })
  }

  /**
    * 获取kafka streaming数据
    * @return StreamingContext
    */
  def recently(): StreamingContext = {
    val Array(brokers, topics) = Array(ConfigurationUtil.getProperty("kafka.metadata.broker.list"),ConfigurationUtil.getProperty("topics"))
    val sparkConf = new SparkConf().setAppName(this.getClass.getName.replace("$",""))
    sparkConf.set("spark.streaming.stopGracefullyOnShutdown","true")  //确保在kill任务时，能够处理完最后一批数据，再关闭程序，不会发生强制kill导致数据处理中断，没处理完的数据丢失
    sparkConf.set("spark.streaming.backpressure.enabled","true")  //开启后spark自动根据系统负载选择最优消费速率
    sparkConf.set("spark.streaming.backpressure.initialRate","100")
    val ssc = new StreamingContext(new SparkContext(sparkConf), Seconds(15))//5 minutes
    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
    //从kafka中获取所有游戏日志数据
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet)
    //获取人tuple的values
    val valuesDstream = messages.map(_._2.toLowerCase)
    //game_account(3),logintime(4)
    val loginInfo=valuesDstream.map(x=>{(x.split("\\|")(3),x.split("\\|",-1)(4))}) //登录日志
    gameRecently(loginInfo); //最近在玩
    ssc.checkpoint(ConfigurationUtil.getProperty("checkpointdirlogin"))
    ssc
  }

}


