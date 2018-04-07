package com.xiaopeng.bi.app

import java.sql.{Connection, PreparedStatement, SQLException}

import com.xiaopeng.bi.checkdata.MissInfo2Redis
import com.xiaopeng.bi.utils.{ConfigurationUtil, Hadoop, JdbcUtil, JedisUtil}
import kafka.serializer.StringDecoder
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by denglh on 2016/11/3.
  * function:实现最近在玩,累计逻辑,下载等实现
  */
object AppV2_2 {
  def main(args: Array[String]) {
    Hadoop.hd
    val ssc= StreamingContext.getOrCreate(ConfigurationUtil.getProperty("App2_2"), apps _)
    ssc.start()
    ssc.awaitTermination()
  }
  /**
    * 获取kafka streaming数据
    * @return StreamingContext
    */
  def apps(): StreamingContext = {
    val Array(brokers, topics) = Array(ConfigurationUtil.getProperty("kafka.metadata.broker.list"),ConfigurationUtil.getProperty("topicsApp"))
    val sparkConf = new SparkConf().setAppName(this.getClass.getName.replace("$", ""))
    sparkConf.set("spark.streaming.backpressure.enabled","true")
    val ssc = new StreamingContext(new SparkContext(sparkConf), Seconds(30))//500 Seconds
    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
    //从kafka中获取所有游戏日志数据
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet)
    //获取人tuple的values
    val valuesDstream = messages.map(_._2.toLowerCase)

    //手机内安装，time(0),deviceid(1),packname(2），appname（3），status（4）,platform(5),需要判断是否有6个长度
    val appInstall=valuesDstream.filter(x=>x.split("\\|")(0).contains("bi_install_app")).filter(_.split("\\|",-1).length>=6)
      .map(x=>{(x.split("\\|")(0).split(",")(0),x.split("\\|")(1),x.split("\\|")(2),x.split("\\|")(3),x.split("\\|",-1)(4).toInt,if(x.split("\\|",-1)(5).equals("")||x.split("\\|",-1)(5).equals("1")) {"android"} else {"ios"})})

    //device_id(11),uid(9),loaddowntime(8),gamei_id(1),channel_id(2)
    val appdownloadInfo=valuesDstream.filter(x=>x.split("\\|",-1)(0).contains("bi_appdownload"))
      .filter(_.split("\\|",-1).length>=11)
      .filter(!_.split("\\|",-1)(9).equals(""))
      .map(x=>{(x.split("\\|",-1)(3).concat(x.split("\\|",-1)(11)),x.split("\\|",-1)(9),x.split("\\|",-1)(8),x.split("\\|",-1)(1).toInt,x.split("\\|",-1)(2).toInt)}) //APP下载游戏日志，device_id,uid,downloadtime,game_id,channel_id

    //game_account(5),time(6),oriprice(10),status(19),order_no(2)
    val orderInfo: DStream[(String, String, Float, String, String)] =valuesDstream.filter(x=>x.split("\\|")(0).contains("bi_order"))
      .filter(_.split("\\|",-1).length>=26)
      .filter(_.split("\\|",-1)(19).toInt>0)
      .filter(_.split("\\|",-1)(19).toInt%4==0)
      .filter(_.split("\\|",-1)(25).toLowerCase.contains("v2.2"))
      .map(x=>{(x.split("\\|")(5),x.split("\\|")(6),x.split("\\|")(10).toFloat,x.split("\\|")(19),x.split("\\|")(2))})//从订单日志来，状态=4；ver=v2.2 ；

    //game_account(3),logintime(4)
    val loginInfo=valuesDstream.filter(x=>x.split("\\|",-1)(0).contains("bi_login")).filter(_.split("\\|",-1).length>=4).map(x=>{(x.split("\\|",-1)(3),x.split("\\|",-1)(4))}) //登录日志

    //手机安装
    try {appPhoneApps(appInstall); }    catch {case e: Exception => e.printStackTrace}

    //累计下载
    try {appDownloads(appdownloadInfo); }catch {case e: Exception => e.printStackTrace}

    //累计流水
    try { orderLtdPrice(orderInfo);  }catch {case e: Exception => e.printStackTrace}

    //最近在玩
    try {gameRecently(loginInfo); }catch {case e: Exception => e.printStackTrace}

    ssc.checkpoint(ConfigurationUtil.getProperty("App2_2"))
    ssc
  }

  /**
    * 统计累计下载
    * logic:只对第一次下载进行累计,根据userid，deviceid，gameid，通过redis作为中介，判断是否存在，不存在则插入到redisdb2
    *
    * @param appdownloadInfo[imei,uid,time,gamei_id,channel_id]
    */
  def appDownloads(appdownloadInfo: DStream[(String, String,String,Int,Int)]) = {
    val sql="insert into bi_app_quota(uid,uname,loads) values(?,?,?) on duplicate key update loads=loads+?"
    val firstSql="insert into bi_app_download(uid,game_id,channel_id,download_time) values(?,?,?,?) on duplicate key update download_time=? "
    appdownloadInfo.foreachRDD(rdd=>{
      println("-------->appDownloads开始处理")
      rdd.foreachPartition(fp=>{
        val conn = JdbcUtil.getConn() //数据库连接
        val ps: PreparedStatement = conn.prepareStatement(sql)
        val firstPs: PreparedStatement = conn.prepareStatement(firstSql)
        //获取redis
        val pool= JedisUtil.getJedisPool
        val jedis=pool.getResource
        for(ds <-fp){
          //判断是否有值在redis中
          jedis.select(2)
          if ((!jedis.exists("appDownloads|"+ds._1 + "|" + ds._2+"|"+ds._4.toString))&&(!ds._2.equals("")))  //没有则插入到redis
          {
            val uid = ds._2 //通行证id
            jedis.select(0)
            if (jedis.hget(uid + "_member", "username") == null && (!uid.equals(""))) {
              MissInfo2Redis.checkMember(ds._2)
            } //判断是否通行证已经存在redis中，没则从mysql补数
            val uname = jedis.hget(ds._2 + "_member", "username") //获取通行证
            jedis.select(2) //使用2号数据库
            //把结果数据插入到表中,但是通行证和id不能为空
            if ((!uid.equals("")) && uname != null) {
              //累计统计
              ps.setString(1, uid)
              ps.setString(2, uname)
              ps.setInt(3, 1)
              ps.setInt(4, 1)
              ps.executeUpdate()
              //首充下载统计
              firstPs.setString(1, uid)
              firstPs.setInt(2, ds._4)
              firstPs.setInt(3, ds._5)
              firstPs.setString(4, ds._3)
              //update
              firstPs.setString(5, ds._3)
              firstPs.executeUpdate()
              //第一次下载数据插入到redis中
              val download = new java.util.HashMap[String, String]()
              download.put("download_time",ds._3.toString)
              jedis.hmset("appDownloads|" + ds._1 + "|" + ds._2 + "|" + ds._4.toString, download)
              jedis.expire("appDownloads|" + ds._1 + "|" + ds._2 + "|" + ds._4.toString, 3600 * 24 * 30)
            } else {
              println("-------->appDownloads累计下载通行证不存在")
            }
          }
          else {println("-------->appDownloads该设备该通行证已经累计下载一次，不在进行统计")}
        }
        conn.close()
        //redis close
        pool.returnResource(jedis)
        pool.destroy()
      })
      println("-------->appDownloads处理结束")
    })
  }

  /**
    * 把数据加载到bi_app_phoneapps表，记录手机内应用安装情况
    * @param appInstall time(0),deviceid(1),packname(2），appname（3），status（4）,os(5)
    * */
  def appPhoneApps(appInstall: DStream[(String, String, String, String, Int,String)]) = {
    val sql="insert into bi_app_phoneapps(device_id,pkg_name,app_name,status,opera_date,platform) values(?,?,?,?,?,?) on duplicate key update app_name=?,opera_date=?,status=?,platform=?"
    appInstall.foreachRDD(rdd=>{
      println("-------->appPhoneApps开始处理")
      rdd.foreachPartition(fp=>{
        val conn = JdbcUtil.getConn() //数据库连接
        val ps: PreparedStatement = conn.prepareStatement(sql)
        //获取redis
        val pool = JedisUtil.getJedisPool
        val jedis=pool.getResource
        jedis.select(10)
        fp.foreach(ds => {
          val pkg=jedis.hget(ds._3.toLowerCase,"package_name")
          if(pkg!=null) {
            println("---------------------------->"+pkg)
            ps.setString(1, ds._2)
            ps.setString(2, ds._3)
            ps.setString(3, ds._4)
            ps.setInt(4, if(ds._5==0){2} else 1)
            ps.setString(5, ds._1)
            ps.setString(6, ds._6)
            //update info
            ps.setString(7, ds._4)
            ps.setString(8, ds._1)
            ps.setInt(9, if(ds._5==0){2} else 1)
            ps.setString(10, ds._6)
            ps.executeUpdate()
          }
        })
        conn.close()
        //redis close
        pool.returnResource(jedis)
        pool.destroy()
      })
      println("-------->appPhoneApps处理结束")
    })
  }

  /**
    * 统计累计流水
    * logic:根据订单日志求累计流水
    * @param orderInfo[game_account,time,oriprice,status,orderno]
    */
  def orderLtdPrice(orderInfo: DStream[(String,String, Float,String,String)]) = {
      orderInfo.foreachRDD(rdd=>
      {
      println("-------->orderLtdPrice开始处理")
      rdd.foreachPartition(fp=>{
        val conn = JdbcUtil.getConn() //数据库连接
        val sql="insert into bi_app_quota(uid,uname,ori_price) values(?,?,?) on duplicate key update ori_price=ori_price+?"
        val ps: PreparedStatement = conn.prepareStatement(sql)
        //获取redis
        val pool= JedisUtil.getJedisPool
        val jedis= pool.getResource
        fp.foreach(order=>
        {
          jedis.select(0)
          var bind_member_id: String =jedis.hget(order._1,"bind_member_id")
          println("########################:"+jedis.hget(order._1,"game_id"))
          if(!jedis.exists(order._1))  //判断账号是否在表中，若没有则补充账号
          {MissInfo2Redis.checkAccount(order._1) }
          else if(bind_member_id.length>10) //长度大于是说明是手机号
          {
            MissInfo2Redis.checkAccountBindMember(order._1,bind_member_id)
          }
          bind_member_id =jedis.hget(order._1,"bind_member_id")
          val uid=if(bind_member_id==null||bind_member_id.equals("")) "0" else bind_member_id
          //判断是否通行证id有对应的通行证名称
          if(!jedis.exists(uid+"_member")&&(!uid.equals("0"))&&uid.length<11){MissInfo2Redis.checkMember(uid)}
          val uname=jedis.hget(uid+"_member","username")  //获取通行证
          val oriPrice=if(order._4.toInt==4) order._3 else {0-order._3}
          //把结果数据插入到表中,但是通行证和通行证id不能为空,同时判断是否已经统计过
          jedis.select(2)
          if(uname!=null&&(uid.toInt>0)&&(!jedis.exists("order_no|" + order._5.toString))) {
            try
            {
              ps.setString(1, uid)
              ps.setString(2, uname)
              ps.setFloat(3, oriPrice)
              ps.setFloat(4, oriPrice)
              val st=ps.executeUpdate()
              //当插入成功则放到redis，并且进行超时处理
              if(st>0) {
                val orderno = new java.util.HashMap[String, String]()
                orderno.put("order_no", order._5.toString)
                jedis.hmset("order_no|" + order._5.toString, orderno)
                jedis.expire(order._5, 3600 * 7*24)
              }else
              {
                //对于处理失败的订单进行收集，有利于手工处理，保留30天
                jedis.select(2)
                val ext = new java.util.HashMap[String, String]()
                ext.put("game_account", order._1.toString)
                ext.put("order_time", order._2.toString)
                ext.put("oriprice", order._3.toString)
                ext.put("status", order._4.toString)
                ext.put("orderno", order._5.toString)
                jedis.hmset("app2.2_execfail|" + order._1.toString+"|"+ order._5.toString+"|"+order._4, ext)
                jedis.expire("app2.2_execfail|" + order._1.toString+"|"+ order._5.toString+"|"+order._4, 3600 * 24*15)
              }
            }
            catch
              {
                case e: SQLException => e.printStackTrace
              }
          }
          else {println("--------->orderLtdPrice累计流水归属通行证不存在或者已经被统计过uid:"+uid+",orderno:"+order._5)}
        })
        conn.close()
        //redis close
        pool.returnResource(jedis)
        pool.destroy()
      })
      println("-------->orderLtdPrice处理结束")
    })
  }

  /**
    * 把数据加载到bi_app_game_recently表，记录最近数据
    * @param   loginInfo 【game_account,logintime】
    * */
  def gameRecently(loginInfo: DStream[(String, String)]) = {
    val results = loginInfo
    results.foreachRDD(rdd => {
      rdd.foreachPartition(rp => {
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
    })
  }


}


