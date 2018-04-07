package com.xiaopeng.bi.centurioncard

import java.sql.PreparedStatement

import com.xiaopeng.bi.checkdata.MissInfo2Redis
import com.xiaopeng.bi.utils._
import kafka.serializer.StringDecoder
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import redis.clients.jedis.{Jedis, JedisPool}

/**
  * Created by Administrator on 2016/11/19.
  */
object CenturioncardGameAcc {
  Hadoop.hd
  var batch: Int = 5
  def batchInt(bt:Int):Unit={batch=bt}

  def main(args: Array[String]) {
    batchInt(args(0).toInt)
    val ssc =  StreamingContext.getOrCreate(ConfigurationUtil.getProperty("checkpointdirgameacc"), gameAcc _)
    ssc.start()
    ssc.awaitTermination()
  }
 /**
  *获取regi主题账号日志和order主题订单日志
  * 从redis中取游戏等商品信息，包括游戏名称、主游戏、游戏图片、黑金卡名称、账号中找操作系统、来源、注册时间等
  * 1、对退款的情况会出现不同时间 状态为8的日志信息,只记录第一条
  * 2、对购买多账号时需要进行按逗号分隔
  * 3、多从账号、通行证中找不到的数据，使用hive中实现（bi_centurioncard_gameacc需要调用着）
  * 4、找不到账号日志 则存到redis
  */
  def gameAcc():StreamingContext = {
    val Array(brokers: String, topics: String) = Array(ConfigurationUtil.getProperty("kafka.metadata.broker.list"), ConfigurationUtil.getProperty("gameAccTopic"))
    val sparkConf = new SparkConf().setAppName(this.getClass.getName.replace("$",""))
    sparkConf.set("spark.streaming.kafka.maxRatePerPartition","5");
    sparkConf.set("spark.streaming.stopGracefullyOnShutdown","true")
    sparkConf.set("spark.streaming.backpressure.enabled","true")
    val ssc = new StreamingContext(new SparkContext(sparkConf), Seconds(batch)) //Minutes(3) Seconds
    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
    //从kafka中获取所有游戏登录数据
    val messages: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet)

    //获取人tuple的values
    val valuesDstream = messages.map(_._2)
    //订单信息：游戏账号（5），订单号（2），订单日期（6），游戏id（7），渠道id（8），返利人id（18），产品类型（22），订单状态（19）,充值流水（10），返利金额（16）
    //只取有订单号的并且返利人为黑金卡
    val orderInfo: DStream[(String, String, String, String, String, String, Int, Int, Float, Float)] =valuesDstream.filter(x=>x.split("\\|")(0).contains("bi_order"))
                                .filter(_.split("\\|")(2).trim.length>0)   //只取有订单的，没有订单的没有黑金卡下单身份信息
                                .filter(!_.split("\\|")(19).contains("0")) //不为0
                                .filter(_.split("\\|")(19).toInt%4==0)     //只取状态为4或者8的数据
                                .filter(_.split("\\|",-1)(23).contains("1"))  //只取黑金卡下单时身份是1的日志数据
                                .map(x=>{(x.split("\\|")(5).trim.toLowerCase,x.split("\\|")(2),x.split("\\|")(6),x.split("\\|")(7),x.split("\\|")(8),x.split("\\|")(18),x.split("\\|")(22).toInt,x.split("\\|")(19).toInt,x.split("\\|")(10).toFloat,x.split("\\|")(16).toFloat)})
     orderInfo.foreachRDD(rdd=>{
     //补充丢失数据
     operaRedisData
     rdd.foreachPartition(fp=>{
        val pool: JedisPool =JedisUtil.getJedisPool
       val jedis=pool.getResource
        for(row<-fp)
            {
              //调用函数对row数据进行加载
              loadOrder(jedis,row,0)
            }
       pool.returnResource(jedis)
        pool.destroy()
      })
    })
    ssc.checkpoint(ConfigurationUtil.getProperty("checkpointdirgameacc"))
    ssc
  }

  /**
   *把订单数据处理 插入到表中,当标识位为0时，是从kafka来的日志数据，1为redis缓存数据重新加载
   *或者插入到redis
   */
  def loadOrder(jedis:Jedis,row: (String, String, String, String, String, String, Int, Int, Float, Float),flag: Int) ={
    val accounts=row._1.split(",").length
    for(account<-row._1.split(","))
    {
      jedis.select(0)
      val game_account=account
      val order_no=row._2
      val order_time=row._3
      val order_date=if(row._3==null) {"0000-00-00".substring(0,10)} else {row._3.substring(0,10)}
      val game_id=row._4
      val channel_id=row._5
      //原始
      val owner_id=row._6
      //获取
      val prod_type=if(row._7==1||row._7==3){0} else if(row._7==2||row._7==4){1} else if (row._7==6){2} else 100
      val order_status=if(row._8==4){"已完成"} else "已退款"
      //对两次产生8的日志进行redis处理，不然会出现退款日志多
      var order_8=""
      var order_4=""
      if(row._8==8)
        {
         order_8=order_no+"|"+game_account+"|"+order_status
        } else if(row._8==4) order_4=order_no+"|"+game_account
      //流水
      val ori_price: Float =if(row._8==4) row._9 else {0-row._9}
      val rebate_amount =if(row._8==4) row._10/accounts else {0-row._10/accounts}
      //取游戏产品数据
      val mainid=if(jedis.hget(game_id+"_bgame","mainid")==null) game_id else jedis.hget(game_id+"_bgame","mainid") //如果没找到主游戏就用子游戏
      val main_name=if(jedis.hget(game_id+"_bgame","main_name")==null) {if (jedis.hget(game_id+"_bgame","name")==null) {""} else jedis.hget(game_id+"_bgame","name")} else jedis.hget(game_id+"_bgame","main_name")
      val pic_small=if(jedis.hget(game_id+"_bgame","pic_small")==null) "" else jedis.hget(game_id+"_bgame","pic_small")
      //是否新增用户，若是首充则算1，其他的在hive中计算
      jedis.select(2)
      val is_user_add=if(prod_type==0) 1 else if (jedis.exists(game_account+"_historyRecharge")) {0} else 1;
      //用来判断是否新增交易用户
      jedis.set(game_account+"_historyRecharge",game_account)
      jedis.select(0)
      //通行证，账号信息；通行证能找到 账号也能找到则插入目标表，否则到redis 等下一次计算
      val user_account=if(jedis.hget(owner_id+"_member","username")==null) "" else jedis.hget(owner_id+"_member","username")
      val os=if(jedis.hget(game_account, "reg_os_type")==null) "UNKNOW" else jedis.hget(game_account, "reg_os_type")
      val reg_time=if(jedis.hget(game_account, "reg_time")==null) "0000-00-00" else jedis.hget(game_account, "reg_time")
      val reg_type=if((if(jedis.hget(game_account, "reg_resource")!=null)  jedis.hget(game_account, "reg_resource")).equals("8")) 1 else 2

      println("----------------"+{if(user_account.equals("")) "通行证找不到:"+owner_id else user_account })
      println("----------------"+{if(jedis.hget(game_account, "reg_os_type")==null) "账号表找不到账号:"+game_account else game_account })
      jedis.select(2)
      val ost8: String =jedis.get("order_status_8|"+order_8)
      val ost4: String =jedis.get("order_status_4|"+order_4)
      jedis.select(0)
      if(jedis.hget(game_account, "reg_os_type")!=null&&jedis.hget(owner_id+"_member","username")!=null)
     // if(jedis.hget(game_account, "reg_os_type")!=null&&jedis.hget(owner_id+"_member","username")!=null&&rs==null)
      {
        //插入数据到数据库,若是退款，但是没有以前的下单完成日志也不需要写
        if(row._8==8&&ost4==null)
          {println("订单完成状态没有，只有退单数据，不写表!")}
        else
        {processGameAcc(jedis,order_8,order_4,game_account, order_no, order_date,order_time, channel_id, prod_type, order_status, ori_price, rebate_amount, mainid, main_name, pic_small, is_user_add, user_account, os, reg_time, reg_type,row)}
      }
      else if (flag!=1){ //对于定时处理redis的调用不需要执行下面失实现
        jedis.select(0)
        //补充丢失通行证
        if(user_account.equals(""))
        {
          println("----------------通行证找不到:"+owner_id)
          //若从mysql补充数据成功则吧redis中的数据过时
          if(MissInfo2Redis.checkMember(owner_id).equals("0"))
          {
            println("miss is ok")
          }
        }
        //补充丢失账号
        if(jedis.hget(game_account, "reg_os_type")==null)
        {
          println("----------------账号表找不到账号:"+game_account)
          val ck= new MissInfo2Redis()
          //若从mysql补充数据成功则吧redis中的数据过时
          if(MissInfo2Redis.checkAccount(game_account.trim.toLowerCase)!=0)
          {
            println("miss is ok")
          }
        }
        jedis.select(2) //整行数据+账号数据 插入到数据库2
        jedis.set("gameacc_|"+game_account+"|"+row._2+"|"+row._3+"|"+row._4+"|"+row._5+"|"+row._6+"|"+row._7+"|"+row._8+"|"+row._9+"|"+row._10,game_account+"|"+row._2+"|"+row._3+"|"+row._4+"|"+row._5+"|"+row._6+"|"+row._7+"|"+row._8+"|"+row._9+"|"+row._10)
        jedis.expire("gameacc_|"+game_account+"|"+row._2+"|"+row._3+"|"+row._4+"|"+row._5+"|"+row._6+"|"+row._7+"|"+row._8+"|"+row._9+"|"+row._10,50)           //设置过期时间，30分钟
        println("gameacc_|"+game_account+"|"+row._2+"|"+row._3+"|"+row._4+"|"+row._5+"|"+row._6+"|"+row._7+"|"+row._8+"|"+row._9+"|"+row._10+"-->数据匹配不到缓存成功!")
      }
    }
  }
 /**
  * 推送数据到数据库,bi_centurioncard_gameacc表
  * 对于退单的数据可能存在退款中也为8的日志，存在应用上的bug
  */
  def processGameAcc(jedis:Jedis,order_8:String,order_4:String,game_account: String, order_no: String, order_date: String,order_time:String,  channel_id: String, rec_type: Int,
                order_status: String, ori_price: Float, rebate_amount: Float, mainid: String, main_name: String, pic_small: String,
                is_user_add: Int, user_account: String, os: String, reg_time: String, reg_type: Int,row: (String, String, String, String, String, String, Int, Int, Float, Float))
  {
    val mysqlsql="insert into bi_centurioncard_gameacc(statistics_date,user_account,game_account,reg_type,rec_type,rec_amount,reb_amount,order_no,order_status,game_name,game_icon,game_regi_date,os,game_id,channel_id,is_user_add,order_time,game_regi_time)" +
      " values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) on duplicate key update reg_type=?,rec_type=?,rec_amount=?,reb_amount=?,order_status=?,game_icon=?,is_user_add=?,game_regi_time=?"
    val sql2Mysql=mysqlsql.toLowerCase
    val conn = JdbcUtil.getConn()
    val ps: PreparedStatement = conn.prepareStatement(sql2Mysql)
    ps.setString(1, order_date)
    ps.setString(2, user_account)
    ps.setString(3, game_account)
    ps.setInt(4, reg_type)
    ps.setInt(5, rec_type)
    ps.setFloat(6, ori_price)
    ps.setFloat(7, rebate_amount)
    ps.setString(8, order_no)
    ps.setString(9, order_status)
    ps.setString(10, main_name)
    ps.setString(11, pic_small)
    ps.setString(12, reg_time)
    ps.setString(13, os)
    ps.setString(14, mainid)
    ps.setString(15, channel_id)
    ps.setInt(16, is_user_add)
    ps.setString(17, order_time)
    ps.setString(18, reg_time)
    //update
    ps.setInt(19, reg_type)
    ps.setInt(20, rec_type)
    ps.setFloat(21, ori_price)
    ps.setFloat(22, rebate_amount)
    ps.setString(23, order_status)
    ps.setString(24, pic_small)
    ps.setInt(25, is_user_add)
    ps.setString(26, reg_time)
    val st=ps.executeUpdate()
    //对此插入错误的数据进行缓存
    if(st!=1)
      {
        jedis.select(2) //整行数据+账号数据 插入到数据库2
        jedis.set("gameacc_|"+game_account+"|"+row._2+"|"+row._3+"|"+row._4+"|"+row._5+"|"+row._6+"|"+row._7+"|"+row._8+"|"+row._9+"|"+row._10,game_account+"|"+row._2+"|"+row._3+"|"+row._4+"|"+row._5+"|"+row._6+"|"+row._7+"|"+row._8+"|"+row._9+"|"+row._10)
        jedis.expire("gameacc_|"+game_account+"|"+row._2+"|"+row._3+"|"+row._4+"|"+row._5+"|"+row._6+"|"+row._7+"|"+row._8+"|"+row._9+"|"+row._10,50)           //设置过期时间，30分钟
        println("gameacc_|"+game_account+"|"+row._2+"|"+row._3+"|"+row._4+"|"+row._5+"|"+row._6+"|"+row._7+"|"+row._8+"|"+row._9+"|"+row._10+"-->数据插入失败")
      }
    //插入状态8成后写入到redis
    if(st>0&&order_8.length>0)
      {
        jedis.select(2)
        jedis.set("order_status_8|"+order_8,order_8)
        jedis.expire("order_status_8|"+order_8,15*24*3600)
        println("退款单插入redis成功！")
      }
    //插入状态8成后写入到redis
    if(st>0&&order_4.length>0)
    {
      jedis.select(2)
      jedis.set("order_status_4|"+order_4,order_4)
      jedis.expire("order_status_4|"+order_4,7*24*3600)
      println("订单完成单插入redis成功！")
    }
    ps.close()
    conn.close()
  }

  /**
   * 定时处理redis中的，从订单中找不到的数据，避免订单日志先到， 账号日志后到的情况
   * 扫描select 2库中找gameacc_关键字的key，然后能处理再找到相关账号的设置失效
   */
  def operaRedisData ={
    val pool: JedisPool =JedisUtil.getJedisPool
    val jedis=pool.getResource
    jedis.select(2)//扫描keys
    for(ks<-jedis.keys("gameacc_*").toArray())
      {
       val ao=ks.toString.split("\\|")
      //  new 成tuple10类型
       val t10 = new Tuple10(ao(1),ao(2),ao(3),ao(4),ao(5),ao(6),ao(7).toInt,ao(8).toInt,ao(9).toFloat,ao(10).toFloat)
       loadOrder(jedis,t10,1)  //1用来判断是否再入redis
      }
    pool.returnResource(jedis)
    pool.destroy()
  }
}
