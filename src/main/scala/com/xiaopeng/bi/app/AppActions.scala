package com.xiaopeng.bi.app

import java.sql.{Connection, PreparedStatement}

import com.xiaopeng.bi.utils._
import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import redis.clients.jedis.{Jedis, JedisPool}

import scala.collection.mutable.ArrayBuffer


/**
  * Created by denglh on 2016/11/3.
  * function:实现最近在玩,累计逻辑,下载等实现
  */
object AppActions {
  val logger = Logger.getLogger(this.getClass)

  /**
    * 统计累计下载，只对第一次下载进行累计
    * @param rdd
    */
  def appDownloads(rdd: RDD[(String)]) = {
    val sql="insert into bi_app_quota(uid,uname,loads) values(?,?,?) on duplicate key update loads=loads+?"
    val firstSql="insert into bi_app_download(uid,game_id,channel_id,download_time) values(?,?,?,?) on duplicate key update download_time=? "
    val rows = rdd.filter(x => {
      val arr = x.split("\\|", -1)
      //排除截断日志   只取存在订单号的数据，不存在订单号的为代金券消费  只取直充  只取状态为4的数据
      arr(0).contains("bi_appdownload") && arr.length >= 11  && !arr(9).contains("")
    }).map(x => {
      val odInfo = x.split("\\|", -1)
      (odInfo(3).concat(odInfo(11)),odInfo(9),odInfo(8),odInfo(1).toInt,odInfo(2).toInt)
    })
    rows.foreachPartition(fp => {
        val conn = JdbcUtil.getConn() //数据库连接
        val ps: PreparedStatement = conn.prepareStatement(sql)
        val firstPs: PreparedStatement = conn.prepareStatement(firstSql)
        //获取redis
        val pool: JedisPool = JedisUtil.getJedisPool;
        val jedis0 = pool.getResource
        val jedis2=pool.getResource
        jedis2.select(2)
        for (ds <- fp) {
          //判断是否有值在redis中
          if ((!jedis2.exists("appDownloads|" + ds._1 + "|" + ds._2 + "|" + ds._4.toString)) && (!ds._2.equals(""))) //没有则插入到redis
          {
            val uid = ds._2 //通行证id
            val uname = jedis0.hget(ds._2 + "_member", "username") //获取通行证
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
              jedis2.set("appDownloads|" + ds._1 + "|" + ds._2 + "|" + ds._4.toString, "")
              jedis2.expire("appDownloads|" + ds._1 + "|" + ds._2 + "|" + ds._4.toString, 3600 * 24 * 30)
            }
          }
        }
        ps.close()
        firstPs.close()
        conn.close()
        //redis close
        pool.returnResource(jedis0)
        pool.returnResource(jedis2)
        pool.destroy()
      })

  }

  /**
    * 把数据加载到bi_app_phoneapps表，记录手机内应用安装情况
    * @param rdd
    */
  def appPhoneApps(rdd: RDD[(String)]) = {
    val sql="insert into bi_app_phoneapps(device_id,pkg_name,app_name,status,opera_date,platform) values(?,?,?,?,?,?) on duplicate key update app_name=?,opera_date=?,status=?,platform=?"
    val rows = rdd.filter(x => {
    val arr = x.split("\\|", -1)
      //排除截断日志   只取存在订单号的数据，不存在订单号的为代金券消费  只取直充  只取状态为4的数据
      arr(0).contains("bi_install_app") && arr.length >= 6
    }).map(x => {
      val odInfo = x.split("\\|", -1)
      (odInfo(0).split(",")(0),odInfo(1),odInfo(2),odInfo(3),odInfo(4).toInt,if(odInfo(5).equals("")||odInfo(5).equals("1")) {"android"} else {"ios"})})
    rows.foreachPartition(fp=>{
        val conn = JdbcUtil.getConn() //数据库连接
        val ps: PreparedStatement = conn.prepareStatement(sql)
        //获取redis
        //创建jedis客户端
        val pool: JedisPool = JedisUtil.getJedisPool;
        val jedis = pool.getResource
        jedis.select(10)
        fp.foreach(ds => {
          val pkg=jedis.hget(ds._3.toLowerCase,"package_name")
          if(pkg!=null) {
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
        ps.close()
        conn.close()
        //redis close
        pool.returnResource(jedis)
        pool.destroy()
      })

  }

  /**
    * 统计累计流水
    * @param rdd
    */
  def orderLtdPrice(rdd: RDD[(String)]) = {
    val rows = rdd.filter(x => {
      val arr = x.split("\\|", -1)
      //排除截断日志   只取存在订单号的数据，不存在订单号的为代金券消费  只取直充  只取状态为4的数据
      arr(0).contains("bi_order") && arr.length >= 26  && arr(25).contains("v2.2") && arr(19).toInt == 4
    }).map(x => {
      val odInfo = x.split("\\|", -1)
      (odInfo(5).trim.toLowerCase,odInfo(6),odInfo(10).toFloat,odInfo(19),odInfo(2))
    })
    rows.foreachPartition(fp=>{
        val conn = JdbcUtil.getConn() //数据库连接
        val sql="insert into bi_app_quota(uid,uname,ori_price) values(?,?,?) on duplicate key update ori_price=ori_price+?"
        val ps: PreparedStatement = conn.prepareStatement(sql)
        //获取redis
        val pool: JedisPool = JedisUtil.getJedisPool;
        val jedis = pool.getResource;
        val jedis2=pool.getResource
        jedis.select(0)
        jedis2.select(2)
        fp.foreach(order=>
        {
          var bind_member_id: String =jedis.hget(order._1,"bind_member_id")

          bind_member_id =jedis.hget(order._1,"bind_member_id")
          val uid=if(bind_member_id==null||bind_member_id.equals("")) "0" else bind_member_id
          //判断是否通行证id有对应的通行证名称
          val uname=jedis.hget(uid+"_member","username")  //获取通行证
          val oriPrice=if(order._4.toInt==4) order._3 else {0-order._3}
          //把结果数据插入到表中,但是通行证和通行证id不能为空,同时判断是否已经统计过
          if(uname!=null&&(uid.toInt>0)&&uid.length<11&&(!jedis2.exists("order_no|" + order._5.toString))) {
            try
            {
              ps.setString(1, uid)
              ps.setString(2, uname)
              ps.setFloat(3, oriPrice)
              ps.setFloat(4, oriPrice)
              ps.executeUpdate()
            }
            jedis2.set("order_no|" + order._5.toString,"")
            jedis2.expire("order_no|" + order._5.toString,3600*24)
          }
        })
        ps.close()
        conn.close()
        //redis close
        pool.returnResource(jedis)
        pool.returnResource(jedis2)
        pool.destroy()
      })

  }

  /**
    * 把数据加载到bi_app_game_recently表，最近在玩
    * @param rdd
    */
  def gameRecently(rdd: RDD[(String)]) = {
    val rows = rdd.filter(x => {
      val arr = x.split("\\|", -1)
      //排除截断日志   只取存在订单号的数据，不存在订单号的为代金券消费  只取直充  只取状态为4的数据
      arr(0).contains("bi_login") && arr.length >= 4
    }).map(x => {
      val odInfo = x.split("\\|", -1)
      (odInfo(3),odInfo(4))
    })
    rows.foreachPartition(rp => {
        val conn = JdbcUtil.getConn() //数据库连接
        //都有数据时插入语句
        val sql2Mysql = "insert into bi_app_game_recently(uid,game_id,channel_id,recently_login_time) values(?,?,?,?)  on duplicate key update recently_login_time=?"
        val ps: PreparedStatement = conn.prepareStatement(sql2Mysql)
        //获取redis
        val pool: JedisPool = JedisUtil.getJedisPool;
        val jedis: Jedis = pool.getResource;
        rp.foreach(row => {
          val bind_member_id = jedis.hget(row._1, "bind_member_id")
          val uid = if (bind_member_id == null || bind_member_id.equals("")) "0" else bind_member_id //通过游戏账号获取通行证id
          //判断是否符合条件插入表中
          if (!uid.equals("0")&&uid.length()<11) {
            //当形成的数据没有订单数据时不更新订单日期，若形成的结果数据没有登录数据时不更新登录时间
            val game_id = jedis.hget(row._1, "game_id") //根据账号找gameid
            val channel_id = jedis.hget(row._1, "channel_id") //根据账号找渠道id
            val recently_login_time = row._2 //最近在玩时间
            if (bind_member_id != null && game_id != null && channel_id != null) {
              ps.setString(1, bind_member_id)
              ps.setString(2, game_id)
              ps.setString(3, channel_id)
              ps.setString(4, recently_login_time)
              //update
              ps.setString(5, recently_login_time)
              ps.executeUpdate();
            }
          }
        })
          //关闭mysql连接
          ps.close()
          conn.close()
          //redis resource
          pool.returnResource(jedis)
          pool.destroy()
        })
  }

  /**
    * 计算积分
    * @param rdd
    */
  def appPointQuota(rdd: RDD[(String)]) ={
    val sc = rdd.sparkContext
    val sqlContext = SQLContextSingleton.getInstance(sc)
    AppStreamingUtils.parsePointsSignDetail(rdd, sqlContext).parsePointsRechargeDetail(rdd, sqlContext)
    val appPointsDF = sqlContext.sql("select * from app_points_sign_detail where resourse=4")
                                     .unionAll(sqlContext.sql("select obtain_time,obtain_id,uid,uname,game_id,game_name,points" +
                                     ",ori_price,resourse from app_points_recharge_detail where  order_state=4 and version='v2.2'"))
    appPointsDF.rdd.foreachPartition(rows => {
      //创建jedis客户端
      val pool: JedisPool = JedisUtil.getJedisPool;
      val jedis = pool.getResource

      val conn = JdbcUtil.getConn() //数据库连接
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
      val quotaSqlText = "insert into bi_app_quota(uid,uname,points,create_time) values(?,?,?,?)" +
        " on duplicate key update points=points+?"

      val pointsParams = new ArrayBuffer[Array[Any]]()
      val quotaParams = new ArrayBuffer[Array[Any]]()
      for (insertedRow <- insertedRows) {
        pointsParams.+=(Array[Any](insertedRow(0),insertedRow(1),insertedRow(2),insertedRow(3),insertedRow(4)
          ,insertedRow(5),insertedRow(6),insertedRow(7),insertedRow(8),insertedRow(9),DateUtils.getNowFullDate("yyyy-MM-dd HH:mm:ss")))
        quotaParams.+=(Array[Any](insertedRow(2),insertedRow(3),insertedRow(6),
          DateUtils.getNowFullDate("yyyy-MM-dd HH:mm:ss"),insertedRow(6)))
      }
      try {
        JdbcUtil.doBatch(pointsSqlText, pointsParams, conn)
        JdbcUtil.doBatch(quotaSqlText, quotaParams, conn)
      } catch {
        case ex: Exception => {
          logger.error("=========================插入app point detail表异常：" + ex)
        }
      } finally {
        statement.close()
        conn.close
      }

      pool.returnResource(jedis)
      pool.destroy()

    })
  }


}


