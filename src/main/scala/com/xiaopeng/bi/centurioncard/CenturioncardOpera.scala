package com.xiaopeng.bi.centurioncard

import java.sql.{PreparedStatement, ResultSet, SQLException, Statement}
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import com.xiaopeng.bi.utils.JdbcUtil

object CenturioncardOpera {

  /**
    * 当月流水
    *
    * @param monthSourceSql
    * @param insertMonthSql
    */
  def monthIncome(monthSourceSql: String, insertMonthSql: String) =
  {
    val connBiHippo = JdbcUtil.getBiHippoConn()
    val connMoose = JdbcUtil.getConn()
    var stmt: Statement = null
    val ps: PreparedStatement = connMoose.prepareStatement(insertMonthSql)
    try {
      stmt = connBiHippo.createStatement
    }
    catch {
      case e: SQLException => {
        e.printStackTrace
      }
    }
    val rs: ResultSet = stmt.executeQuery(monthSourceSql)
    while (rs.next) {
      ps.setString(1, rs.getString("dt"))
      ps.setString(2, rs.getString("user_account"))
      ps.setFloat(3, rs.getFloat("rec_amount"))
      ps.setFloat(4, rs.getFloat("rec_amount"))
      ps.executeUpdate()
    }
    connMoose.close()
    connBiHippo.close()
  }

  /**
    * 首页当日流水
    *
    * @param currentdaySourceSql
    * @param insertCurrentdaySql
    */
  def dayIncome(currentdaySourceSql: String, insertCurrentdaySql: String): Unit = {
    val connBiHippo = JdbcUtil.getBiHippoConn()
    val connMoose = JdbcUtil.getConn()
    var stmt: Statement = null
    val ps: PreparedStatement = connMoose.prepareStatement(insertCurrentdaySql)
    try {
      stmt = connBiHippo.createStatement
    }
    catch {
      case e: SQLException => {
        e.printStackTrace
      }
    }
    val rs: ResultSet = stmt.executeQuery(currentdaySourceSql)
    while (rs.next) {
      ps.setString(1, rs.getString("dt"))
      ps.setString(2, rs.getString("user_account"))
      ps.setFloat(3, rs.getFloat("reb_amount"))
      ps.setFloat(4, rs.getFloat("rec_amount"))
      ps.setInt(5, rs.getInt("day_add_users"))
      ps.setFloat(6, rs.getFloat("reb_amount"))
      ps.setFloat(7, rs.getFloat("rec_amount"))
      ps.setInt(8, rs.getInt("day_add_users"))
      ps.executeUpdate()
    }
    connMoose.close()
    connBiHippo.close()

  }

  /**
    * 从交易明细数据中统计数据到推广表
    *
    * @param extendSourceSql
    * @param extendInsertSql
    */
  def extendIncome(extendSourceSql: String, extendInsertSql: String): Unit = {
    val connBiHippo = JdbcUtil.getBiHippoConn()
    val connMoose = JdbcUtil.getConn()
    var stmt: Statement = null
    val ps: PreparedStatement = connMoose.prepareStatement(extendInsertSql)
    try {
      stmt = connBiHippo.createStatement
    }
    catch {
      case e: SQLException => {
        e.printStackTrace
      }
    }
    val rs: ResultSet = stmt.executeQuery(extendSourceSql)
    while (rs.next) {
      ps.setString(1, rs.getString("statistics_date"))
      ps.setString(2, rs.getString("user_account"))
      ps.setInt(3, rs.getInt("game_id"))
      ps.setString(4, rs.getString("game_name"))
      ps.setString(5, rs.getString("os"))
      ps.setInt(6, rs.getInt("day_add_users"))
      ps.setFloat(7, rs.getFloat("rec_amount"))
      ps.setInt(8, rs.getInt("dealings"))
      //update
      ps.setString(9, rs.getString("game_name"))
      ps.setInt(10, rs.getInt("day_add_users"))
      ps.setFloat(11, rs.getFloat("rec_amount"))
      ps.setInt(12, rs.getInt("dealings"))
      ps.executeUpdate()
    }
    connMoose.close()
    connBiHippo.close()
  }

  /**
    * 从账号明细数据中统计新增注册数数据到推广表
    */
  def extendRege(currentday:String,nextday:String): Unit = {
    val extendInsertSql="insert into bi_centurioncard_extend(statistics_date,user_account,game_id,game_name,os,add_regi_accounts) values(?,?,?,?,?,?) on duplicate key update game_name=?,add_regi_accounts=?"
    val extendSourceSql = "select left(regi_time,10) statistics_date,user_account,game_id,game_name ,platform as os, count(game_account) add_regi_accounts from bi_centurioncard_accountinfo \nwhere regi_time >='currentday' and regi_time <'nextday' group by left(regi_time,10),user_account,game_id,game_name,platform".replace("currentday",currentday).replace("nextday",nextday)
    println(extendSourceSql)
    val connBiHippo = JdbcUtil.getBiHippoConn()
    val connMoose = JdbcUtil.getConn()
    var stmt: Statement = null
    val ps: PreparedStatement = connMoose.prepareStatement(extendInsertSql)
    try {
      stmt = connBiHippo.createStatement
    }
    catch {
      case e: SQLException => {
        e.printStackTrace
      }
    }
    val rs: ResultSet = stmt.executeQuery(extendSourceSql)
    while (rs.next) {
      ps.setString(1, rs.getString("statistics_date"))
      ps.setString(2, rs.getString("user_account"))
      ps.setInt(3, rs.getInt("game_id"))
      ps.setString(4, rs.getString("game_name"))
      ps.setString(5, rs.getString("os"))
      ps.setInt(6, rs.getInt("add_regi_accounts"))
      //update
      ps.setString(7, rs.getString("game_name"))
      ps.setInt(8, rs.getInt("add_regi_accounts"))
      ps.executeUpdate()
    }
    connMoose.close()
    connBiHippo.close()
  }

  /**
    * 首页昨天指标数据
    *
    * @param lastDaySourceSql
    * @param lastDayInsertSql
    */
  def lastDayIncome(lastDaySourceSql: String, lastDayInsertSql: String): Unit ={
    val connBiHippo = JdbcUtil.getBiHippoConn()
    val connMoose = JdbcUtil.getConn()
    var stmt: Statement = null
    val ps: PreparedStatement = connMoose.prepareStatement(lastDayInsertSql)
    try {
      stmt = connBiHippo.createStatement
    }
    catch {
      case e: SQLException => {
        e.printStackTrace
      }
    }
    val rs: ResultSet = stmt.executeQuery(lastDaySourceSql)
    while (rs.next) {
      ps.setString(1, rs.getString("dt"))
      ps.setString(2, rs.getString("user_account"))
      ps.setFloat(3, rs.getFloat("reb_amount"))
      ps.setFloat(4, rs.getFloat("rec_amount"))
      ps.setInt(5, rs.getInt("day_add_users"))
      ps.setFloat(6, rs.getFloat("reb_amount"))
      ps.setFloat(7, rs.getFloat("rec_amount"))
      ps.setInt(8, rs.getInt("day_add_users"))
      ps.executeUpdate()
    }
    connMoose.close()
    connBiHippo.close()
  }

  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      System.err.println("Usage: <currentday> ")
      System.exit(1)
    }
    if (args.length > 1) {
      System.err.println("参数个数传入太多，固定为1个： <currentday>  ")
      System.exit(1)
    }
    //跑数日期，当日
    val currentday =args(0)
    val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val date: Date = dateFormat.parse(currentday)
    val cal: Calendar = Calendar.getInstance()
    cal.setTime(date)
    //昨天
    cal.add(Calendar.DATE, -1)
    val yesterday = dateFormat.format(cal.getTime())
    //明天
    cal.add(Calendar.DATE, +2)
    val nextday=dateFormat.format(cal.getTime())
    //月第一天
    val monthfirstday =currentday.substring(0, 7) + "-01"
    //当日流水
    val currentdaySourceSql = "select user_account,statistics_date dt,sum(rec_amount) rec_amount,sum(reb_amount) reb_amount,sum(is_user_add) as day_add_users from bi_centurioncard_gameacc where statistics_date='currentday' group by user_account,statistics_date".replace("currentday", currentday).replace("yesterday", yesterday).replace("monthfirstday", monthfirstday)
    //month
    val monthSourceSql = "select user_account,'currentday' as dt,sum(income) rec_amount \nfrom  bi_centurioncard_extend gc where statistics_date>='monthfirstday' and statistics_date<='currentday' and income>0 group by gc.user_account".replace("currentday", currentday).replace("yesterday", yesterday).replace("monthfirstday", monthfirstday)
    //月流水insert
    val insertMonthSql = " insert into bi_centurioncard_opera(statistics_date,user_account,month_income)" +
      " values(?,?,?)" +
      " on duplicate key update month_income=?"
    //日流水insert
    val insertCurrentdaySql = "insert into bi_centurioncard_opera(statistics_date,user_account,day_profit,day_income,day_add_users) values(?,?,?,?,?) " +
      " on duplicate key update day_profit=?,day_income=?,day_add_users=?"

    //extend流水source
    val extendSourceSql="select user_account,statistics_date,os,game_id,game_name,sum(rec_amount) rec_amount,count(distinct case when order_status='已完成' then order_no else null end) dealings,\nsum(is_user_add) day_add_users from bi_centurioncard_gameacc where statistics_date='currentday'\ngroup by user_account,statistics_date,os,game_id,game_name".replace("currentday", currentday)
    //extend流水推送
    val extendInsertSql=" insert into bi_centurioncard_extend(statistics_date,user_account,game_id,game_name,os,add_users,income,dealings)" +
      " values(?,?,?,?,?,?,?,?)" +
      " on duplicate key update game_name=?,add_users=?,income=?,dealings=?"

    //昨天流水和收益source
    val lastDaySourceSql="select user_account,'currentday' as dt,sum(rec_amount) rec_amount,sum(reb_amount) reb_amount,sum(is_user_add) as day_add_users from bi_centurioncard_gameacc where statistics_date='yesterday' group by user_account".replace("currentday", currentday).replace("yesterday", yesterday).replace("monthfirstday", monthfirstday)
    //昨天流水和收益推送
    val lastDayInsertSql = "insert into bi_centurioncard_opera(statistics_date,user_account,lsday_profit,lsday_income,lsday_add_users) values(?,?,?,?,?) " +
      " on duplicate key update lsday_profit=?,lsday_income=?,lsday_add_users=?"

    println("输入参数为："+currentday)
    println("开始处理时间:"+ new Date(System.currentTimeMillis))
    println(currentdaySourceSql)
    println(monthSourceSql)
    println(extendSourceSql)
    println(lastDaySourceSql)
    //extend流水
    extendIncome(extendSourceSql,extendInsertSql)
    //extend新增注册用户数
    extendRege(currentday,nextday)
    //首页月流水操作
    monthIncome(monthSourceSql,insertMonthSql)
    //首页日流水
    dayIncome(currentdaySourceSql,insertCurrentdaySql)
    //昨天流水,凌晨跑昨天的数
    val d:Date = new Date();
    val hours = d.getHours
    println("开始结束时间:"+ new Date(System.currentTimeMillis))
    if(hours<=4) {
      println(lastDaySourceSql)
      //昨天流水和首页，
      lastDayIncome(lastDaySourceSql, lastDayInsertSql)
    }

  }
}
