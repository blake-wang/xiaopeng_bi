package com.xiaopeng.bi.centurioncard

import java.sql.{PreparedStatement, ResultSet, SQLException, Statement}
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import com.xiaopeng.bi.utils.JdbcUtil

object CenturioncardOperaTotal {

  /**
    * logic:从推广数据总统计当日新增流水和账号数，然后从首页汇总数据中拿取昨天的数据，累计到目前的数据=昨天+今天新增，
    * 注意：必须在晚上4点后统计
    *
    * @param IASourceSql
    * @param IATargetSql
    */
  def curIncomeVSAccounts(IASourceSql: String, IATargetSql: String): Unit = {
    val connBiHippo = JdbcUtil.getBiHippoConn()
    val connMoose = JdbcUtil.getConn()
    var stmt: Statement = null
    val ps: PreparedStatement = connMoose.prepareStatement(IATargetSql)
    try {
      stmt = connBiHippo.createStatement
    }
    catch {
      case e: SQLException => {
        e.printStackTrace
      }
    }
    val rs: ResultSet = stmt.executeQuery(IASourceSql)
    while (rs.next) {
      ps.setString(1, rs.getString("statistics_date"))
      ps.setString(2, rs.getString("user_account"))
      ps.setFloat(3, rs.getFloat("tocur_income"))
      ps.setInt(4, rs.getInt("tocur_total_gameaccs"))
      //update
      ps.setFloat(5, rs.getFloat("tocur_income"))
      ps.setInt(6, rs.getInt("tocur_total_gameaccs"))
      ps.executeUpdate()
    }
    connMoose.close()
    connBiHippo.close()
  }

  /**
    * logic:统计未交易账号
    * 注意：必须在晚上4点后统计
    *
    * @param IASourceSql
    * @param IATargetSql
    */
  def curChargesAccounts(IASourceSql: String, IATargetSql: String): Unit = {
    val connBiHippo = JdbcUtil.getBiHippoConn()
    val connMoose = JdbcUtil.getConn()
    var stmt: Statement = null
    val ps: PreparedStatement = connMoose.prepareStatement(IATargetSql)
    try {
      stmt = connBiHippo.createStatement
    }
    catch {
      case e: SQLException => {
        e.printStackTrace
      }
    }
    val rs: ResultSet = stmt.executeQuery(IASourceSql)
    while (rs.next) {
      ps.setString(1, rs.getString("statistics_date"))
      ps.setString(2, rs.getString("user_account"))
      ps.setFloat(3, rs.getFloat("tocur_amount"))
      ps.setInt(4, rs.getInt("tocur_gameaccs"))
      //update
      ps.setFloat(5, rs.getFloat("tocur_amount"))
      ps.setInt(6, rs.getInt("tocur_gameaccs"))
      ps.executeUpdate()
    }
    connMoose.close()
    connBiHippo.close()
  }

  /**
    * main exec
    *
    * @param args
    */
  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      System.err.println("Usage: <currentday> ")
      System.exit(1)
    }
    if (args.length > 1) {
      System.err.println("参数个数传入太多，固定为1个： <currentday>  ")
      System.exit(1)
    }
    //跑数日期
    val currentday =args(0)
    //昨天
    val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val date: Date = dateFormat.parse(currentday)
    val cal: Calendar = Calendar.getInstance()
    cal.setTime(date)
    cal.add(Calendar.DATE, -1)
    val yesterday = dateFormat.format(cal.getTime())
    //月第一天
    val monthfirstday =currentday.substring(0, 7) + "-01"
    //curIncomeVSAccounts source
    val IASourceSql = "select a.statistics_date,a.user_account,if(b.tocur_income is null,0,b.tocur_income)+a.income as tocur_income,\nif(b.tocur_total_gameaccs is null,0,b.tocur_total_gameaccs)+add_regi_accounts  tocur_total_gameaccs\nfrom  (select statistics_date,user_account,sum(add_regi_accounts) add_regi_accounts,sum(income) income  \nfrom bi_centurioncard_extend where statistics_date='currentday' group by statistics_date,user_account) a  \nleft join bi_centurioncard_opera b on a.user_account=b.user_account and b.statistics_date='yesterday'".replace("currentday", currentday).replace("yesterday", yesterday).replace("monthfirstday", monthfirstday)
    //curIncomeVSAccounts target
    val IATargetSql = "insert into bi_centurioncard_opera(statistics_date,user_account,tocur_income,tocur_total_gameaccs) values(?,?,?,?) on duplicate key update tocur_income=?,tocur_total_gameaccs=? "

    //curChargesAccounts source
    val CASourceSql = "select a.statistics_date,a.user_account,if(b.tocur_amount is null,0,b.tocur_amount)+a.rebate_amount as tocur_amount,\nif(b.tocur_gameaccs is null,0,b.tocur_gameaccs)+add_deal_accounts  tocur_gameaccs\nfrom  \n(\nselect statistics_date,user_account,sum(reb_amount) rebate_amount,sum(is_user_add) add_deal_accounts from bi_centurioncard_gameacc \nwhere statistics_date='currentday' and order_status='已完成'  group by statistics_date,user_account\n) a  \nleft join bi_centurioncard_opera b on a.user_account=b.user_account and b.statistics_date='yesterday'".replace("currentday", currentday).replace("yesterday", yesterday).replace("monthfirstday", monthfirstday)
    //curChargesAccounts target
    val CATargetSql="insert into bi_centurioncard_opera(statistics_date,user_account,tocur_amount,tocur_gameaccs) values(?,?,?,?) on duplicate key update tocur_amount=?,tocur_gameaccs=?"

    println("输入参数为："+currentday)
    println("开始处理时间:"+ new Date(System.currentTimeMillis))
    //今天新增 凌晨4点开始跑,凌晨跑数
    val d:Date = new Date();
    val hours = d.getHours
    println(hours)
    if(hours>=4) {
      println(IASourceSql)
      println(CASourceSql)
      println(IATargetSql)
      println(CATargetSql)
      curIncomeVSAccounts(IASourceSql,IATargetSql)
      curChargesAccounts(CASourceSql,CATargetSql)
    } else {println("this time not need run curIncomeVSAccounts")}
    println("处理结束时间:"+ new Date(System.currentTimeMillis))
  }
}
