package com.xiaopeng.bi.utils

import java.text.{ParseException, SimpleDateFormat}
import java.util.{Calendar, Date}

/**
  * Created by JSJSB-0071 on 2016/8/25.
  */
object DateUtils {
  val HOUR_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH")
  val TIME_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
  val DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd")
  val DATEKEY_FORMAT = new SimpleDateFormat("yyyyMMdd")
  val mm_FORMAT = new SimpleDateFormat("mm")
  val HH_FORMAT = new SimpleDateFormat("HH")


  def getNowFullDate(pattern: String): String = {
    val now: Date = new Date()
    TIME_FORMAT.format(now)
  }

  /**
    * 判断一个时间是否在另一个时间之前
    *
    * @param time1 第一个时间
    * @param time2 第二个时间
    * @return 判断结果
    */
  def beforeTime(time1: String, time2: String): Boolean = {
    try {
      val dateTime1 = TIME_FORMAT.parse(time1)
      val dateTime2 = TIME_FORMAT.parse(time2)
      if (dateTime1.before(dateTime2)) return true
    } catch {
      case e: Exception => {
        e.printStackTrace()
        return false
      }
    }
    return false
  }


  /**
    * 判断一个时间是否在另一个时间之后
    *
    * @param time1 第一个时间
    * @param time2 第二个时间
    * @return 判断结果
    */
  def afterTime(time1: String, time2: String): Boolean = {
    try {
      val dateTime1 = TIME_FORMAT.parse(time1)
      val dateTime2 = TIME_FORMAT.parse(time2)
      if (dateTime1.after(dateTime2)) return true
    } catch {
      case e: Exception => {
        e.printStackTrace()
        return false
      }
    }
    return false
  }

  /**
    * 判断一个时间是否在另一个时间之前
    *
    * @param time1 第一个时间
    * @param time2 第二个时间
    * @return 判断结果
    */
  def beforeHour(time1: String, time2: String): Boolean = {
    try {
      val dateTime1 = HOUR_FORMAT.parse(time1)
      val dateTime2 = HOUR_FORMAT.parse(time2)
      if (dateTime1.before(dateTime2)) return true
    } catch {
      case e: Exception => {
        e.printStackTrace()
        return false
      }
    }
    return false
  }

  /**
    * 判断一个时间是否在另一个时间之后
    *
    * @param time1 第一个时间
    * @param time2 第二个时间
    * @return 判断结果
    */
  def afterHour(time1: String, time2: String): Boolean = {
    try {
      val dateTime1 = HOUR_FORMAT.parse(time1)
      val dateTime2 = HOUR_FORMAT.parse(time2)
      if (dateTime1.after(dateTime2)) return true
    } catch {
      case e: Exception => {
        e.printStackTrace()
        return false
      }
    }
    return false
  }

  /**
    * 判断一个时间是否在另一个时间之前
    *
    * @param time1 第一个时间
    * @param time2 第二个时间
    * @return 判断结果
    */
  def beforeDay(time1: String, time2: String): Boolean = {
    try {
      val dateTime1 = DATE_FORMAT.parse(time1)
      val dateTime2 = DATE_FORMAT.parse(time2)
      if (dateTime1.before(dateTime2)) return true
    } catch {
      case e: Exception => {
        e.printStackTrace()
        return false
      }
    }
    return false
  }

  /**
    * 判断一个时间是否在另一个时间之后
    *
    * @param time1 第一个时间
    * @param time2 第二个时间
    * @return 判断结果
    */
  def afterDay(time1: String, time2: String): Boolean = {
    try {
      val dateTime1 = DATE_FORMAT.parse(time1)
      val dateTime2 = DATE_FORMAT.parse(time2)
      if (dateTime1.after(dateTime2)) return true
    } catch {
      case e: Exception => {
        e.printStackTrace()
        return false
      }
    }
    return false
  }

  /**
    * 判断两个时间的 天数差
    *
    * @param time1 第一个时间
    * @param time2 第二个时间
    * @return 判断结果
    */
  def compareToDay(time1: String, time2: String): Int = {
    try {
      val dateTime1 = DATE_FORMAT.parse(time1)
      val dateTime2 = DATE_FORMAT.parse(time2)
      ((dateTime1.getTime - dateTime2.getTime) / (24 * 60 * 60 * 1000) + "").toInt
    } catch {
      case e: Exception => {
        e.printStackTrace()
        0
      }
    }

  }

  /**
    *
    * 给某个时间添加  几天 :  day 不能为负数
    *
    * @param time1 第一个时间
    * @param day   天数
    * @return 时间
    */
  def addDay(time1: String, day: Int): String = {
    try {
      val dateTime1 = DATE_FORMAT.parse(time1)
      DATE_FORMAT.format(new Date((dateTime1.getTime + day * (24 * 60 * 60 * 1000).toLong)))
    } catch {
      case e: Exception => {
        e.printStackTrace()
        null
      }
    }
  }

  /**
    *
    * 给某个时间减去几天 :  day 不能为负数
    *
    * @param time1 第一个时间
    * @param day   天数
    * @return 时间
    */
  def LessDay(time1: String, day: Int): String = {
    try {
      val dateTime1 = DATE_FORMAT.parse(time1)
      DATE_FORMAT.format(new Date((dateTime1.getTime - day * (24 * 60 * 60 * 1000).toLong)))
    } catch {
      case e: Exception => {
        e.printStackTrace()
        null
      }
    }

  }


  def getDateForRequest(requestDateStr: String): String = {
    try {
      val split = requestDateStr.split("\\[")(1).split("\\]")(0).split(" ")
      val dateStr = split(0).split("/")

      dateStr(2).split(":", -1)(0) + "-" + changeEnglishMonthTo(dateStr(1)) + "-" + dateStr(0)
    } catch {
      case ex: Exception => {
        "0000-00-00"
      }
    }
  }

  def getDateForRequestNew(requestDateStr: String): String = {
    try {
      val split = requestDateStr.split("\\[")(1).split("\\]")(0).split(" ")
      val dateStr = split(0).split("/")

      dateStr(2).split(":", -1)(0) + "-" + changeEnglishMonthTo(dateStr(1)) + "-" + dateStr(0) + " " + dateStr(2).substring(5)
    } catch {
      case ex: Exception => {
        "0000-00-00 00:00:00"
      }
    }
  }


  def getDateHourForRequest(requestDateStr: String): String = {
    try {
      val split = requestDateStr.split("\\[")(1).split("\\]")(0).split(":")
      val dateStr = split(0).split("/")
      val hour = split(1)
      dateStr(2) + "-" + changeEnglishMonthTo(dateStr(1)) + "-" + dateStr(0) + " " + hour
    } catch {
      case ex: Exception => {
        "0000-00-00 00"
      }
    }
  }

  def changeEnglishMonthTo(m: String): String = {
    if (m.equals("Jan")) {
      "01"
    } else if (m.equals("Feb")) {
      "02"
    } else if (m.equals("Mar")) {
      "03"
    } else if (m.equals("Apr")) {
      "04"
    } else if (m.equals("May")) {
      "05"
    } else if (m.equals("Jun")) {
      "06"
    } else if (m.equals("Jul")) {
      "07"
    } else if (m.equals("Aug")) {
      "08"
    } else if (m.equals("Sept")) {
      "09"
    } else if (m.equals("Sep")) {
      "09"
    } else if (m.equals("Oct")) {
      "10"
    } else if (m.equals("Nov")) {
      "11"
    } else if (m.equals("Dec")) {
      "12"
    } else {
      "00"
    }

  }

  def getTodayTime(): String = {
    val now: Date = new Date()
    TIME_FORMAT.format(now)
  }

  def getTodayDate(): String = {
    return DATE_FORMAT.format(new Date());
  }

  def getYesterDayDate(): String = {
    val cal = Calendar.getInstance
    cal.add(Calendar.DATE, -1)
    return DATE_FORMAT.format(cal.getTime);
  }

  /**
    * 获取临界时间，当小时在0点时跑昨天的数据，避免数据不及时导致昨天的数据遗漏
    */
  def getCriticalDate(): String = {
    val cal = Calendar.getInstance
    var dt = ""
    val hour = new Integer(HH_FORMAT.format(new Date))
    if (hour == 0) {
      cal.add(Calendar.DATE, -1)
      dt = DATE_FORMAT.format(cal.getTime)
    }
    else {
      cal.add(Calendar.DATE, 0)
      dt = DATE_FORMAT.format(cal.getTime)
    }
    dt
  }

  def getDateHour(): String = {
    val now: Date = new Date()
    HOUR_FORMAT.format(now)
  }

  def getHour(): String = {
    val now: Date = new Date()
    HH_FORMAT.format(now)
  }

  /**
    * 获取临界小时，当分钟小于 30时同时更新上一个小时的数据，避免数据不及时导致数据迁移失败
    */
  def getCriticalHour(): String = {
    val cal = Calendar.getInstance
    var dt = ""
    val mm = new Integer(mm_FORMAT.format(new Date))
    if (mm <= 39) {
      cal.add(Calendar.HOUR, -1)
      dt = HOUR_FORMAT.format(cal.getTime)
    }
    else {
      cal.add(Calendar.HOUR, 0)
      dt = HOUR_FORMAT.format(cal.getTime)
    }
    dt
  }

  /**
    * 把时间戳转化为字符串
    *
    * @param time 时间字符串
    * @return Date
    */
  def formatTime(time: Long): String = {
    try {
      return TIME_FORMAT.format(new Date(time))
    } catch {
      case e: ParseException => {
        e.printStackTrace()
      }
    }
    null
  }

  /**
    * 把 String 转化为 Date
    *
    * @param time 时间字符串
    * @return Date
    */
  def parseTime(time: String): Date = {
    try {
      return TIME_FORMAT.parse(time)
    } catch {
      case e: ParseException => {
        e.printStackTrace()
      }
    }
    null
  }

  /**
    * 获取日期,根据传入的整型数据，判断几天前的数据或者几天后的数据
    *
    * @return string
    */
  def getday(bt: Int): String = {

    val cal = Calendar.getInstance
    var dt = ""
    cal.add(Calendar.DATE, bt)
    dt = DATE_FORMAT.format(cal.getTime)
    dt
  }

}
