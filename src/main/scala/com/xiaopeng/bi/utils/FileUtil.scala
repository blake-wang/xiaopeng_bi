package com.xiaopeng.bi.utils

import java.io.{BufferedWriter, File, FileWriter}

import org.apache.spark.sql.DataFrame

/**
  * Created by kequan on 4/26/17.
  */
object FileUtil {


  /**
    * del
    * @param fileName
    */
  def delfile(fileName: String) = {
    //将数据存入文件
    val file = new File(fileName)
    if (file.exists()) {
      file.delete()
    }

  }

  /**
    * 覆盖
    * @param fileName
    * @param s
    */
  def overWriteTofile(fileName: String,s:String) = {
    //将数据存入文件
    val file = new File(fileName)
    if (!file.exists()) {
      file.createNewFile()
    }
    val writer = new BufferedWriter(new FileWriter(file, true))
    writer.write(s,0,s.length)
    writer.flush()
    writer.close()
  }


  /**
    * 追加到文件
    * @param fileName
    * @param s
    */
  def apppendTofile(fileName: String,s:String) = {
    //将数据存入文件
    val file = new File(fileName)
    if (!file.exists()) {
      file.createNewFile()
    }
    val writer = new BufferedWriter(new FileWriter(file, true))
    writer.append(s)
    writer.newLine()
    writer.flush()
    writer.close()
  }

  /**
    * 用于测试的一个方法
    * @param df
    * @param fileName
    */
  def apppendDfTofile(df: DataFrame, fileName: String) = {
    df.foreachPartition(iter=>{
      iter.foreach(t=>{
        apppendTofile(fileName,t.toString());
      })
    })
  }

}
