package com.xiao.newmall.realtime.util

import java.sql.{Connection, DriverManager, ResultSet, ResultSetMetaData, Statement}

import com.alibaba.fastjson.JSONObject

import scala.collection.mutable.ListBuffer

object MySQLUtil {

  def   queryList(sql:String):List[JSONObject]={
    Class.forName("com.mysql.jdbc.Driver")
    val resultList: ListBuffer[JSONObject] = new  ListBuffer[JSONObject]()
    val conn: Connection = DriverManager.getConnection("jdbc:mysql://centos03:3306/newmall_rs?characterEncoding=utf-8&useSSL=false","root","Rootroot")
    val stat: Statement = conn.createStatement
    println(sql)
    val rs: ResultSet = stat.executeQuery(sql)
    val md: ResultSetMetaData = rs.getMetaData
    while (  rs.next ) {
      val rowData = new JSONObject();
      for (i  <-1 to md.getColumnCount  ) {
        rowData.put(md.getColumnName(i), rs.getObject(i))
      }
      resultList+=rowData
    }

    stat.close()
    conn.close()
    resultList.toList
  }

  def main(args: Array[String]): Unit = {
    val list:  List[ JSONObject] = queryList("select * from  newmall_rs.offsets")
    println(list)
  }
}
