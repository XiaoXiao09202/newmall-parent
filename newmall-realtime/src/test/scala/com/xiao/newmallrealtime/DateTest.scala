package com.xiao.newmallrealtime

import java.text.SimpleDateFormat
import java.util.Date

import org.junit.Test

object DateTest {
  def main(args: Array[String]): Unit = {
    val ts="1604224103939".toLong
    val dateFormat = new SimpleDateFormat("YYYY-MM-DD")
    val dateStr: String = dateFormat.format(new Date(ts))
    val day = dateStr.substring(0,10)
    println(day)
  }

}
