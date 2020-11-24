package com.xiao.newmallrealtime

import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.JSON
import com.xiao.newmall.realtime.bean.dim.ProvinceInfo
import org.junit.Test

object DateTest {
  def main(args: Array[String]): Unit = {
    val ts="1604224103939".toLong
    val dateFormat = new SimpleDateFormat("YYYY-MM-DD")
    val dateStr: String = dateFormat.format(new Date(ts))
    val day = dateStr.substring(0,10)
    println(day)



    val jsonStr = "{\"area_code\":\"340000\",\"name\":\"安徽\",\"region_id\":\"2\",\"iso_3166_2\":\"CN-AH\",\"id\":9,\"iso_code\":\"CN-34\"}"
    val provinceInfo: ProvinceInfo = JSON.parseObject(jsonStr, classOf[ProvinceInfo])

    println(provinceInfo.toString)
  }

}
